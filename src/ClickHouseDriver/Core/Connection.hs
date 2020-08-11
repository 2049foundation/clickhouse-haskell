{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module ClickHouseDriver.Core.Connection
  ( tcpConnect,
    defaultTCPConnection,
    TCPConnection (..),
    sendQuery,
    receiveData,
    sendData,
    receiveResult,
    closeConnection
  )
where

import qualified ClickHouseDriver.Core.Block as Block
import qualified ClickHouseDriver.Core.Block as Block
import qualified ClickHouseDriver.Core.ClientProtocol as Client
import ClickHouseDriver.Core.Column
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.QueryProcessingStage as Stage
import qualified ClickHouseDriver.Core.ServerProtocol as Server
import ClickHouseDriver.Core.Types
import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Control.Monad.State.Lazy
import Control.Monad.Writer
import qualified Data.Binary as Binary
import Data.ByteString hiding (filter, unpack)
import Data.ByteString.Builder
import Data.ByteString.Char8 (unpack)
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as L
import Data.Int
import qualified Data.Vector as V
import Data.Vector (Vector, (!))
import Data.Word
import Network.HTTP.Client
import qualified Network.Simple.TCP as TCP
import Network.Socket


--Debug 
import Debug.Trace 

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""

versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

sendHello :: (ByteString, ByteString, ByteString) -> Socket -> IO ()
sendHello (database, usrname, password) sock = do
  (_, w) <- runWriterT writeHello
  TCP.sendLazy sock (toLazyByteString w)
  where
    writeHello :: IOWriter Builder
    writeHello = do
      writeVarUInt Client._HELLO
      writeBinaryStr ("ClickHouse " <> _CLIENT_NAME)
      writeVarUInt _CLIENT_VERSION_MAJOR
      writeVarUInt _CLIENT_VERSION_MINOR
      writeVarUInt _CLIENT_REVISION
      writeBinaryStr database
      writeBinaryStr usrname
      writeBinaryStr password

receiveHello :: Buffer -> IO (Either ByteString ServerInfo)
receiveHello buf = do
  (res, _) <- runStateT receiveHello' buf
  return res

receiveHello' :: Reader (Either ByteString ServerInfo)
receiveHello' = do
  packet_type <- readVarInt
  if packet_type == Server._HELLO
    then do
      server_name <- readBinaryStr
      server_version_major <- readVarInt
      server_version_minor <- readVarInt
      server_revision <- readVarInt
      server_timezone <-
        if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
          then do
            s <- readBinaryStr
            return $ Just s
          else return Nothing
      server_displayname <-
        if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
          then do
            s <- readBinaryStr
            return s
          else return ""
      server_version_dispatch <-
        if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
          then do
            s <- readVarInt
            return s
          else return server_revision
      return $
        Right
          ServerInfo
            { name = server_name,
              version_major = server_version_major,
              version_minor = server_version_minor,
              version_patch = server_version_dispatch,
              revision = server_revision,
              timezone = server_timezone,
              display_name = server_displayname
            }
    else
      if packet_type == Server._EXCEPTION
        then do
          e <- readBinaryStr
          e2 <- readBinaryStr
          e3 <- readBinaryStr
          return $ Left ("exception" <> e <> " " <> e2 <> " " <> e3)
        else return $ Left "Error"

defaultTCPConnection :: IO (Either String TCPConnection)
defaultTCPConnection = tcpConnect "localhost" "9000" "default" "12345612341" "default" False

tcpConnect ::
  ByteString ->
  ByteString ->
  ByteString ->
  ByteString ->
  ByteString ->
  Bool ->
  IO (Either String TCPConnection)
tcpConnect host port user password database compression = do
  (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
  sendHello (database, user, password) sock
  let isCompressed = if compression then Client._COMPRESSION_ENABLE else Client._COMPRESSION_DISABLE

  buf <- createBuffer _BUFFER_SIZE sock
  hello <- receiveHello buf
  case hello of
    Right x ->
      return $
        Right
          TCPConnection
            { tcpHost = host,
              tcpPort = port,
              tcpUsername = user,
              tcpPassword = password,
              tcpSocket = sock,
              tcpSockAdrr = sockaddr,
              serverInfo = x,
              tcpCompression = isCompressed
            }
    Left "Exception" -> return $ Left "exception"
    Left x -> do
      print ("error is " <> x)
      TCP.closeSock sock
      return $ Left "Connection error"

sendQuery :: ByteString -> Maybe ByteString -> TCPConnection -> IO ()
sendQuery
  query
  query_id
  env@TCPConnection
    { tcpCompression = comp,
      tcpSocket = sock,
      serverInfo = info
    } = do
    print (info)
    (_, r) <- runWriterT $ do
      writeVarUInt Client._QUERY
      writeBinaryStr
        ( case query_id of
            Nothing -> ""
            Just x -> x
        )
      let revis = revision info
      if revis >= _DBMS_MIN_REVISION_WITH_CLIENT_INFO
        then do
          let client_info = getDefaultClientInfo (_DBMS_NAME <> " " <> _CLIENT_NAME)
          writeInfo client_info info
        else return ()
      writeVarUInt 0
      writeVarUInt Stage._COMPLETE
      writeVarUInt comp
      writeBinaryStr query
    let res = toLazyByteString r
    TCP.sendLazy sock (toLazyByteString r)

sendData :: ByteString -> TCPConnection -> IO ()
sendData table_name TCPConnection {tcpSocket = sock} = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
  (_, r) <- runWriterT $ do
    writeVarUInt Client._DATA
    writeBinaryStr table_name
    Block.writeInfo info
    -- TODO need to support sending columns
    writeVarUInt 0 -- #col
    writeVarUInt 0 -- #row
  TCP.sendLazy sock (toLazyByteString r)

sendCancel :: TCPConnection -> IO ()
sendCancel TCPConnection {tcpSocket = sock} = do
  (_, c) <- runWriterT $ writeVarUInt Client._CANCEL
  TCP.sendLazy sock (toLazyByteString c)

receiveData :: ServerInfo -> Reader Block.Block
receiveData ServerInfo {revision = revision} = do
  xx <-
    if revision >= _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
      then readBinaryStr
      else return ""
  block <- Block.readBlockInputStream
  return block

receiveResult :: ServerInfo -> Reader (Vector (Vector ClickhouseType))
receiveResult info = do
  packets <- packetGen
  let onlyDataPacket = filter isBlock packets
      dataVectors = (ClickHouseDriver.Core.Column.transpose . Block.cdata . queryData) <$> onlyDataPacket
  return $ (V.concat dataVectors)
  where
    isBlock :: Packet -> Bool
    isBlock Block {queryData=Block.ColumnOrientedBlock{cdata=d}} = (V.length d > 0 &&  V.length (d ! 0) > 0)
    isBlock _ = False

    packetGen :: Reader [Packet]
    packetGen = do
      packet <- receivePacket
      case packet of
        EndOfStream -> return []
        _ -> do
          next <- packetGen
          return (packet : next)
  
    receivePacket :: Reader Packet
    receivePacket = do
      packet_type <- readVarInt
      case packet_type of
        1 -> do  -- Data
          result <- receiveData info
          return $ Block result

        3 -> do --Progress
          progress <- readProgress (revision info)
          return Progress {prog = progress}
          
        6 -> do --Profile
          profile_info <- readBlockStreamProfileInfo
          return StreamProfileInfo {profile = profile_info}

        5 -> do --End of stream
          return EndOfStream

        _ -> return Exception {message = "error"}

closeConnection :: Either String TCPConnection->IO ()
closeConnection (Right TCPConnection{tcpSocket=sock}) = TCP.closeSock sock
closeConnection (Left e) = print e

writeInfo ::
  (MonoidMap ByteString w) =>
  ClientInfo ->
  ServerInfo ->
  IOWriter w
writeInfo
  ( ClientInfo
      client_name
      interface
      client_version_major
      client_version_minor
      client_version_patch
      client_revision
      initial_user
      initial_query_id
      initial_address
      quota_key
      query_kind
    )
  ServerInfo {revision = server_revision, display_name = host_name}
    | server_revision < _DBMS_MIN_REVISION_WITH_CLIENT_INFO =
      error "Method writeInfo is called for unsupported server revision"
    | otherwise = do
      writeVarUInt 1
      writeBinaryStr initial_user
      writeBinaryStr initial_query_id
      writeBinaryStr initial_address

      writeVarUInt (if interface == HTTP then 0 else 1)

      writeBinaryStr "" -- os_user. Seems that haskell modules don't have support of getting system username yet.
      writeBinaryStr host_name
      writeBinaryStr "haskell" --client_name
      writeVarUInt client_version_major
      writeVarUInt client_version_minor
      writeVarUInt client_revision
      if server_revision >= _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
        then writeBinaryStr quota_key
        else return ()
      if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
        then writeVarUInt client_version_patch
        else return ()