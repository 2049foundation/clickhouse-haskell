{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}

module ClickHouseDriver.Core.Connection
  ( tcpConnect,
    sendQuery,
    receiveData,
    sendData,
    receiveResult,
    closeConnection
  )
where

import qualified ClickHouseDriver.Core.Block as Block
import qualified ClickHouseDriver.Core.Block as Block
import qualified ClickHouseDriver.Core.Error as Error
import qualified ClickHouseDriver.Core.ClientProtocol as Client
import ClickHouseDriver.Core.Column
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.QueryProcessingStage as Stage
import qualified ClickHouseDriver.Core.ServerProtocol as Server
import ClickHouseDriver.Core.Types
import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Control.Monad.State.Lazy (runStateT, get)
import Control.Monad.Writer
import qualified Data.Binary as Binary
import Data.ByteString hiding (filter, unpack)
import Data.ByteString.Builder (toLazyByteString, Builder)
import Data.ByteString.Char8 (unpack)
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as L
import Data.Int
import qualified Data.Vector as V
import Data.Vector (Vector, (!))
import Data.Word
import qualified Network.Simple.TCP as TCP
import Network.Socket (Socket)
import System.Timeout
import Control.Monad.Loops (iterateWhile)
import qualified Data.List as List
--Debug 
import Debug.Trace (trace)

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""

versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

-- | set timeout to 10 seconds
ping :: Int->TCPConnection->IO(Maybe String)
ping timelimit TCPConnection{tcpHost=host,tcpPort=port,tcpSocket=sock}
  = timeout timelimit $ do
      r <- execWriterT $ writeVarUInt Client._PING
      TCP.sendLazy sock (toLazyByteString r)
      buf <- createBuffer 1024 sock
      (packet_type,_) <- runStateT (iterateWhile ( == Server._PROGRESS) readVarInt) buf
      if packet_type /= Server._PONG
        then do
          let p_type = Server.toString $ fromIntegral packet_type
          let report = "Unexpected packet from server " <> show host <> ":" 
                      <> show port <> ", expected " <> "Pong!" <> ", got " 
                      <> show p_type
          return $ show Error.ServerException{
                      code = Error._UNEXPECTED_PACKET_FROM_SERVER,
                      message = report,
                      nested = Nothing
                  }
        else return "PONG!"

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
              context = Context{
                server_info = Just x,
                client_info = Nothing,
                client_setting = Nothing
              },
              tcpCompression = isCompressed
            }
    Left "Exception" -> return $ Left "exception"
    Left x -> do
      print ("error is " <> x)
      TCP.closeSock sock
      return $ Left "Connection error"

sendQuery ::TCPConnection-> ByteString -> Maybe ByteString -> IO ()
sendQuery TCPConnection{context=Context{server_info=Nothing}} _ _ = error "Empty server info"
sendQuery
  env@TCPConnection
    { tcpCompression = comp,
      tcpSocket = sock,
      context = Context{server_info=Just info}
    } 
  query
  query_id = do
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

sendData :: TCPConnection -> ByteString -> Maybe Block -> IO ()
sendData TCPConnection {tcpSocket = sock, context=ctx} table_name maybe_block = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
  r <- execWriterT $ do
    writeVarUInt Client._DATA
    writeBinaryStr table_name
    Block.writeInfo info
    case maybe_block of
      Nothing -> do
        writeVarUInt 0 -- #col
        writeVarUInt 0 -- #row
      Just block -> do
        Block.writeBlockOutputStream ctx block

  TCP.sendLazy sock $ toLazyByteString r

sendCancel :: TCPConnection -> IO ()
sendCancel TCPConnection {tcpSocket = sock} = do
  c <- execWriterT $ writeVarUInt Client._CANCEL
  TCP.sendLazy sock (toLazyByteString c)

processInsertQuery :: TCPConnection
                      ->ServerInfo
                      ->ByteString
                      ->Maybe ByteString
                      ->[[ClickhouseType]]
                      ->IO ByteString
processInsertQuery tcp@TCPConnection{tcpSocket=sock} server_info query_without_data query_id items = do
  sendQuery tcp query_without_data query_id
  buf <- createBuffer 1024 sock
  (sample_block,_) <- runStateT 
    (iterateWhile (\case Block{..} -> True
                         MultiString _ -> False
                         _ -> error "unexpected packet type"
                  )
     $ receivePacket server_info) buf
  let vectorized = V.map V.fromList (V.fromList $ List.transpose items)
  let dataBlock = case sample_block of
        Block typeinfo@ColumnOrientedBlock{
          columns_with_type = cwt
        } -> 
          typeinfo{
            cdata = vectorized
          }
        _ -> error "unexpected packet type"
  -- TODO add slicer for blocks
  sendData tcp "" (Just dataBlock)
  runStateT (receivePacket server_info) buf
  return "1"

receiveData :: ServerInfo -> Reader Block.Block
receiveData ServerInfo {revision = revision} = do
  xx <-
    if revision >= _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
      then readBinaryStr
      else return ""
  block <- Block.readBlockInputStream
  return block

receiveResult :: ServerInfo->QueryInfo-> Reader CKResult
receiveResult info queryinfo = do
  packets <- packetGen
  let onlyDataPacket = filter isBlock packets
      dataVectors = (ClickHouseDriver.Core.Column.transpose . Block.cdata . queryData) <$> onlyDataPacket
      newQueryInfo = Prelude.foldl updateQueryInfo queryinfo packets
  return $ CKResult (V.concat dataVectors) newQueryInfo
  where
    updateQueryInfo :: QueryInfo->Packet->QueryInfo
    updateQueryInfo q (Progress prog) 
      = storeProgress q prog
    updateQueryInfo q (StreamProfileInfo profile) 
      = storeProfile q profile
    updateQueryInfo q _ = q

    isBlock :: Packet -> Bool
    isBlock Block {queryData=Block.ColumnOrientedBlock{cdata=d}} 
            = (V.length d > 0 &&  V.length (d ! 0) > 0)
    isBlock _ = False

    packetGen :: Reader [Packet]
    packetGen = do
      packet <- receivePacket info
      case packet of
        EndOfStream -> return []
        _ -> do
          next <- packetGen
          return (packet : next)
  
receivePacket :: ServerInfo->Reader Packet
receivePacket info = do
  packet_type <- readVarInt
  case packet_type of
    1 -> (receiveData info) >>= (return . Block) -- Data
    2 -> (Error.readException Nothing) >>= (error . show) -- Exception
    3 -> (readProgress $ revision info) >>= (return . Progress) -- Progress
    5 -> return EndOfStream -- End of Stream
    6 -> readBlockStreamProfileInfo >>= (return . StreamProfileInfo) --Profile
    7 -> (receiveData info) >>= (return . Block) -- Total
    8 -> (receiveData info) >>= (return . Block) -- Extreme
          -- 10 -> return undefined -- Log
    11 -> do -- MutiStrings message
      first <- readBinaryStr
      second <- readBinaryStr
      return $ MultiString (first, second)
    0 -> return Hello -- Hello
    _ -> do
      closeBufferSocket
      error $
        show
          Error.ServerException
            { code = Error._UNKNOWN_PACKET_FROM_SERVER,
              message = "Unknown packet from server",
              nested = Nothing
            }

closeConnection :: TCPConnection->IO ()
closeConnection TCPConnection{tcpSocket=sock} = TCP.closeSock sock

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

-------------------------------------------------------------------------------------------------------------------
---Helpers 

{-# INLINE closeBufferSocket #-}
closeBufferSocket :: Reader ()
closeBufferSocket = do
  buf <- get
  let sock = ClickHouseDriver.IO.BufferedReader.socket buf
  TCP.closeSock sock