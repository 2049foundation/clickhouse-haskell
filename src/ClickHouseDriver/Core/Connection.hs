{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module ClickHouseDriver.Core.Connection
  ( tcpConnect,
    defaultTCPConnection,
    TCPConnection (..),
    sendQuery,
    receiveData,
    sendData,
  )
where

import ClickHouseDriver.Core.Types
import qualified ClickHouseDriver.Core.Block as Block
import qualified ClickHouseDriver.Core.ClientProtocol as Client
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.QueryProcessingStage as Stage
import qualified ClickHouseDriver.Core.ServerProtocol as Server
import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Control.Monad.State.Lazy
import qualified Data.Binary as Binary
import Data.ByteString hiding (unpack)
import Data.ByteString.Builder
import Data.ByteString.Char8 (unpack)
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as L
import Data.Int
import Data.Word
import Network.HTTP.Client
import qualified Network.Simple.TCP as TCP
import Network.Socket
import Control.Monad.Writer

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""


versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

sendHello :: (ByteString, ByteString, ByteString)-> Socket -> IO ()
sendHello (database, usrname, password)  sock = do
  (_,w) <- runWriterT writeHello 
  print ("w = " <> (toLazyByteString w))
  TCP.sendLazy sock (toLazyByteString w) where
    writeHello ::IOWriter Builder
    writeHello = do
      writeVarUInt Client._HELLO
      writeBinaryStr ("ClickHouse " <> _CLIENT_NAME)
      writeVarUInt _CLIENT_VERSION_MAJOR
      writeVarUInt _CLIENT_VERSION_MINOR
      writeVarUInt _CLIENT_REVISION
      writeBinaryStr database
      writeBinaryStr usrname
      writeBinaryStr password
  

receiveHello :: ByteString -> IO (Either ByteString ServerInfo)
receiveHello str = do
  (res, _) <- runStateT receiveHello' str
  return res

receiveHello' :: StateT ByteString IO (Either ByteString ServerInfo)
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

tcpConnect :: ByteString 
           -> ByteString 
           -> ByteString 
           -> ByteString 
           -> ByteString 
           -> Bool 
           -> IO (Either String TCPConnection)
tcpConnect host port user password database compression = do
  (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
  sendHello (database, user, password) sock
  hello <- TCP.recv sock _BUFFER_SIZE
  let isCompressed = if compression then Client._COMPRESSION_ENABLE else Client._COMPRESSION_DISABLE
  case hello of
    Nothing -> return $ Left "Connection failed"
    Just x -> do
      info <- receiveHello x
      print info
      case info of
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
sendQuery query query_id env@TCPConnection {tcpCompression = comp, tcpSocket = sock, serverInfo=info} = do
  (_,r) <- runWriterT $ do 
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
        writeInfo client_info revis
      else return ()
    writeVarUInt 0
    writeVarUInt Stage._COMPLETE
    writeVarUInt comp
    writeBinaryStr query
  TCP.sendLazy sock (toLazyByteString r)

sendData :: ByteString -> TCPConnection -> IO ()
sendData table_name TCPConnection {tcpSocket = sock} = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
  (_,r) <- runWriterT $ do
    writeVarUInt Client._DATA
    writeBinaryStr table_name
    Block.writeInfo info
    writeVarUInt 0 -- #col
    writeVarUInt 0 -- #row
  TCP.sendLazy sock (toLazyByteString r)

sendCancel :: TCPConnection -> IO ()
sendCancel TCPConnection {tcpSocket = sock} = do
  (_,c) <- runWriterT $ writeVarUInt Client._CANCEL
  TCP.sendLazy sock (toLazyByteString c) where

    

receivePacket :: ByteString -> IO ()
receivePacket istr = undefined

--let packet_type = runStateT readVarInt istr

receiveData :: StateT ByteString IO ByteString
receiveData = do
  packet_type <- readVarInt
  result <- readBinaryStr
  return result

writeInfo :: (MonoidMap ByteString w)=>ClientInfo->Word->IOWriter w

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
  ) server_revision
  | server_revision < _DBMS_MIN_REVISION_WITH_CLIENT_INFO 
    = error "Method writeInfo is called for unsupported server revision"
  | otherwise = do
    writeVarUInt 0
    writeBinaryStr initial_user
    writeBinaryStr initial_query_id
    writeBinaryStr initial_address
    
    writeVarUInt (if interface == HTTP then 0 else 1)

    writeBinaryStr ""
    --writeBinaryStr hostname
    writeBinaryStr client_name
    writeVarUInt client_version_major
    writeVarUInt client_version_minor
    writeVarUInt client_revision
    if server_revision >= _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
      then writeBinaryStr quota_key
      else return ()
    if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
      then writeVarUInt client_version_patch
      else return ()