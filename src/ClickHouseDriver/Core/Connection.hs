
{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.Connection (
    tcpConnect,
    httpConnect,
    defaultHttpConnection,
    ClickHouseConnection(..)
) where

import Data.ByteString.Builder
import ClickHouseDriver.IO.BufferedWriter
import ClickHouseDriver.IO.BufferedReader
import Network.Socket                                           
import qualified Network.Simple.TCP                        as TCP
import qualified Data.ByteString.Lazy                      as L
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.ClientProtocol      as Client
import qualified ClickHouseDriver.Core.ServerProtocol      as Server
import ClickHouseDriver.Core.Types
import Data.ByteString                                     hiding (unpack)
import Data.ByteString.Char8                               (unpack)
import Network.HTTP.Client
import Control.Monad.State.Lazy
import Data.Word

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""

data ServerInfo = ServerInfo {
    name :: ByteString,
    version_major :: Word16,
    version_minor :: Word16,
    version_patch :: Word16,
    revision      :: Word16,
    timezone      :: Maybe ByteString,
    display_name  :: ByteString
}

data ClickHouseConnection
  = HttpConnection
      { httpHost :: {-# UNPACK #-} !String,
        httpPort :: {-# UNPACK #-} !Int,
        httpUsername :: {-# UNPACK #-} !String,
        httpPassword :: {-# UNPACK #-} !String,
        httpManager :: {-# UNPACK #-} !Manager
      }
  | TCPConnection
      { tcpHost :: {-# UNPACK #-} !ByteString,
        tcpPort :: {-# UNPACK #-} !ByteString,
        tcpUsername :: {-# UNPACK #-} !ByteString,
        tcpPassword :: {-# UNPACK #-} !ByteString,
        tcpSocket   :: {-# UNPACK #-} !Socket,
        tcpSockAdrr :: {-# UNPACK #-} !SockAddr,
        serverInfo  :: {-# UNPACK #-} !ServerInfo
      }

defaultHttpConnection :: IO (ClickHouseConnection)
defaultHttpConnection = httpConnect DEFAULT_USERNAME DEFAULT_PASSWORD 8123 DEFAULT_HOST_NAME


httpConnect :: String->String->Int->String->IO(ClickHouseConnection)
httpConnect user password port host = do
  mng <- newManager defaultManagerSettings
  return HttpConnection {
    httpHost = host,
    httpPassword = password,
    httpPort = port,
    httpUsername = user,
    httpManager = mng
  }


versionTuple :: ServerInfo->(Word16,Word16,Word16)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

sendHello ::  (ByteString,ByteString,ByteString)->Socket->IO()
sendHello (database, usrname, password) sock  = do
    w <- writeVarUInt Client._HELLO mempty 
        >>= writeBinaryStr _CLIENT_NAME 
        >>= writeVarUInt _CLIENT_VERSION_MAJOR
        >>= writeVarUInt _CLIENT_VERSION_MINOR
        >>= writeVarUInt _CLIENT_REVISION
        >>= writeBinaryStr database
        >>= writeBinaryStr usrname
        >>= writeBinaryStr password
    TCP.sendLazy sock (toLazyByteString w)


receiveHello :: ByteString->IO(Either ByteString ServerInfo)
receiveHello str = do
  (res,_) <- runStateT receiveHello' str
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
      server_timezone <- if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE then do
            s<-readBinaryStr
            return $ Just s 
            else return Nothing
      server_displayname <- if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME then do
            s <- readBinaryStr
            return s
            else return ""
      server_version_dispatch <- if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH then do
            s <- readVarInt
            return s
            else return server_revision
      return $ Right ServerInfo {
        name = server_name,
        version_major = server_version_major,
        version_minor = server_version_minor,
        version_patch = server_version_dispatch,
        revision      = server_revision,
        timezone      = server_timezone,
        display_name  = server_displayname
      }
    else if packet_type == Server._EXCEPTION
      then
        return $ Left "exception"
      else
        return $ Left "Error"

tcpConnect :: ByteString->ByteString->ByteString->ByteString->IO(Either String ClickHouseConnection)
tcpConnect host port user password = do
    (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
    sendHello (host, host, password) sock
    hello <- TCP.recv sock _BUFFER_SIZE
    case hello of
      Nothing-> return $ Left "Connection failed"
      Just x -> do
        info <- receiveHello x
        case info of
          Right x -> return $ Right TCPConnection {
          tcpHost = host,
          tcpPort = port,
          tcpUsername = user,
          tcpPassword = password,
          tcpSocket   = sock,
          tcpSockAdrr = sockaddr,
          serverInfo  = x
        }
          Left "Exception" -> return $ Left "exception"
          Left "Error"     -> do
            TCP.closeSock sock
            return $ Left "Error"

