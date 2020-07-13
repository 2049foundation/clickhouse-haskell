{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP#-}
module ClickHouseDriver.Core.Defines
  (
    defaultHttpConnection,
    _BUFFER_SIZE,
    _DBMS_NAME,
    _CLIENT_NAME,
    _CLIENT_VERSION_MAJOR,
    _CLIENT_VERSION_MINOR,
    _CLIENT_REVISION
  )
where

import ClickHouseDriver.Core.Types
import Data.ByteString.Internal
import Network.HTTP.Client
import Network.Socket (SockAddr, Socket)
import Network.Simple.TCP

#define DEFAULT_PORT "9000"
_DEFAULT_SECURE_PORT = 9440
_DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264
_DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554

_DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903
 -- Legacy above.
_DBMS_MIN_REVISION_WITH_CLIENTca_INFO  = 54032
_DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE =  54058
_DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO =  54060
_DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
_DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401
_DBMS_MIN_REVISION_WITH_SERVER_LOGS = 54406
_DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410
_DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420
_DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429

-- Timeouts
_DBMS_DEFAULT_CONNECT_TIMEOUT_SEC = 10
_DBMS_DEFAULT_TIMEOUT_SEC = 300 
_DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC = 5
_DEFAULT_COMPRESS_BLOCK_SIZE = 1048576
_DEFAULT_INSERT_BLOCK_SIZE = 1048576
_DBMS_NAME  = "ClickHouse"      :: ByteString
_CLIENT_NAME = "haskell-driver" :: ByteString
_CLIENT_VERSION_MAJOR = 18 :: Word
_CLIENT_VERSION_MINOR = 10 :: Word
_CLIENT_VERSION_PATCH = 3
_CLIENT_REVISION = 54429   :: Word

_STRINGS_ENCODING = "utf-8" :: ByteString
#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""
_DEFAULT_HTTP_PORT = 8123
_BUFFER_SIZE       = 1048576 :: Int

defaultHttpConnection :: IO (ClickHouseConnection)
defaultHttpConnection = do
  mng <- newManager defaultManagerSettings
  return
    HttpConnection
      { httpHost = DEFAULT_HOST_NAME,
        httpPassword = DEFAULT_PASSWORD,
        httpPort = _DEFAULT_HTTP_PORT,
        httpUsername = DEFAULT_USERNAME,
        httpManager = mng
      }

defaultTCPConnection :: IO (ClickHouseConnection)
defaultTCPConnection = do
  (sock, sockadrr) <- connectSock DEFAULT_HOST_NAME DEFAULT_PORT
  
  return
    TCPConnection
      {
        tcpHost = DEFAULT_HOST_NAME,
        tcpPassword = DEFAULT_PASSWORD,
        tcpUsername = DEFAULT_USERNAME,
        tcpSocket   = sock,
        tcpSockAdrr = sockadrr,
        tcpPort     = DEFAULT_PORT
      }
