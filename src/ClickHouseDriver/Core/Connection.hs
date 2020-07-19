{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}


module ClickHouseDriver.Core.Connection (
    tcpConnect,
    httpConnect,
    defaultHttpConnection,
    defaultTCPConnection,
    ClickHouseConnection(..),
    sendQuery,
    receiveData
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
import qualified ClickHouseDriver.Core.QueryProcessingStage         as Stage
import qualified Data.ByteString.Char8 as C8

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
} deriving Show

data ClickHouseConnection
  = HttpConnection
      { httpHost ::                    !String,
        httpPort :: {-# UNPACK #-}     !Int,
        httpUsername ::                !String,
        httpPassword :: {-# UNPACK #-} !String,
        httpManager ::                 !Manager
      }
  | TCPConnection
      { tcpHost :: {-# UNPACK #-} !ByteString,
        tcpPort :: {-# UNPACK #-} !ByteString,
        tcpUsername :: {-# UNPACK #-} !ByteString,
        tcpPassword :: {-# UNPACK #-} !ByteString,
        tcpSocket   :: {-# UNPACK #-} !Socket,
        tcpSockAdrr ::                !SockAddr,
        serverInfo  :: {-# UNPACK #-} !ServerInfo,
        tcpCompression :: {-#UNPACK #-} !Word
      }

data Packet = Block | Exception {message :: ByteString} | Progress

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
        >>= writeBinaryStr ("ClickHouse " <>_CLIENT_NAME )
        >>= writeVarUInt _CLIENT_VERSION_MAJOR
        >>= writeVarUInt _CLIENT_VERSION_MINOR
        >>= writeVarUInt _CLIENT_REVISION
        >>= writeBinaryStr database
        >>= writeBinaryStr usrname
        >>= writeBinaryStr password
    print ("w = " <> (toLazyByteString w))
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
      then do
        e <- readBinaryStr
        e2 <- readBinaryStr
        e3 <- readBinaryStr
        return $ Left ("exception" <> e <> " " <> e2 <> " " <> e3)
      else
        return $ Left "Error"

defaultTCPConnection :: IO(Either String ClickHouseConnection)
defaultTCPConnection = tcpConnect "localhost" "9000" "default" "12345612341" "default" False

tcpConnect :: ByteString->ByteString->ByteString->ByteString->ByteString->Bool->IO(Either String ClickHouseConnection)
tcpConnect host port user password database compression = do
    (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
    sendHello (database, user, password) sock
    hello <- TCP.recv sock _BUFFER_SIZE
    let isCompressed = if compression then Client._COMPRESSION_ENABLE else Client._COMPRESSION_DISABLE
    case hello of
      Nothing-> return $ Left "Connection failed"
      Just x -> do
        info <- receiveHello x
        print info
        case info of
          Right x -> 
            return $ 
              Right TCPConnection {
                tcpHost = host,
                tcpPort = port,
                tcpUsername = user,
                tcpPassword = password,
                tcpSocket   = sock,
                tcpSockAdrr = sockaddr,
                serverInfo  = x,
                tcpCompression = isCompressed
              }
          Left "Exception" -> return $ Left "exception"
          Left x     -> do
            print ("error is " <> x)
            TCP.closeSock sock
            return $ Left "Connection error"


sendQuery :: ByteString->Maybe ByteString->ClickHouseConnection->IO()
sendQuery query query_id env@TCPConnection{tcpCompression=comp, tcpSocket=sock}= do
  r <- writeVarUInt Client._QUERY mempty
   >>= writeBinaryStr (case query_id of
        Nothing->""
        Just x->x)
   >>= writeInfo env _CLIENT_NAME
   >>= writeVarUInt 0
   >>= writeVarUInt Stage._COMPLETE
   >>= writeVarUInt comp
   >>= writeBinaryStr query
  let lstr = toLazyByteString r
  print ("Query = "<> lstr)
  TCP.sendLazy sock (lstr)


receivePacket :: ByteString->IO()
receivePacket istr = undefined
  --let packet_type = runStateT readVarInt istr


receiveData :: StateT ByteString IO ByteString
receiveData = do
  packet_type <- readVarInt
  result <- readBinaryStr
  return result


writeInfo :: ClickHouseConnection->ByteString->Builder->IO(Builder)
writeInfo (TCPConnection host port username password sock sockaddr 
  ServerInfo{revision=revision,display_name=host_name} comp) client_name builder= do
  let initial_user = "" :: ByteString
  let initial_query_id = "" :: ByteString
  let initial_address = "0.0.0.0:0" :: ByteString
  let quota_key = "" :: ByteString
  let interface = 1 :: Word
  let queryKind = 1 :: Word

  --TODO exceptions
  kind <- writeVarUInt queryKind builder

  initial <- writeBinaryStr initial_user kind
   >>= writeBinaryStr initial_query_id
   >>= writeBinaryStr initial_address

  writeInterface <- writeVarUInt interface initial

  client_info <- writeBinaryStr "darth" writeInterface
    >>= writeBinaryStr host_name
    >>= writeBinaryStr client_name
    >>= writeVarUInt _CLIENT_VERSION_MAJOR
    >>= writeVarUInt _CLIENT_VERSION_MINOR
    >>= writeVarUInt _CLIENT_REVISION
    >>= writeBinaryStr quota_key
    >>= writeVarUInt _CLIENT_VERSION_PATCH
  
  return client_info
  
writeInfo _ _ builder= return builder