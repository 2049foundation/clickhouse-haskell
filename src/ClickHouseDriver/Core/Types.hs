{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.Types(
    ServerInfo(..),
    TCPConnection(..),
    ClientInfo(..),
    Context(..),
    Interface(..),
    QueryKind(..),
    getDefaultClientInfo,
) where

import Data.ByteString
import Network.Socket
import ClickHouseDriver.Core.Defines
import ClickHouseDriver.IO.BufferedWriter

data ServerInfo = ServerInfo
  { name :: {-# UNPACK #-} !ByteString,
    version_major :: {-# UNPACK #-} !Word,
    version_minor ::{-# UNPACK #-}  !Word,
    version_patch ::{-# UNPACK #-}  !Word,
    revision :: !Word,
    timezone :: Maybe ByteString,
    display_name ::{-# UNPACK #-}  !ByteString
  }
  deriving Show

data TCPConnection = TCPConnection
  { tcpHost :: {-# UNPACK #-} !ByteString,
    tcpPort :: {-# UNPACK #-} !ByteString,
    tcpUsername :: {-# UNPACK #-} !ByteString,
    tcpPassword :: {-# UNPACK #-} !ByteString,
    tcpSocket :: !Socket,
    tcpSockAdrr :: !SockAddr,
    serverInfo :: {-# UNPACK #-} !ServerInfo,
    tcpCompression :: {-# UNPACK #-} !Word
  }
  deriving Show

data ClientInfo = ClientInfo {
  client_name :: {-#UNPACK#-} !ByteString,
  interface :: Interface,
  client_version_major ::{-# UNPACK #-} !Word,
  client_version_minor ::{-# UNPACK #-} !Word,
  client_version_patch ::{-# UNPACK #-} !Word,
  client_revision :: {-# UNPACK #-} !Word,
  initial_user :: {-# UNPACK #-} !ByteString,
  initial_query_id :: {-# UNPACK #-} !ByteString,
  initial_address :: {-# UNPACK #-} !ByteString,
  quota_key :: {-# UNPACK #-} !ByteString,
  query_kind :: QueryKind
} deriving Show

getDefaultClientInfo :: ByteString->ClientInfo
getDefaultClientInfo name = ClientInfo {
  client_name = name,
  interface = TCP,
  client_version_major = _CLIENT_VERSION_MAJOR,
  client_version_minor = _CLIENT_VERSION_MINOR,
  client_version_patch = _CLIENT_VERSION_PATCH,
  client_revision = _CLIENT_REVISION,
  initial_user = "",
  initial_query_id = "",
  initial_address = "0.0.0.0:0",
  quota_key = "",
  query_kind = INITIAL_QUERY
}

data Interface = TCP | HTTP
      deriving (Show, Eq)

data QueryKind = NO_QUERY | INITIAL_QUERY | SECOND_QUERY
      deriving (Show, Eq)


data Context = Context {
  client_info :: ClientInfo,
  server_info :: ServerInfo
}  deriving Show

data Packet = Block | Exception {message :: ByteString} | Progress
      deriving (Show, Eq)