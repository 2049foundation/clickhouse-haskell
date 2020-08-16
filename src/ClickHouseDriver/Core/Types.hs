{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.Types
  ( ServerInfo (..),
    TCPConnection (..),
    ClientInfo (..),
    Context (..),
    Interface (..),
    QueryKind (..),
    getDefaultClientInfo,
    Packet (..),
    readProgress,
    readBlockStreamProfileInfo,
  )
where

import ClickHouseDriver.Core.Block
import qualified ClickHouseDriver.Core.Defines as Defines
import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Data.ByteString
import Network.Socket
import qualified ClickHouseDriver.Core.Error as Error

-- Debug 
import Debug.Trace

data ServerInfo = ServerInfo
  { name :: {-# UNPACK #-} !ByteString,
    version_major :: {-# UNPACK #-} !Word,
    version_minor :: {-# UNPACK #-} !Word,
    version_patch :: {-# UNPACK #-} !Word,
    revision :: !Word,
    timezone :: Maybe ByteString,
    display_name :: {-# UNPACK #-} !ByteString
  }
  deriving (Show)

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
  deriving (Show)

data ClientInfo = ClientInfo
  { client_name :: {-# UNPACK #-} !ByteString,
    interface :: Interface,
    client_version_major :: {-# UNPACK #-} !Word,
    client_version_minor :: {-# UNPACK #-} !Word,
    client_version_patch :: {-# UNPACK #-} !Word,
    client_revision :: {-# UNPACK #-} !Word,
    initial_user :: {-# UNPACK #-} !ByteString,
    initial_query_id :: {-# UNPACK #-} !ByteString,
    initial_address :: {-# UNPACK #-} !ByteString,
    quota_key :: {-# UNPACK #-} !ByteString,
    query_kind :: QueryKind
  }
  deriving (Show)

getDefaultClientInfo :: ByteString -> ClientInfo
getDefaultClientInfo name =
  ClientInfo
    { client_name = name,
      interface = TCP,
      client_version_major = Defines._CLIENT_VERSION_MAJOR,
      client_version_minor = Defines._CLIENT_VERSION_MINOR,
      client_version_patch = Defines._CLIENT_VERSION_PATCH,
      client_revision = Defines._CLIENT_REVISION,
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

data Context = Context
  { client_info :: ClientInfo,
    server_info :: ServerInfo
  }
  deriving (Show)

data Packet
  = Block {queryData :: Block}
  | Exception {message :: Error.ClickhouseException}
  | Progress {prog :: Progress}
  | StreamProfileInfo {profile :: BlockStreamProfileInfo}
  | EndOfStream
  deriving (Show)

data Progress = Prog
  { rows :: {-# UNPACK #-} !Word,
    bytes :: {-# UNPACK #-} !Word,
    total_rows :: {-# UNPACK #-} !Word,
    written_tows :: {-# UNPACK #-} !Word,
    written_bytes :: {-# UNPACK #-} !Word
  }
  deriving (Show)

increment :: Progress -> Progress -> Progress
increment (Prog a b c d e) (Prog a' b' c' d' e') =
  Prog (a + a') (b + b') (c + c') (d + d') (e + e')

readProgress :: Word -> Reader Progress
readProgress server_revision = do
  rows <- readVarInt
  bytes <- readVarInt

  let revision = server_revision
  total_rows <-
    if revision >= Defines._DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS
      then readVarInt
      else return 0
  if revision >= Defines._DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
    then do
      written_rows <- readVarInt
      written_bytes <- readVarInt
      return $ Prog rows bytes total_rows written_rows written_bytes
    else do
      return $ Prog rows bytes total_rows 0 0

data BlockStreamProfileInfo = ProfileInfo
  { number_rows :: {-# UNPACK #-} !Word,
    blocks :: {-# UNPACK #-} !Word,
    number_bytes :: {-# UNPACK #-} !Word,
    applied_limit :: {-# UNPACK #-} !Bool,
    rows_before_limit :: {-# UNPACK #-} !Word,
    calculated_rows_before_limit :: {-# UNPACK #-} !Bool
  }
  deriving Show

readBlockStreamProfileInfo :: Reader BlockStreamProfileInfo
readBlockStreamProfileInfo = do
  rows <- readVarInt
  blocks <- readVarInt
  bytes <- readVarInt
  applied_limit <- (>= 0) <$> readBinaryUInt8
  rows_before_limit <- readVarInt
  calculated_rows_before_limit <- (>= 0) <$> readBinaryUInt8
  return $ ProfileInfo rows blocks bytes applied_limit rows_before_limit calculated_rows_before_limit
