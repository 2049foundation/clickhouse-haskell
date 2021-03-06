-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric     #-}

-- | Implementation of data types for internal use  Most users should
-- import "ClickHouseDriver.Core" instead.
--

module Database.ClickHouseDriver.Types
  ( ServerInfo (..),
    TCPConnection (..),
    getServerInfo,
    getClientInfo,
    getClientSetting,
    ClientInfo (..),
    ClientSetting(..),
    Context (..),
    Interface (..),
    QueryKind (..),
    getDefaultClientInfo,
    Packet (..),
    readProgress,
    readBlockStreamProfileInfo,
    QueryInfo(..),
    Progress(..),
    BlockStreamProfileInfo(..),
    storeElasped,
    storeProfile,
    storeProgress,
    defaultProfile,
    defaultProgress,
    defaultQueryInfo,
    ClickhouseType(..),
    BlockInfo(..),
    Block(..),
    CKResult(..),
    writeBlockInfo,
    ConnParams(..),
    setClientInfo,
    setClientSetting,
    setServerInfo
  )
where

import qualified Database.ClickHouseDriver.Defines      as Defines
import Database.ClickHouseDriver.IO.BufferedReader
    ( Reader, readVarInt, readBinaryUInt8 )
import Database.ClickHouseDriver.IO.BufferedWriter
    ( Writer, writeVarUInt, writeBinaryUInt8, writeBinaryInt32)
import           Data.ByteString                    (ByteString)
import Data.ByteString.Builder ( Builder )
import Data.Default.Class ( Default(..) )
import Data.Int ( Int8, Int16, Int32, Int64 )
import           Data.Vector                        (Vector)
import Data.Word ( Word8, Word16, Word32, Word64 )
import GHC.Generics ( Generic )
import           Network.Socket                     (SockAddr, Socket)

-----------------------------------------------------------

-----------------------------------------------------------
data BlockInfo = Info
  { is_overflows :: !Bool,
    bucket_num :: {-# UNPACK #-} !Int32
  } 
  deriving Show

writeBlockInfo :: BlockInfo->Writer Builder
writeBlockInfo Info{is_overflows, bucket_num} = do
  writeVarUInt 1
  writeBinaryUInt8 (if is_overflows then 1 else 0)
  writeVarUInt 2
  writeBinaryInt32 bucket_num
  writeVarUInt 0

data Block = ColumnOrientedBlock
  { columns_with_type :: Vector (ByteString, ByteString),
    cdata :: Vector (Vector ClickhouseType),
    info :: BlockInfo
  }
  deriving Show
------------------------------------------------------------
data ClickhouseType
  = CKInt8 !Int8
  | CKInt16 !Int16
  | CKInt32 !Int32
  | CKInt64 !Int64
  | CKInt128 !Int64 !Int64
  | CKUInt8 !Word8
  | CKUInt16 !Word16
  | CKUInt32 !Word32
  | CKUInt64 !Word64
  | CKUInt128 !Word64 !Word64
  | CKString !ByteString
  | CKTuple !(Vector ClickhouseType)
  | CKArray !(Vector ClickhouseType)
  | CKDecimal !Float
  | CKDecimal32 !Float
  | CKDecimal64 !Double
  | CKDecimal128 !Double
  | CKIPv4 !(Word8, Word8, Word8, Word8)
  | CKIPv6 !(Word16,Word16,Word16,Word16,
         Word16, Word16, Word16, Word16)
  | CKDate {
    year :: !Integer,
    month :: !Int,
    day :: !Int 
  }
  | CKNull
  deriving (Show, Eq)

----------------------------------------------------------
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

setServerInfo :: Maybe ServerInfo->TCPConnection->TCPConnection
setServerInfo server_info tcp@TCPConnection{context=ctx} 
  = tcp{context=ctx{server_info=server_info}}
---------------------------------------------------------
data TCPConnection = TCPConnection
  { tcpHost :: {-# UNPACK #-} !ByteString,
    -- ^ host name, default = "localhost" 
    tcpPort :: {-# UNPACK #-} !ByteString,
    -- ^ port number, default = "8123"
    tcpUsername :: {-# UNPACK #-} !ByteString,
    -- ^ username, default = "default"
    tcpPassword :: {-# UNPACK #-} !ByteString,
    -- ^ password, dafault = ""
    tcpSocket :: !Socket,
    -- ^ socket for communication
    tcpSockAdrr :: !SockAddr,
    context :: !Context,
    -- ^ server and client informations
    tcpCompression :: {-# UNPACK #-} !Word
    -- ^ should the data be compressed or not. Not applied yet. 
  }
  deriving (Show)

getServerInfo :: TCPConnection->Maybe ServerInfo
getServerInfo TCPConnection{context=Context{server_info=server_info}} = server_info

getClientInfo :: TCPConnection->Maybe ClientInfo
getClientInfo TCPConnection{context=Context{client_info=client_info}} = client_info

getClientSetting :: TCPConnection->Maybe ClientSetting
getClientSetting TCPConnection{context=Context{client_setting=client_setting}} = client_setting
------------------------------------------------------------------
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

setClientInfo :: Maybe ClientInfo -> TCPConnection -> TCPConnection
setClientInfo client_info tcp@TCPConnection{context=ctx}
  = tcp{context=ctx{client_info=client_info}}
-------------------------------------------------------------------
data ClientSetting 
  = ClientSetting {
      insert_block_size ::{-# UNPACK #-} !Word,
      strings_as_bytes :: !Bool,
      strings_encoding ::{-# UNPACK #-} !ByteString
  }
  deriving Show

setClientSetting :: Maybe ClientSetting->TCPConnection->TCPConnection
setClientSetting client_setting tcp@TCPConnection{context=ctx} 
  = tcp{context=ctx{client_setting=client_setting}}

-------------------------------------------------------------------
data Interface = TCP | HTTP
  deriving (Show, Eq)

data QueryKind = NO_QUERY | INITIAL_QUERY | SECOND_QUERY
  deriving (Show, Eq)

data Context = Context
  { client_info :: Maybe ClientInfo,
    server_info :: Maybe ServerInfo,
    client_setting :: Maybe ClientSetting
  }
  deriving Show

data Packet
  = Block {queryData :: !Block}
  | Progress {prog :: !Progress}
  | StreamProfileInfo {profile :: !BlockStreamProfileInfo}
  | MultiString !(ByteString, ByteString)
  | ErrorMessage !String
  | Hello
  | EndOfStream
  deriving (Show)
------------------------------------------------------------
data Progress = Prog
  { rows :: {-# UNPACK #-} !Word,
    bytes :: {-# UNPACK #-} !Word,
    total_rows :: {-# UNPACK #-} !Word,
    written_rows :: {-# UNPACK #-} !Word,
    written_bytes :: {-# UNPACK #-} !Word
  }
  deriving (Show)

instance Default Progress where
  def = defaultProgress

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

defaultProgress :: Progress
defaultProgress = Prog 0 0 0 0 0
----------------------------------------------------------------------
data BlockStreamProfileInfo = ProfileInfo
  { number_rows :: {-# UNPACK #-} !Word,
    blocks :: {-# UNPACK #-} !Word,
    number_bytes :: {-# UNPACK #-} !Word,
    applied_limit :: !Bool,
    rows_before_limit :: {-# UNPACK #-} !Word,
    calculated_rows_before_limit :: !Bool
  }
  deriving Show

instance Default BlockStreamProfileInfo where
  def = defaultProfile

defaultProfile :: BlockStreamProfileInfo
defaultProfile = ProfileInfo 0 0 0 False 0 False

readBlockStreamProfileInfo :: Reader BlockStreamProfileInfo
readBlockStreamProfileInfo = do
  rows <- readVarInt
  blocks <- readVarInt
  bytes <- readVarInt
  applied_limit <- (>= 0) <$> readBinaryUInt8
  rows_before_limit <- readVarInt
  calculated_rows_before_limit <- (>= 0) <$> readBinaryUInt8
  return $ ProfileInfo rows blocks bytes applied_limit rows_before_limit calculated_rows_before_limit
-----------------------------------------------------------------------
data QueryInfo = QueryInfo 
 { profile_info :: !BlockStreamProfileInfo,
   progress :: !Progress,
   elapsed :: {-# UNPACK #-} !Word
 } deriving Show

instance Default QueryInfo where
  def = defaultQueryInfo

storeProfile :: QueryInfo->BlockStreamProfileInfo->QueryInfo
storeProfile (QueryInfo _ progress elapsed) new_profile 
              = QueryInfo new_profile progress elapsed

storeProgress :: QueryInfo->Progress->QueryInfo
storeProgress (QueryInfo profile progress elapsed) new_progress 
              = QueryInfo profile (increment progress new_progress) elapsed

storeElasped :: QueryInfo->Word->QueryInfo
storeElasped (QueryInfo profile progress _)
              = QueryInfo profile progress 

defaultQueryInfo :: QueryInfo
defaultQueryInfo = 
  QueryInfo
  { progress = defaultProgress,
    profile_info = defaultProfile,
    elapsed = 0
  }
-------------------------------------------------------------------------
data CKResult = CKResult
 { query_result :: Vector (Vector ClickhouseType),
   query_info :: !QueryInfo
 }
 deriving Show
-------------------------------------------------------------------------
data ConnParams = ConnParams{
      username'    :: !ByteString,
      host'        :: !ByteString,
      port'        :: !ByteString,
      password'    :: !ByteString,
      compression' :: !Bool,
      database'    :: !ByteString
    }
  deriving (Show, Generic)