-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}

-- | Implementation of data types for internal use  Most users should
-- import "ClickHouseDriver.Core" instead.
--

module Database.ClickHouseDriver.Types
  ( ServerInfo (..),
    TCPConnection (..),
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
    ConnParams(..)
  )
where

import qualified Database.ClickHouseDriver.Defines      as Defines
import Database.ClickHouseDriver.IO.BufferedReader
    (readVarInt, readBinaryUInt8 )
import Database.ClickHouseDriver.IO.BufferedWriter
    ( writeVarUInt, writeBinaryUInt8, writeBinaryInt32)
import Data.Default.Class ( Default(..) )
import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Word ( Word8, Word16, Word32, Word64 )
import GHC.Generics ( Generic )
import           Network.Socket                     (SockAddr, Socket)
import qualified Z.Data.Parser as P
import Z.Data.Vector (Bytes, Vector, pack)
import qualified Z.Data.Builder as B
import Z.IO.Network ( HostName, PortNumber, AddrInfo, UVStream)
import Z.IO.UV.UVStream (UVStream)
import Z.Data.Parser (Parser)
import ListT

-----------------------------------------------------------

-----------------------------------------------------------
data BlockInfo = Info
  { is_overflows :: !Bool,
    bucket_num :: {-# UNPACK #-} !Int32
  } 
  deriving Show

writeBlockInfo :: BlockInfo->B.Builder ()
writeBlockInfo Info{is_overflows, bucket_num} = do
  writeVarUInt 1
  writeBinaryUInt8 (if is_overflows then 1 else 0)
  writeVarUInt 2
  writeBinaryInt32 bucket_num
  writeVarUInt 0

data Block = ColumnOrientedBlock
  { columns_with_type :: [(Bytes, Bytes)],
    cdata :: [[ClickhouseType]],
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
  | CKString !Bytes
  | CKTuple (Vector ClickhouseType)
  | CKArray (Vector ClickhouseType)
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
  { name :: {-# UNPACK #-} !Bytes,
    version_major :: {-# UNPACK #-} !Word,
    version_minor :: {-# UNPACK #-} !Word,
    version_patch :: {-# UNPACK #-} !Word,
    revision :: !Word,
    timezone :: Maybe Bytes,
    display_name :: {-# UNPACK #-} !Bytes
  }
  deriving (Show)

---------------------------------------------------------
data TCPConnection = TCPConnection
  { tcpHost :: {-# UNPACK #-} !HostName,
    -- ^ host name, default = "localhost" 
    tcpPort :: {-# UNPACK #-} !PortNumber,
    -- ^ port number, default = "8123"
    tcpUsername :: {-# UNPACK #-} !Bytes,
    -- ^ username, default = "default"
    tcpPassword :: {-# UNPACK #-} !Bytes,

    database :: !Bytes,
    -- ^ password, dafault = ""
    -- ^ server and client informations
    tcpCompression :: {-# UNPACK #-} !Word
    -- ^ should the data be compressed or not. Not applied yet. 
  }
  deriving (Show)

------------------------------------------------------------------
data ClientInfo = ClientInfo
  { client_name :: {-# UNPACK #-} !Bytes,
    interface :: Interface,
    client_version_major :: {-# UNPACK #-} !Word,
    client_version_minor :: {-# UNPACK #-} !Word,
    client_version_patch :: {-# UNPACK #-} !Word,
    client_revision :: {-# UNPACK #-} !Word,
    initial_user :: {-# UNPACK #-} !Bytes,
    initial_query_id :: {-# UNPACK #-} !Bytes,
    initial_address :: {-# UNPACK #-} !Bytes,
    quota_key :: {-# UNPACK #-} !Bytes,
    query_kind :: QueryKind
  }
  deriving (Show)

getDefaultClientInfo :: Bytes -> ClientInfo
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

-------------------------------------------------------------------
data ClientSetting 
  = ClientSetting {
      insert_block_size ::{-# UNPACK #-} !Word,
      strings_as_bytes :: !Bool,
      strings_encoding ::{-# UNPACK #-} !Bytes
  }
  deriving Show

-------------------------------------------------------------------
data Interface = TCP | HTTP
  deriving (Show, Eq)

data QueryKind = NO_QUERY | INITIAL_QUERY | SECOND_QUERY
  deriving (Show, Eq)

data Context = Context
  { client_info :: Maybe ClientInfo,
    server_info :: ServerInfo,
    client_setting :: Maybe ClientSetting
  }
  deriving Show

data Packet
  = Block {queryData :: !Block}
  | Progress {prog :: !Progress}
  | StreamProfileInfo {profile :: !BlockStreamProfileInfo}
  | MultiString !(Bytes, Bytes)
  | ErrorMessage String
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

readProgress :: Word -> P.Parser Progress
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

readBlockStreamProfileInfo :: P.Parser BlockStreamProfileInfo
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
 { query_result :: [[ClickhouseType]],
   query_info :: !QueryInfo
 }
 deriving Show
-------------------------------------------------------------------------
data ConnParams = ConnParams{
      host'        :: !HostName,
      port'        :: !PortNumber,
      username'    :: !Bytes,
      password'    :: !Bytes,
      database'    :: !Bytes,
      compression' :: !Bool
    }
  deriving (Show, Generic)
