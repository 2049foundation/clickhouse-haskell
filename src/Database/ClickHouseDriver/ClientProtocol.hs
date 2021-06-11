-- Copyright (c) 2020-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
-------------------------------------------------------------------------
-- This module provides constant for client protocols. 
{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouseDriver.ClientProtocol where

import           Data.ByteString (ByteString)
import           Data.Vector     (Vector, fromList, (!?))
import qualified Z.Data.Vector   as Z
import Data.Maybe ( fromMaybe )


-- Name, version, revision, default DB
_HELLO :: Word
_HELLO = 0 :: Word

-- Query id, settings,
_QUERY :: Word
_QUERY = 1 :: Word

-- A block of data
_DATA :: Word
_DATA = 2 :: Word

-- Cancel the query execution
_CANCEL :: Word
_CANCEL = 3 :: Word

-- Check that the connection to the server is alive
_PING :: Word
_PING = 4 :: Word

_TABLES_STATUS_REQUEST :: Word
_TABLES_STATUS_REQUEST = 5 :: Word

_COMPRESSION_ENABLE :: Word
_COMPRESSION_ENABLE = 1 :: Word

_COMPRESSION_DISABLE :: Word
_COMPRESSION_DISABLE = 0 :: Word

_COMPRESSION_METHOD_LZ4 :: Word
_COMPRESSION_METHOD_LZ4 = 1 :: Word

_COMPRESSION_METHOD_LZ4HC :: Word
_COMPRESSION_METHOD_LZ4HC = 2 :: Word

_COMPRESSION_METHOD_ZSTD :: Word
_COMPRESSION_METHOD_ZSTD = 3 :: Word

_COMPRESSION_METHOD_BYTE_LZ4 :: Integer
_COMPRESSION_METHOD_BYTE_LZ4 = 0x82

_COMPRESSION_METHOD_BYTE_ZSTD :: Integer
_COMPRESSION_METHOD_BYTE_ZSTD = 0x90

typeStr :: Vector Z.Bytes
typeStr = fromList ["Hello", "Query", "Data", "Cancel", "Ping", "TablesStatusRequest"]

toString :: Int -> Z.Bytes
toString n =
  Data.Maybe.fromMaybe "Unknown Packet" (typeStr !? n)