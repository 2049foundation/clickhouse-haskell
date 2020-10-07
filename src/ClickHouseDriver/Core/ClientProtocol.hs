{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ClientProtocol where

import           Data.ByteString (ByteString)
import           Data.Vector     (Vector, fromList, (!?))


-- Name, version, revision, default DB
_HELLO = 0 :: Word

-- Query id, settings,
_QUERY = 1 :: Word

-- A block of data
_DATA = 2 :: Word

-- Cancel the query execution
_CANCEL = 3 :: Word

-- Check that the connection to the server is alive
_PING = 4 :: Word

_TABLES_STATUS_REQUEST = 5 :: Word

_COMPRESSION_ENABLE = 1 :: Word

_COMPRESSION_DISABLE = 0 :: Word

_COMPRESSION_METHOD_LZ4 = 1 :: Word

_COMPRESSION_METHOD_LZ4HC = 2 :: Word

_COMPRESSION_METHOD_ZSTD = 3 :: Word

_COMPRESSION_METHOD_BYTE_LZ4 = 0x82

_COMPRESSION_METHOD_BYTE_ZSTD = 0x90

typeStr :: Vector ByteString
typeStr = fromList ["Hello", "Query", "Data", "Cancel", "Ping", "TablesStatusRequest"]

toString :: Int -> ByteString
toString n = 
  case typeStr !? n of
    Nothing -> "Unknown Packet"
    Just t -> t