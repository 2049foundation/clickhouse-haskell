{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ClientProtocol where

import Data.ByteString
import Data.Vector

_HELLO = 0 :: Word

_QUERY = 1 :: Word

_DATA = 2 :: Word

_CANCEL = 3 :: Word

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
toString n
  | n > 5 || n < 0 = "Unknown Packet"
  | otherwise = typeStr ! n