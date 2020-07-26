{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ServerProtocol where

import Data.ByteString (ByteString)
import Data.Vector
import Data.Word

_HELLO = 0 :: Word

_DATA = 1 :: Word

_EXCEPTION = 2 :: Word

_PROGRESS = 3 :: Word

_PONG = 4 :: Word

_END_OF_STREAM = 5 :: Word

_PROFILE_INFO = 6 :: Word

_TOTAL = 7 :: Word

_EXTREMES = 8 :: Word

_TABLES_STATUS_RESPONSE = 9 :: Word

_LOG = 10 :: Word

_TABLE_COLUMNS = 11 :: Word

typeStr :: Vector ByteString
typeStr =
  fromList
    [ "Hello",
      "Data",
      "Exception",
      "Progress",
      "Pong",
      "EndOfStream",
      "ProfileInfo",
      "Totals",
      "Extremes",
      "TablesStatusResponse",
      "Log",
      "TableColumns"
    ]

toString :: Int -> ByteString
toString n
  | n > 11 || n < 0 = "Unknown Packet"
  | otherwise = typeStr ! n