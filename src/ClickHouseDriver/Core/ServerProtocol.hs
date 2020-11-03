{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ServerProtocol where

import           Data.ByteString (ByteString)
import           Data.Vector     (Vector, fromList, (!?))


-- Name, version, revision
_HELLO :: Word
_HELLO = 0 :: Word

-- A block of data
_DATA :: Word
_DATA = 1 :: Word

-- The exception during query execution
_EXCEPTION :: Word
_EXCEPTION = 2 :: Word

-- Query execution process rows read, bytes read
_PROGRESS :: Word
_PROGRESS = 3 :: Word

-- ping response
_PONG :: Word
_PONG = 4 :: Word

-- All packets were transimitted
_END_OF_STREAM :: Word
_END_OF_STREAM = 5 :: Word

-- Packet with profiling info
_PROFILE_INFO :: Word
_PROFILE_INFO = 6 :: Word

-- A block with totals
_TOTAL :: Word
_TOTAL = 7 :: Word

-- A block with minimums and maximums
_EXTREMES :: Word
_EXTREMES = 8 :: Word

-- A response to TableStatus request
_TABLES_STATUS_RESPONSE :: Word
_TABLES_STATUS_RESPONSE = 9 :: Word

-- A System logs of the query execution
_LOG :: Word
_LOG = 10 :: Word

-- Columns' description for default values calculation
_TABLE_COLUMNS :: Word
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
toString n = 
  case typeStr !? n of
    Nothing -> "Unknown Packet"
    Just t -> t