{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ClientProtocol where
import Data.Vector
import Data.ByteString

_HELLO = 0 :: Word
_QUERY = 1
_DATA  = 2
_CANCEL = 3
_PING = 4
_TABLES_STATUS_REQUEST = 5


typeStr :: Vector ByteString
typeStr = fromList ["Hello", "Query", "Data", "Cancel", "Ping", "TablesStatusRequest"]

toString :: Int->ByteString
toString n 
    | n > 5 || n < 0 = "Unknown Packet"
    | otherwise = typeStr ! n
