{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ServerProtocol where
 
import Data.Vector        
import Data.ByteString    (ByteString)
import Data.Word

_HELLO = 0 :: Word16
_DATA  = 1 :: Word16
_EXCEPTION = 2 :: Word16
_PROGRESS = 3:: Word16
_PONG      = 4:: Word16
_END_OF_STREAM = 5:: Word16
_PROFILE_INFO = 6:: Word16
_TOTAL        =7:: Word16
_EXTREMES     =8:: Word16
_TABLES_STATUS_RESPONSE= 9:: Word16
_LOG          =10:: Word16
_TABLE_COLUMNS =11:: Word16


typeStr :: Vector ByteString
typeStr = fromList ["Hello", "Data", "Exception", "Progress", "Pong", "EndOfStream",
        "ProfileInfo", "Totals", "Extremes", "TablesStatusResponse", "Log",
        "TableColumns"]

toString :: Int->ByteString
toString n
    | n > 11 || n < 0 = "Unknown Packet"
    | otherwise = typeStr ! n 