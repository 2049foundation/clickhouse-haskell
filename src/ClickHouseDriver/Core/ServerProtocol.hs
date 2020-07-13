{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.ServerProtocol where
 
import Data.Vector        
import Data.ByteString    (ByteString)

#define HELLO     0
#define DATA      1
#define EXCEPTION 2
#define PROGRESS  3
#define PONG      4
#define END_OF_STREAM 5
#define PROFILE_INFO 6
#define TOTAL        7
#define EXTREMES     8
#define TABLES_STATUS_RESPONSE 9
#define LOG          10
#define TABLE_COLUMNS 11


typeStr :: Vector ByteString
typeStr = fromList ["Hello", "Data", "Exception", "Progress", "Pong", "EndOfStream",
        "ProfileInfo", "Totals", "Extremes", "TablesStatusResponse", "Log",
        "TableColumns"]

toString :: Int->ByteString
toString n
    | n > 11 || n < 0 = "Unknown Packet"
    | otherwise = typeStr ! n 

