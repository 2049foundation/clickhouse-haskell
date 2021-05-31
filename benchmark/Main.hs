{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
module Main where

import Database.ClickHouseDriver
    ( ConnParams(password'), createClient, query, closeClient, printTranspose)
import Database.ClickHouseDriver.Column (putInformat)
import Data.Default.Class (def)
import Data.ByteString hiding (putStrLn, readFile)
import Data.Time ( diffUTCTime, getCurrentTime )


main :: IO()
main = do
    putStrLn "Start benchmark for Clickhouse-Haskell"
    let params = def :: ConnParams
    conn <- createClient params{password'=""}
    start <- getCurrentTime
    res <- query conn "SELECT * FROM supplier LIMIT 10000"
    case res of
        Left _ -> putStrLn "Failed"
        Right !r -> do 
            --printTranspose r
            putStrLn "success!"
            end <- getCurrentTime
            print $ diffUTCTime end start
            closeClient conn

