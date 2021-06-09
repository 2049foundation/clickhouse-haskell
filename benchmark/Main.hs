{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Database.ClickHouseDriver
    ( ConnParams(password'), createClient, query, closeClient)
import Database.ClickHouseDriver.Column (printInformat)
import Data.Default.Class (def)
import Data.ByteString hiding (putStrLn, readFile, map, filter)
import Data.Time ( diffUTCTime, getCurrentTime )

data Mydata = Mydata {
    idstr :: String,
    integer :: Int
} deriving Show

(.$) :: a -> (a -> c) -> c
(.$) = flip ($)

myfunc :: [Int]
myfunc = [1..30] .$ map (+ 1) .$ filter even .$ map (\x->x*x)

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
           -- printInformat r
            putStrLn "success!"
            end <- getCurrentTime
            print $ diffUTCTime end start
            closeClient conn

