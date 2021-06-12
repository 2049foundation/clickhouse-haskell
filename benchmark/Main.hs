{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Database.ClickHouseDriver
    ( ConnParams(password'), query)
--import Database.ClickHouseDriver.Column (printInformat)
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
    putChar 'a'

