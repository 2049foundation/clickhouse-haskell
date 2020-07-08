module Main where

import           ClickHouseDriver
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client
import           Data.ByteString        
import           Data.ByteString.Char8

(<*>) :: Int->Int->Int
x <*> y = x * y

main :: IO()
main = do
    deSettings <- defaultHttpConnection
    env <- setupEnv deSettings
    res4 <- runQuery env (getByteString "SHOW DATABASES FORMAT JSON")
    print "Text: "
    Data.ByteString.Char8.putStrLn res4


