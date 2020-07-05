module Main where

import           ClickHouseDriver
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client
import           Data.ByteString

(<*>) :: Int->Int->Int
x <*> y = x * y

main :: IO()
main = do
    deSettings <- defaultHttpConnection
    env <- setupEnv deSettings
    res3 <- runQuery env (getTextM ["SHOW DATABASES", "SHOW DATABASES"])
    res4 <- runQuery env (getTextM $ Just "SHOW DATABASES")
    print "Text: "
    print res3



