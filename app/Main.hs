module Main where

import           ClickHouseDriver
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client


main :: IO()
main = do

    deSettings <- defaultConnection
    env <- setupEnv deSettings
    res3 <- runQuery env (getTextM ["SHOW DATABASES", "SHOW DATABASES"])
    print "Text: "
    print res3



