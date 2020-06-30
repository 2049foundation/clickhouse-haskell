module Main where

import           ClickHouseDriver
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client


main :: IO()
main = do
    let deSetting = ClickHouseConnectionSettings {
        ciHost = "localhost",
        ciPassword = "12345612341",
        ciPort = 8123,
        ciUsername = "default"
    }
    env <- defaultEnv deSetting
    res3 <- runQuery env (getTextM ["SHOW DATABASES", "SHOW DATABASES"])
    print "Text: "
    print res3



