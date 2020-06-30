module Main where

import           ClickHouseDriver.HttpConnection as CC
import           ClickHouseDriver.Query
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client


main2 :: IO ()
main2 = do
    manager <- newManager defaultManagerSettings
    env <- initEnv (stateSet (httpState manager) stateEmpty) ()
    res <- runHaxl env (getURL "http://default:12345612341@localhost:8123/ping")
    print res

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



