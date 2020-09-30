{-# LANGUAGE OverloadedStrings #-}
module ClickHouseDriver.Core.ConnectionPool () where

import ClickHouseDriver.Core.Connection
import ClickHouseDriver.Core.Types
import Data.Default.Class
import Network.Socket
import Data.Streaming.Network
import Data.ConnectionPool
import Data.ConnectionPool.Internal.ResourcePoolParams

createClickhousePool :: IO (ConnectionPool TcpClient)
createClickhousePool = undefined

testing :: IO ()
testing = do
    pool <- createTcpClientPool 
        (ResourcePoolParams 1 0.5 1) 
        (clientSettingsTCP 8123 "127.0.0.1")
    print "pool"
