{-# LANGUAGE OverloadedStrings #-}
module ClickHouseDriver.Core.ConnectionPool () where

import           ClickHouseDriver.Core.Connection
import           ClickHouseDriver.Core.Types
import           Data.ConnectionPool
import           Data.ConnectionPool.Internal.ResourcePoolParams
import           Data.Default.Class
import           Data.Streaming.Network
import           Network.Socket
import           Data.Time.Clock

createClickhousePool :: Int
                      ->NominalDiffTime
                      ->Int
                      ->IO (ConnectionPool TcpClient)
createClickhousePool numberOfStripes resourceIdleTimeout numberOfResourcesPerStripe
    = createTcpClientPool 
        (ResourcePoolParams 
         numberOfStripes
         resourceIdleTimeout 
         numberOfResourcesPerStripe)
        (clientSettingsTCP 9000 "127.0.0.1")

testing :: IO ()
testing = do
    pool <- createTcpClientPool
        (ResourcePoolParams 1 0.5 1)
        (clientSettingsTCP 9000 "127.0.0.1")
    print "pool"