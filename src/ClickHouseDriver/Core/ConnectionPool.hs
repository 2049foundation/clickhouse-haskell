{-# LANGUAGE OverloadedStrings #-}
module ClickHouseDriver.Core.ConnectionPool (
    CKPool
) where

import           ClickHouseDriver.Core.Connection
import           ClickHouseDriver.Core.Types
import           Data.ConnectionPool
import           Data.ConnectionPool.Internal.ResourcePoolParams
import           Data.Default.Class
import           Data.Streaming.Network
import           Network.Socket
import           Data.Time.Clock
import           Data.ByteString

data CKPool = CKPool {
        base :: ConnectionPool TcpClient,
        username' :: !ByteString,
        password' :: !ByteString,
        host' :: !ByteString,
        port' :: !Int,
        compression' :: !Word
    }

createBasePool :: Int
                ->NominalDiffTime
                ->Int
                ->IO (ConnectionPool TcpClient)
createBasePool numberOfStripes resourceIdleTimeout numberOfResourcesPerStripe
    = createTcpClientPool 
        (ResourcePoolParams 
         numberOfStripes
         resourceIdleTimeout 
         numberOfResourcesPerStripe)
        (clientSettingsTCP 9000 "127.0.0.1")

defaultBasePool :: IO (ConnectionPool TcpClient)
defaultBasePool = createTcpClientPool def (clientSettingsTCP 9000 "127.0.0.1")

testing :: IO ()
testing = do
    pool <- createTcpClientPool
        (ResourcePoolParams 1 0.5 1)
        (clientSettingsTCP 9000 "127.0.0.1")
    print "pool"