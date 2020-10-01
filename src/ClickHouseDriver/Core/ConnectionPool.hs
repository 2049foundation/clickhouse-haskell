{-# LANGUAGE OverloadedStrings #-}
module ClickHouseDriver.Core.ConnectionPool (
    CKPool,
    createCKPool
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
import           Control.Concurrent
import           Control.Monad

data CKConnectionParams = CKParams {
        username' :: !ByteString,
        password' :: !ByteString,
        host' :: !ByteString,
        port' :: !Int,
        compression' :: !Word
    }

data CKPool = CKPool {
        base :: ConnectionPool TcpClient,
        params :: CKConnectionParams
    }

createBasePool ::ResourcePoolParams->IO (ConnectionPool TcpClient)
createBasePool params
    = createTcpClientPool 
        params
        (clientSettingsTCP 9000 "127.0.0.1")

createCKPool :: CKConnectionParams->ResourcePoolParams->IO(ConnectionPool TcpClient)
createCKPool ckParams params = do
    pool <- createBasePool params
    thread1 <- newEmptyMVar
    void . forkIO . withTcpClientConnection pool $ \appData->do
        threadDelay 1000
        appWrite appData "1: what is this?"
        putMVar thread1 ()
    return undefined

defaultBasePool :: IO (ConnectionPool TcpClient)
defaultBasePool = createTcpClientPool def (clientSettingsTCP 9000 "127.0.0.1")