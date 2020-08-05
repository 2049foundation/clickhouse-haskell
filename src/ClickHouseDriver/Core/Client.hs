
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ClickHouseDriver.Core.Client
  ( execute,
    deploySettings,
    client
  )
where

import ClickHouseDriver.Core.Block
import ClickHouseDriver.Core.Column hiding (length)
import ClickHouseDriver.Core.Connection
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.Defines as Defines
import Control.Concurrent.Async
import Control.Exception
import Control.Monad.State hiding (State)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import Data.Hashable
import Data.Typeable
import Data.Vector hiding (length)
import Haxl.Core
import qualified Network.Simple.TCP as TCP
import Network.Socket
import Text.Printf
import ClickHouseDriver.IO.BufferedReader

data Query a where
  FetchData :: String -> Query (Vector (Vector ClickhouseType))

deriving instance Show (Query a)

deriving instance Typeable Query

deriving instance Eq (Query a)

instance ShowP Query where showp = show

instance Hashable (Query a) where
  hashWithSalt salt (FetchData cmd) = hashWithSalt salt cmd

instance DataSourceName Query where
  dataSourceName _ = "ClickhouseDataSource"

instance DataSource u Query where
  fetch (Settings settings) _flags _usrenv = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    res <- mapConcurrently (fetchData settings) blockedFetches
    case res of
      [()] -> return ()

instance StateKey Query where
  data State Query = Settings TCPConnection

settings :: TCPConnection -> State Query
settings = Settings

fetchData :: TCPConnection -> BlockedFetch Query -> IO ()
fetchData settings fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    sendQuery queryStr Nothing settings
    sendData "" settings
    let server_info = serverInfo settings
    let sock = tcpSocket settings
    
    {-r1 <- TCP.recv sock 1024
    print r1
    r2 <- TCP.recv sock 1024
    print r2-}


    buf <- createBuffer _BUFFER_SIZE sock
    
    (res, _) <- runStateT (receiveResult server_info) buf
    print "result = "
    print (res)

    TCP.closeSock sock
    return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Vector (Vector ClickhouseType)))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp = initEnv (stateSet (settings tcp) stateEmpty) ()

client :: Either String TCPConnection -> IO(Env () w)
client (Left e) = error e
client (Right tcp) = initEnv (stateSet (settings tcp) stateEmpty) ()

executeQuery :: String -> GenHaxl u w (Vector (Vector ClickhouseType))
executeQuery = dataFetch . FetchData

execute :: String -> Env () w -> IO (Vector (Vector ClickhouseType))
execute query env = runHaxl env (executeQuery query)