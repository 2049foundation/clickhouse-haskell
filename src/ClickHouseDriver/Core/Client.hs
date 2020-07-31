{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP  #-}

module ClickHouseDriver.Core.Client (
  execute,
  deploySettings
) where

import ClickHouseDriver.Core.Connection
import ClickHouseDriver.Core.Block
import ClickHouseDriver.Core.Defines
import Haxl.Core
import ClickHouseDriver.Core.Column hiding (length)
import Data.Vector hiding (length)
import Data.Hashable
import Data.Typeable
import Network.Socket
import Text.Printf
import Control.Concurrent.Async
import qualified Data.ByteString as BS
import Control.Exception
import qualified Data.ByteString.Char8 as C8
import Control.Monad.State
import qualified Network.Simple.TCP as TCP

data Query a where
    FetchData :: String->Query (Vector (Vector ClickhouseType))
    
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

settings :: TCPConnection->Haxl.Core.State Query
settings = Settings

fetchData :: TCPConnection->BlockedFetch Query->IO()
fetchData settings fetch = do
  let (queryStr, var) = case fetch of
          BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    sendQuery queryStr Nothing settings
    sendData "" settings
    let server_info = serverInfo settings
    let sock = tcpSocket settings
    maybeRaw <- TCP.recv sock 2048
    let raw = case maybeRaw of
          Nothing-> ""
          Just x -> x
    (results,_) <- runStateT (receiveResult server_info) raw
    return results
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Vector (Vector ClickhouseType)))

deploySettings :: TCPConnection->IO (Env () w)
deploySettings tcp = initEnv (stateSet (settings tcp) stateEmpty) ()

execute :: String->GenHaxl u w (Vector (Vector ClickhouseType))
execute = dataFetch . FetchData