{-# LANGUAGE ApplicativeDo #-}
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
  )
where

import ClickHouseDriver.Core.Block
import ClickHouseDriver.Core.Column hiding (length)
import ClickHouseDriver.Core.Connection
import ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.Defines as Defines
import Control.Concurrent.Async
import Control.Exception
import Control.Monad.State
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import Data.Hashable
import Data.Typeable
import Data.Vector hiding (length)
import Haxl.Core
import qualified Network.Simple.TCP as TCP
import Network.Socket
import Text.Printf

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

settings :: TCPConnection -> Haxl.Core.State Query
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
    maybeRaw <- TCP.recv sock Defines._BUFFER_SIZE
    let raw = case maybeRaw of
          Nothing -> ""
          Just x -> x
    (results, _) <- runStateT (receiveResult server_info) raw
    return results
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Vector (Vector ClickhouseType)))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp = initEnv (stateSet (settings tcp) stateEmpty) ()

execute :: String -> GenHaxl u w (Vector (Vector ClickhouseType))
execute = dataFetch . FetchData