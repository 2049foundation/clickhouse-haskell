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
{-# LANGUAGE ScopedTypeVariables  #-}

module ClickHouseDriver.Core.Client
  ( execute,
    execute1,
    deploySettings,
    client,
    defaultClient,
    closeClient
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
import qualified Network.URI.Encode as NE
import ClickHouseDriver.Core.Types

data Query a where
  FetchData :: String -> Query (CKResult)

deriving instance Show (Query a)

deriving instance Typeable Query

deriving instance Eq (Query a)

instance ShowP Query where showp = show

instance Hashable (Query a) where
  hashWithSalt salt (FetchData cmd) = hashWithSalt salt cmd

instance DataSourceName Query where
  dataSourceName _ = "ClickhouseServer"

instance DataSource u Query where
  fetch (Settings settings) _flags tcpconn = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    res <- mapConcurrently (fetchData settings) blockedFetches
    case res of
      [()] -> return ()

instance StateKey Query where
  data State Query = Settings TCPConnection

fetchData :: TCPConnection-> BlockedFetch Query -> IO ()
fetchData tcpconn fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    sendQuery queryStr Nothing tcpconn
    sendData "" tcpconn
    let server_info = serverInfo tcpconn
    let sock = tcpSocket tcpconn
    buf <- createBuffer _BUFFER_SIZE sock
    (res, _) <- runStateT (receiveResult server_info defaultQueryInfo) buf
    return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (CKResult))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp = 
  initEnv (stateSet (Settings tcp) stateEmpty) ()

defaultClient :: IO(Env () w)
defaultClient = tcpConnect "localhost" "9000" "default" "12345612341" "default" False >>= client

client :: Either String TCPConnection -> IO(Env () w)
client (Left e) = error e
client (Right tcp) = initEnv (stateSet (Settings tcp) stateEmpty) ()

execute1 :: String->Env () w->IO (CKResult)
execute1 query env = runHaxl env (executeQuery query)
  where
    executeQuery :: String -> GenHaxl u w CKResult
    executeQuery = dataFetch . FetchData

execute :: String -> Env () w -> IO (Vector (Vector ClickhouseType))
execute query env = do
  CKResult{query_result=r} <- execute1 query env
  return r  
  
closeClient :: Env () w -> IO()
closeClient env = do
  let st = states env
  let get :: Maybe (State Query) = stateGet st
  case get of
    Nothing -> return ()
    Just (Settings TCPConnection{tcpSocket=sock})
     -> TCP.closeSock sock 