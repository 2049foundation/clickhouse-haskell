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
  ( query,
    executeWithInfo,
    deploySettings,
    client,
    defaultClient,
    closeClient,
    insertMany,
    insertOneRow
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

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  "12345612341"
#define DEFAULT_PORT_NAME "9000"
#define DEFAULT_DATABASE "default"
#define DEFAULT_COMPRESSION_SETTING False


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
    sendQuery tcpconn queryStr Nothing 
    sendData tcpconn "" Nothing
    let serverInfo = case getServerInfo tcpconn of
          Just info -> info
          Nothing -> error "Empty server information"
    let sock = tcpSocket tcpconn
    buf <- createBuffer _BUFFER_SIZE sock
    (res, _) <- runStateT (receiveResult serverInfo defaultQueryInfo) buf
    return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (CKResult))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp = 
  initEnv (stateSet (Settings tcp) stateEmpty) ()

defaultClient :: IO (Env () w)
defaultClient =
  tcpConnect
    DEFAULT_HOST_NAME
    DEFAULT_PORT_NAME
    DEFAULT_USERNAME
    DEFAULT_PASSWORD
    DEFAULT_DATABASE
    DEFAULT_COMPRESSION_SETTING
    >>= client

client :: Either String TCPConnection -> IO(Env () w)
client (Left e) = error e
client (Right tcp) = initEnv (stateSet (Settings tcp) stateEmpty) ()

executeWithInfo :: String->Env () w->IO (CKResult)
executeWithInfo query env = runHaxl env (executeQuery query)
  where
    executeQuery :: String -> GenHaxl u w CKResult
    executeQuery = dataFetch . FetchData

query :: String -> Env () w -> IO (Vector (Vector ClickhouseType))
query query env = do
  CKResult{query_result=r} <- executeWithInfo query env
  return r

insertMany :: Env () w->String->[[ClickhouseType]]->IO(BS.ByteString)
insertMany env cmd items = do
  let st :: Maybe (State Query) = stateGet $ states env
  let tcp = 
        case st of
          Nothing -> error "No Connection."
          Just (Settings tcp) -> tcp
  processInsertQuery tcp (C8.pack cmd) Nothing items

insertOneRow :: Env () w->String->[ClickhouseType]->IO(BS.ByteString)
insertOneRow env cmd items = insertMany env cmd [items]
  
closeClient :: Env () w -> IO()
closeClient env = do
  let get :: Maybe (State Query) = stateGet $ states env
  case get of
    Nothing -> return ()
    Just (Settings TCPConnection{tcpSocket=sock})
     -> TCP.closeSock sock 