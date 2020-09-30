{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeSynonymInstances       #-}

module ClickHouseDriver.Core.Client
  ( query,
    executeWithInfo,
    deploySettings,
    client,
    defaultClient,
    closeClient,
    insertMany,
    insertOneRow,
    ping,
    withQuery
  )
where

import           ClickHouseDriver.Core.Block
import           ClickHouseDriver.Core.Column       hiding (length)
import           ClickHouseDriver.Core.Connection
import           ClickHouseDriver.Core.Defines
import qualified ClickHouseDriver.Core.Defines      as Defines
import           ClickHouseDriver.Core.Types
import           ClickHouseDriver.IO.BufferedReader
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad.State                hiding (State)
import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Char8              as C8
import           Data.Hashable
import           Data.Typeable
import           Data.Vector                        hiding (length)
import           Haxl.Core
import qualified Network.Simple.TCP                 as TCP
import           Network.Socket
import qualified Network.URI.Encode                 as NE
import           Text.Printf

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""
#define DEFAULT_PORT_NAME "9000"
#define DEFAULT_DATABASE "default"
#define DEFAULT_COMPRESSION_SETTING False

{-# INLINE _DEFAULT_PING_WAIT_TIME #-}
_DEFAULT_PING_WAIT_TIME = 10000


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
          Nothing   -> error "Empty server information"
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
client (Left e)    = error e
client (Right tcp) = initEnv (stateSet (Settings tcp) stateEmpty) ()

executeWithInfo :: String->Env () w->IO (CKResult)
executeWithInfo query source = runHaxl source (executeQuery query)
  where
    executeQuery :: String -> GenHaxl u w CKResult
    executeQuery = dataFetch . FetchData

query :: Env () w ->String->IO (Vector (Vector ClickhouseType))
query source cmd = do
  CKResult{query_result=r} <- executeWithInfo cmd source
  return r

withQuery :: Env () w->String->(Vector (Vector ClickhouseType)->IO a)->IO a
withQuery source cmd f = query source cmd >>= f

insertMany :: Env () w->String->[[ClickhouseType]]->IO(BS.ByteString)
insertMany source cmd items = do
  let st :: Maybe (State Query) = stateGet $ states source
  let tcp =
        case st of
          Nothing             -> error "No Connection."
          Just (Settings tcp) -> tcp
  processInsertQuery tcp (C8.pack cmd) Nothing items

insertOneRow :: Env () w->String->[ClickhouseType]->IO(BS.ByteString)
insertOneRow source cmd items = insertMany source cmd [items]

ping :: Env () w->IO()
ping source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> print "empty source"
    Just (Settings tcp)
     -> ping' _DEFAULT_PING_WAIT_TIME tcp >>= print

closeClient :: Env () w -> IO()
closeClient source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> return ()
    Just (Settings TCPConnection{tcpSocket=sock})
     -> TCP.closeSock sock
