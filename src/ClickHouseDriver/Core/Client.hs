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
{-# LANGUAGE RecordWildCards            #-}

module ClickHouseDriver.Core.Client
  ( exec,
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
import           ClickHouseDriver.Core.ConnectionPool
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
import           Data.Pool                          (Pool(..), withResource, destroyAllResources)
import           Data.Time.Clock

{-# INLINE _DEFAULT_PING_WAIT_TIME #-}
_DEFAULT_PING_WAIT_TIME = 10000

{-# INLINE _DEFAULT_USERNAME #-}
_DEFAULT_USERNAME = "default"

{-# INLINE _DEFAULT_HOST_NAME #-}
_DEFAULT_HOST_NAME = "localhost"

{-# INLINE _DEFAULT_PASSWORD #-}
_DEFAULT_PASSWORD =  ""

{-# INLINE _DEFAULT_PORT_NAME #-}
_DEFAULT_PORT_NAME =  "9000"

{-# INLINE _DEFAULT_DATABASE#-}
_DEFAULT_DATABASE =  "default"

{-# INLINE _DEFAULT_COMPRESSION_SETTING #-}
_DEFAULT_COMPRESSION_SETTING =  False


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
  fetch (resource) _flags env = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    res <- mapConcurrently (fetchData resource) blockedFetches
    case res of
      [()] -> return ()

instance StateKey Query where
  data State Query = CKResource TCPConnection
                   | CKPool (Pool TCPConnection)

class Resource a where
  client :: Either String a->IO(Env () w)

fetchData :: State Query-> BlockedFetch Query -> IO ()
fetchData (CKResource tcpconn)  fetch = do
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
fetchData (CKPool pool) fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    withResource pool $ \conn->do
      sendQuery conn queryStr Nothing
      sendData conn "" Nothing
      let serverInfo = case getServerInfo conn of
            Just info -> info
            Nothing -> error "Empty server information"
      let sock = tcpSocket conn
      buf <- createBuffer _BUFFER_SIZE sock
      (res, _) <- runStateT (receiveResult serverInfo defaultQueryInfo) buf
      return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (CKResult))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp =
  initEnv (stateSet (CKResource tcp) stateEmpty) ()

defaultClient :: IO (Env () w)
defaultClient = do
  tcp <- tcpConnect
          _DEFAULT_HOST_NAME
          _DEFAULT_PORT_NAME
          _DEFAULT_USERNAME
          _DEFAULT_PASSWORD
          _DEFAULT_DATABASE
          _DEFAULT_COMPRESSION_SETTING
  case tcp of
    Left e -> client ((Left e) :: Either String (Pool TCPConnection))
    Right conn -> client $ Right conn
  
defaultClientPool :: Int->NominalDiffTime->Int->IO (Env () w)
defaultClientPool numberStripes idleTime maxResources = do
  let params =
        ConnParams{
          username'    = _DEFAULT_USERNAME,
          host'        = _DEFAULT_HOST_NAME,
          port'        = _DEFAULT_PORT_NAME,
          password'    = _DEFAULT_PASSWORD,
          compression' = _DEFAULT_COMPRESSION_SETTING,          
          database'    = _DEFAULT_DATABASE
        }
  pool <- createConnectionPool params numberStripes idleTime maxResources
  client $ Right pool

instance Resource TCPConnection where
  client (Left e) = error e
  client (Right src) = initEnv (stateSet (CKResource src) stateEmpty) ()

instance Resource (Pool TCPConnection) where
  client (Left e) = error e
  client (Right src) = initEnv (stateSet (CKPool src) stateEmpty) ()

executeWithInfo :: String->Env () w->IO (CKResult)
executeWithInfo query source = runHaxl source (executeQuery query)
  where
    executeQuery :: String -> GenHaxl u w CKResult
    executeQuery = dataFetch . FetchData

exec :: Env () w ->String->IO (Vector (Vector ClickhouseType))
exec source cmd = do
  CKResult{query_result=r} <- executeWithInfo cmd source
  return r

withQuery :: Env () w->String->(Vector (Vector ClickhouseType)->IO a)->IO a
withQuery source cmd f = exec source cmd >>= f

insertMany :: Env () w->String->[[ClickhouseType]]->IO(BS.ByteString)
insertMany source cmd items = do
  let st :: Maybe (State Query) = stateGet $ states source
  case st of
    Nothing             -> error "No Connection."
    Just (CKResource tcp) -> processInsertQuery tcp (C8.pack cmd) Nothing items
    Just (CKPool pool) -> 
      withResource pool $ \tcp->do
        processInsertQuery tcp (C8.pack cmd) Nothing items

insertOneRow :: Env () w->String->[ClickhouseType]->IO(BS.ByteString)
insertOneRow source cmd items = insertMany source cmd [items]

ping :: Env () w->IO()
ping source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> print "empty source"
    Just (CKResource tcp)
     -> ping' _DEFAULT_PING_WAIT_TIME tcp >>= print
    Just (CKPool pool)
     -> withResource pool $ \src->
       ping' _DEFAULT_PING_WAIT_TIME src >>= print 

closeClient :: Env () w -> IO()
closeClient source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> return ()
    Just (CKResource TCPConnection{tcpSocket=sock})
     -> TCP.closeSock sock
    Just (CKPool pool)
     -> destroyAllResources pool