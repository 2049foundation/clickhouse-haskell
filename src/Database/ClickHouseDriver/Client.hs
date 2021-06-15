-- Copyright (c) 2020-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
----------------------------------------------------------------------------

{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}


-- | This module provides implementations of user's APIs
--

module Database.ClickHouseDriver.Client
  ( -- * Data Fetch and Insert
    query,
    queryWithInfo,
    deploySettings,
    insertMany,
    insertOneRow,
    ping,
    Database.ClickHouseDriver.Client.fetch,
    fetchWithInfo,
    execute,
    withCKConnected
  )
where

import Database.ClickHouseDriver.Connection
    ( ping',
      sendQuery,
      sendData,
      processInsertQuery,
      receiveResult, withConnect )
import Database.ClickHouseDriver.Defines
    ( _DEFAULT_COMPRESSION_SETTING,
      _DEFAULT_DATABASE,
      _DEFAULT_HOST_NAME,
      _DEFAULT_PASSWORD,
      _DEFAULT_PORT_NAME,
      _DEFAULT_USERNAME )
import qualified Database.ClickHouseDriver.Defines      as Defines
import Database.ClickHouseDriver.Types
    ( ConnParams(..),
      CKResult(CKResult, query_result),
      TCPConnection(TCPConnection),
      defaultQueryInfo,
      ClickhouseType(..),
      ServerInfo(..),
      Context(..) )

import Control.Concurrent.Async ( mapConcurrently )
import Control.Exception ( SomeException, try )
import Data.Hashable ( Hashable(hashWithSalt) )
import Data.Typeable ( Typeable )
import Z.Data.Vector ( Vector )
import Haxl.Core
    ( putFailure,
      putSuccess,
      dataFetch,
      initEnv,
      runHaxl,
      stateEmpty,
      stateGet,
      stateSet,
      BlockedFetch(..),
      DataSource(fetch),
      DataSourceName(..),
      PerformFetch(SyncFetch),
      Env(states),
      GenHaxl,
      ShowP(..),
      StateKey(State) )
import Text.Printf ( printf )
import Data.Time.Clock ( NominalDiffTime )
import Data.Default.Class ( def, Default )
import Z.IO.UV.UVStream (UVStream)
import qualified Z.IO.Buffered as ZB
import qualified Z.Data.Vector as Z
import qualified Z.Data.ASCII as Z
import qualified Z.Data.Parser as P
import Z.Data.CBytes (fromBytes)
import Z.IO.Network.SocketAddr (PortNumber(..))



-- | GADT 
data Query a where
  FetchData :: String
               -- ^ SQL statement such as "SELECT * FROM table"
             -> Query (Either String CKResult)
               -- ^ result data in Haskell type and additional information

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
    mapConcurrently (fetchData resource) blockedFetches
    return ()

instance StateKey Query where
  data State Query = CKResource (ZB.BufferedInput,ZB.BufferedOutput,Context)

instance Default ConnParams where
    def = ConnParams{
       username'    = _DEFAULT_USERNAME
      ,host'        = fromBytes _DEFAULT_HOST_NAME
      ,port'        = PortNumber _DEFAULT_PORT_NAME
      ,password'    = _DEFAULT_PASSWORD
      ,compression' = _DEFAULT_COMPRESSION_SETTING
      ,database'    = _DEFAULT_DATABASE
    }

-- | fetch data
fetchData :: State Query->BlockedFetch Query->IO ()
fetchData (CKResource (inBuffer, outBuffer, ctx@(Context c_info s_info c_setting)))  fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (Z.pack $ Z.c2w <$> q, var')
  e <- Control.Exception.try $ do
    sendQuery outBuffer s_info queryStr Nothing
    sendData outBuffer ctx "" Nothing
    ZB.readParseChunk (P.parseChunk $ receiveResult s_info defaultQueryInfo) inBuffer
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Either String CKResult))

deploySettings :: ZB.BufferedInput
   -> ZB.BufferedOutput
   -> Context
   -> IO (Env () w)
deploySettings i o ctx =
  initEnv (stateSet (CKResource (i, o, ctx)) stateEmpty) ()

-- | create client with given information such as username, host name and password etc. 
withCKConnected ::ConnParams -> (Env () w -> IO ()) -> IO ()
withCKConnected params f
  = withConnect params \(i, o, ctx) -> do
      env <- deploySettings i o ctx
      f env

-- | fetch data alone with query information
fetchWithInfo :: String->GenHaxl u w (Either String CKResult)
fetchWithInfo = dataFetch . FetchData

-- | Fetch data
fetch :: String
        -- ^ SQL SELECT command
       ->GenHaxl u w (Either String [[ClickhouseType]])
        -- ^ result wrapped in Haxl monad for other tasks run with concurrency.
fetch str = do
  result_with_info <- fetchWithInfo str
  case result_with_info of
    Right CKResult{query_result=r}->return $ Right r
    Left err -> return $ Left err

-- | query result contains query information.
queryWithInfo :: String->Env () w->IO (Either String CKResult)
queryWithInfo query source = runHaxl source (executeQuery query)
  where
    executeQuery :: String->GenHaxl u w (Either String CKResult)
    executeQuery = dataFetch . FetchData

-- | query command
query :: Env () w
        -- ^ Haxl environment for connection
       ->String
        -- ^ Query command for "SELECT" and "SHOW" only
       ->IO (Either String [[ClickhouseType]])
query source cmd = do
  query_with_info <- queryWithInfo cmd source
  case query_with_info of
    Right CKResult{query_result=r}->return $ Right r
    Left err->return $ Left err

-- | For general use e.g. creating table,
-- multiple queries, multiple insertions. 
execute :: Env u w -> GenHaxl u w a -> IO a
execute = runHaxl

insertMany :: Env () w->String->[[ClickhouseType]]->IO Z.Bytes
insertMany source cmd items = do
  let st :: Maybe (State Query) = stateGet $ states source
  case st of
    Nothing             -> error "No Connection."
    Just (CKResource (i, o, ctx)) ->
      processInsertQuery (i, o) ctx (Z.pack $ Z.c2w <$> cmd) Nothing items

insertOneRow :: Env () w
              ->String
              -- ^ SQL command
              ->[ClickhouseType]
              -- ^ a row of local clickhouse data type to be serialized and inserted. 
              ->IO Z.Bytes
              -- ^ The resulting bytestring indicates success or failure.
insertOneRow source cmd items = insertMany source cmd [items]

-- | ping pong 
ping :: ConnParams->Env () w->IO()
ping params source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> print "empty source"
    Just (CKResource (i, o, ctx))
     -> ping' params Defines._DEFAULT_PING_WAIT_TIME (i, o) >>= print