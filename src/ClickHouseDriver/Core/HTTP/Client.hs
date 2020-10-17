-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

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

module ClickHouseDriver.Core.HTTP.Client
  ( settings,
    setupEnv,
    runQuery,
    getByteString,
    getJSON,
    getText,
    getTextM,
    getJsonM,
    insertOneRow,
    insertMany,
    ping,
    insertFromFile,
    defaultHttpClient,
    httpClient,
    exec
  )
where

import           ClickHouseDriver.Core.Column
import           ClickHouseDriver.Core.Defines         as Defines
import           ClickHouseDriver.Core.HTTP.Connection
import           ClickHouseDriver.Core.HTTP.Helpers
import           ClickHouseDriver.Core.HTTP.Types
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad.State.Lazy
import qualified Data.ByteString                       as BS
import           Data.ByteString.Char8                 (pack)
import qualified Data.ByteString.Lazy                  as LBS
import           Data.ByteString.Lazy.Builder          (char8, lazyByteString,
                                                        toLazyByteString)
import qualified Data.ByteString.Lazy.Char8            as C8
import           Data.Hashable
import qualified Data.HashMap.Strict                   as HM
import qualified Data.Text                             as T
import           Data.Text.Encoding
import           Data.Text.Internal.Lazy               (Text)
import           Data.Typeable
import           Haxl.Core
import           Network.HTTP.Client                   (Manager,
                                                        RequestBody (..),
                                                        defaultManagerSettings,
                                                        httpLbs, method,
                                                        newManager,
                                                        parseRequest,
                                                        requestBody,
                                                        responseBody,
                                                        streamFile)
import qualified Network.Simple.TCP                    as TCP
import           Network.Socket                        (SockAddr, Socket)
import           System.FilePath
import           System.IO                             hiding (char8)
import qualified System.IO.Streams                     as Streams
import           Text.Printf

{-Implementation in Haxl-}
--
data HttpClient a where
  FetchByteString :: String -> HttpClient BS.ByteString
  FetchJSON :: String -> HttpClient BS.ByteString
  FetchCSV :: String -> HttpClient BS.ByteString
  FetchText :: String -> HttpClient BS.ByteString
  Ping :: HttpClient BS.ByteString

deriving instance Show (HttpClient a)

deriving instance Typeable HttpClient

deriving instance Eq (HttpClient a)

instance ShowP HttpClient where showp = show

instance Hashable (HttpClient a) where
  hashWithSalt salt (FetchByteString cmd) = hashWithSalt salt cmd
  hashWithSalt salt (FetchJSON cmd) = hashWithSalt salt cmd
  hashWithSalt salt (FetchCSV cmd) = hashWithSalt salt cmd
  hashWithSalt salt Ping = hashWithSalt salt ("ok"::BS.ByteString)

instance DataSourceName HttpClient where
  dataSourceName _ = "ClickhouseDataSource"

instance DataSource u HttpClient where
  fetch (Settings settings) _flags _usrenv = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    res <- mapConcurrently (fetchData settings) blockedFetches
    case res of
      [()] -> return ()

instance StateKey HttpClient where
  data State HttpClient = Settings HttpConnection

settings :: HttpConnection -> Haxl.Core.State HttpClient
settings = Settings

-- | fetch function
fetchData ::
  HttpConnection -> --Connection configuration
  BlockedFetch HttpClient -> --fetched data
  IO ()
fetchData settings fetches = do
  let (queryWithType, var) = case fetches of
        BlockedFetch (FetchJSON query) var' -> (query ++ " FORMAT JSON", var')
        BlockedFetch (FetchCSV query) var' -> (query ++ " FORMAT CSV", var')
        BlockedFetch (FetchByteString query) var' -> (query, var')
        BlockedFetch Ping var' -> ("ping", var')
  e <- Control.Exception.try $ do
    case settings of
      HttpConnection _ _ _ _ mng -> do
        url <- genURL settings queryWithType
        req <- parseRequest url
        ans <- responseBody <$> httpLbs req mng
        return $ LBS.toStrict ans
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (BS.ByteString))
      
-- | Fetch data from ClickHouse client in the text format.
getByteString :: String -> GenHaxl u w BS.ByteString
getByteString = dataFetch . FetchByteString

getText :: String -> GenHaxl u w T.Text
getText cmd = fmap decodeUtf8 (getByteString cmd)

-- | Fetch data from ClickHouse client in the JSON format.
getJSON :: String -> GenHaxl u w JSONResult
getJSON cmd = fmap extract (dataFetch $ FetchJSON cmd)

-- | Fetch data from Clickhouse client with commands warped in a Traversable monad.
getTextM :: (Monad m, Traversable m) => m String -> GenHaxl u w (m T.Text)
getTextM = mapM getText

getJsonM :: (Monad m, Traversable m) => m String -> GenHaxl u w (m JSONResult)
getJsonM = mapM getJSON

exec :: String->Env HttpConnection w->IO (Either C8.ByteString String)
exec cmd' env = do
  let cmd = C8.pack cmd'
  let settings@(HttpConnection _ _ _ _ mng) = userEnv env
  url <- genURL settings ""
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req{ method = "POST"
  , requestBody = RequestBodyLBS cmd} mng
  if ans /= ""
    then return $ Left ans -- error message
    else return $ Right "Created successfully"

insertOneRow :: String
             -> [ClickhouseType]
             -> Env HttpConnection w
             -> IO (Either C8.ByteString String)
insertOneRow table_name arr environment = do
  let row = toString arr
  let cmd = C8.pack ("INSERT INTO " ++ table_name ++ " VALUES " ++ row)
  let settings@(HttpConnection _ _ _ _ mng) = userEnv environment
  url <- genURL settings ""
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req{ method = "POST"
  , requestBody = RequestBodyLBS cmd} mng
  if ans /= ""
    then return $ Left ans -- error message
    else return $ Right "Inserted successfully"

insertMany :: String
           -> [[ClickhouseType]]
           -> Env HttpConnection w
           -> IO(Either C8.ByteString String)
insertMany table_name rows environment = do
  let rowsString = map (lazyByteString . C8.pack . toString) rows
      comma =  char8 ','
      preset = lazyByteString $ C8.pack $ "INSERT INTO " <> table_name <> " VALUES "
      togo = preset <> (foldl1 (\x y-> x <> comma <> y) rowsString)
  let settings@(HttpConnection _ _ _ _ mng) = userEnv environment
  url <- genURL settings ""
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req{method = "POST"
  , requestBody = RequestBodyLBS $ toLazyByteString togo} mng
  print "inserted successfully"
  if ans /= ""
    then return $ Left ans
    else return $ Right "Successful insertion"

insertFromFile :: String->Format->FilePath->Env HttpConnection w->IO(Either C8.ByteString String)
insertFromFile table_name format file environment = do
  fileReqBody <- streamFile file
  let settings@(HttpConnection _ _ _ _ mng) = userEnv environment
  url <- genURL settings ("INSERT INTO " <> table_name 
    <> case format of
          CSV->" FORMAT CSV"
          JSON->" FORMAT JSON"
          TUPLE->" VALUES")
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req {method = "POST"
  , requestBody = fileReqBody} mng
  if ans /= ""
    then return $ Left ans -- error message
    else return $ Right "Inserted successfully"

ping :: GenHaxl u w BS.ByteString
ping = dataFetch $ Ping

-- | Default environment
setupEnv :: (MonadIO m)=>HttpConnection -> m (Env HttpConnection w)
setupEnv csetting = liftIO $ initEnv (stateSet (settings csetting) stateEmpty) csetting

defaultHttpClient :: (MonadIO m)=>m (Env HttpConnection w)
defaultHttpClient = liftIO $ defaultHttpConnection >>= setupEnv

httpClient :: (MonadIO m)=> String->String-> m(Env HttpConnection w)
httpClient user password = liftIO $ httpConnect user password Defines._DEFAULT_HTTP_PORT Defines._DEFAULT_HOST >>= setupEnv

-- | rename runHaxl function.
{-# INLINE runQuery #-}
runQuery :: (MonadIO m)=> Env u w -> GenHaxl u w a -> m a
runQuery env haxl = liftIO $ runHaxl env haxl