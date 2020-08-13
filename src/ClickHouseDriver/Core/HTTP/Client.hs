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
    ping
  )
where

import ClickHouseDriver.Core.HTTP.Connection
import ClickHouseDriver.Core.HTTP.Helpers
import ClickHouseDriver.Core.HTTP.Types
import Control.Concurrent.Async
import Control.Exception
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.HashMap.Strict as HM
import Data.ByteString.Lazy.Builder (toLazyByteString, lazyByteString)
import Data.Hashable
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Text.Internal.Lazy (Text)
import Data.Typeable
import Haxl.Core
import Network.HTTP.Client
  ( Manager,
    defaultManagerSettings,
    httpLbs,
    newManager,
    parseRequest,
    responseBody,
    method,
    requestBody,
    RequestBody(..)
  )
import Network.HTTP.Types.Status (statusCode)
import qualified Network.Simple.TCP as TCP 
import Network.Socket (SockAddr, Socket)
import Text.Printf
import Data.ByteString.Char8     (pack)
import Control.Monad.State.Lazy
import ClickHouseDriver.Core.Column 

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
        url <- genURL settings (replace queryWithType)
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

insertOneRow :: String
             -> [ClickhouseType]
             -> HttpConnection
             -> IO ()
insertOneRow table_name arr settings@(HttpConnection _ _ _ _ mng) = do
  let row = toString arr
  let cmd = C8.pack ("INSERT INTO " ++ table_name ++ " VALUES " ++ row)
  url <- genURL settings ""
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req{ method = "POST"
  , requestBody = RequestBodyLBS cmd} mng
  if ans /= ""
    then error ("Errror message: " ++ C8.unpack ans)
    else print ("Inserted successfully")

insertMany :: String
           -> [[ClickhouseType]]
           -> HttpConnection
           -> IO()
insertMany table_name rows settings@(HttpConnection _ _ _ _ mng) = do
  let rowsString = map (lazyByteString . C8.pack . toString) rows
      comma = lazyByteString ","
      preset = lazyByteString $ C8.pack $ "INSERT INTO " <> table_name <> " VALUES "
      togo = preset <> (foldl1 (\x y-> x <> comma <> y) rowsString)
  url <- genURL settings ""
  req <- parseRequest url
  ans <- responseBody <$> httpLbs req{method = "POST"
  , requestBody = RequestBodyLBS $ toLazyByteString togo} mng
  print "inserted successfully"
  if ans /= ""
    then error ("Errror message: " ++ C8.unpack ans)
    else print ("Inserted successfully")

insertLargeFromFile = undefined

ping :: GenHaxl u w BS.ByteString
ping = dataFetch $ Ping

-- | Default environment
setupEnv ::HttpConnection -> IO (Env () w)
setupEnv csetting = initEnv (stateSet (settings csetting) stateEmpty) ()

-- | rename runHaxl function.
runQuery :: Env u w -> GenHaxl u w a -> IO a
runQuery = runHaxl