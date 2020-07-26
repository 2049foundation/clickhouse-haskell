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
  )
where

import ClickHouseDriver.Core.HTTP.Connection
import ClickHouseDriver.Core.HTTP.Helpers
import ClickHouseDriver.Core.HTTP.Types
import Control.Concurrent.Async
import Control.Exception
import qualified Data.Aeson as JP
import qualified Data.Attoparsec.Lazy as AP
import qualified Data.Attoparsec.Lazy as DAL
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as HM
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
  )
import Network.HTTP.Types.Status (statusCode)
import qualified Network.Simple.TCP as TCP 
import Network.Socket (SockAddr, Socket)
import Text.Parsec
import Text.Printf
import Data.Maybe
import Data.ByteString.Char8     (pack)
import Control.Monad.State.Lazy


{-Implementation in Haxl-}
--
data HttpQuery a where
  FetchByteString :: String -> HttpQuery BS.ByteString
  FetchJSON :: String -> HttpQuery BS.ByteString
  FetchCSV :: String -> HttpQuery BS.ByteString
  FetchText :: String -> HttpQuery BS.ByteString

deriving instance Show (HttpQuery a)

deriving instance Typeable HttpQuery

deriving instance Eq (HttpQuery a)

instance ShowP HttpQuery where showp = show

instance Hashable (HttpQuery a) where
  hashWithSalt salt (FetchByteString cmd) = hashWithSalt salt cmd
  hashWithSalt salt (FetchJSON cmd) = hashWithSalt salt cmd
  hashWithSalt salt (FetchCSV cmd) = hashWithSalt salt cmd

instance DataSourceName HttpQuery where
  dataSourceName _ = "ClickhouseDataSource"

instance DataSource u HttpQuery where
  fetch (Settings settings) _flags _usrenv = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    res <- mapConcurrently (fetchData settings) blockedFetches
    case res of
      [()] -> return ()

instance StateKey HttpQuery where
  data State HttpQuery = Settings HttpConnection

settings :: HttpConnection -> Haxl.Core.State HttpQuery
settings = Settings

-- | fetch function
fetchData ::
  HttpConnection -> --Connection configuration
  BlockedFetch HttpQuery -> --fetched data
  IO ()
fetchData settings fetches = do
  let (queryWithType, var) = case fetches of
        BlockedFetch (FetchJSON query) var' -> (query ++ " FORMAT JSON", var')
        BlockedFetch (FetchCSV query) var' -> (query ++ " FORMAT CSV", var')
        BlockedFetch (FetchByteString query) var' -> (query, var')
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

-- | Default environment
setupEnv ::HttpConnection -> IO (Env () w)
setupEnv csetting = initEnv (stateSet (settings csetting) stateEmpty) ()

-- | rename runHaxl function.
runQuery :: Env u w -> GenHaxl u w a -> IO a
runQuery = runHaxl