{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}

module ClickHouseDriver.HttpConnection (
  httpState,
  getURL
) where

import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as LBS
import           Data.Hashable
import           Data.List
import           Data.Typeable
import           Haxl.Core
import           Network.HTTP.Client
import           Network.HTTP.Types.Status (statusCode)

import qualified Network.Socket            as NS
import qualified Data.Text                 as Text
import           Data.Text                 (Text)
import           Text.Printf
import           Data.Maybe                
import           Control.Exception
import           Control.Concurrent.Async



data HttpRequest a where
  GetURL :: String -> HttpRequest LBS.ByteString
deriving instance Typeable HttpRequest
deriving instance Show (HttpRequest a)
instance ShowP HttpRequest where showp = show
deriving instance Eq (HttpRequest a)
instance Hashable (HttpRequest a) where
  hashWithSalt salt (GetURL u) = hashWithSalt salt u
instance DataSourceName HttpRequest where
  dataSourceName _ = "HttpDataSource"
instance StateKey HttpRequest where
  data State HttpRequest = HttpState Manager
instance DataSource u HttpRequest where
    fetch (HttpState mgr) _flags _userEnv = SyncFetch $ \blockedFetches->do
       printf "Fetching %d urls.\n" (length blockedFetches)
       res <- mapConcurrently (fetchURL mgr) blockedFetches
       case res of
         [()] -> return ()

httpState :: Manager -> State HttpRequest
httpState = HttpState

fetchURL :: Manager --http connection configuration
          ->BlockedFetch HttpRequest --results
          ->IO()
fetchURL mgr (BlockedFetch (GetURL url) var) = do
  e <- Control.Exception.try $ do
    req <- parseRequest url
    responseBody <$> httpLbs req mgr
  either (putFailure var) (putSuccess var)(e :: Either SomeException LBS.ByteString)

getURL :: String -> GenHaxl u w LBS.ByteString
getURL = dataFetch . GetURL


