{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}


module ClickHouseDriver.Query (
    getByteString,
    getJSON,
    getText,
    getCSV,
    getTextM,
    getJsonM,
    getCsvM,
    settings,
    defaultEnv,
    runQuery,
    ClickHouseConnection (..),
    QueryResult (..),
) where


import           ClickHouseDriver.Helpers
import           ClickHouseDriver.Types
import           Control.Concurrent.Async
import           Control.Exception
import qualified Data.Aeson                as JP
import qualified Data.Attoparsec.Lazy      as AP
import qualified Data.Attoparsec.Lazy      as DAL
import qualified Data.ByteString.Lazy      as LBS
import           Data.Hashable
import qualified Data.HashMap.Strict       as HM
import           Data.Text.Internal.Lazy   (Text)
import           Data.Text.Lazy.Encoding
import           Data.Typeable
import           Haxl.Core
import           Network.HTTP.Client       (Manager, defaultManagerSettings,
                                            httpLbs, newManager, parseRequest,
                                            responseBody)
import           Network.HTTP.Types.Status (statusCode)
import           Text.CSV
import           Text.CSV.Lazy.ByteString  (CSVResult)
import qualified Text.CSV.Lazy.ByteString  as CP
import           Text.Parsec
import           Text.Printf



data ClickHouseQuery a where
    FetchByteString :: String -> ClickHouseQuery LBS.ByteString
    FetchJSON       :: String -> ClickHouseQuery LBS.ByteString
    FetchCSV        :: String -> ClickHouseQuery LBS.ByteString

deriving instance Show (ClickHouseQuery a)

deriving instance Typeable ClickHouseQuery

deriving instance Eq (ClickHouseQuery a)

instance ShowP ClickHouseQuery where showp = show

instance Hashable (ClickHouseQuery a) where
    hashWithSalt salt (FetchByteString cmd) = hashWithSalt salt cmd
    hashWithSalt salt (FetchJSON cmd)       = hashWithSalt salt cmd
    hashWithSalt salt (FetchCSV cmd)        = hashWithSalt salt cmd

instance DataSourceName ClickHouseQuery where
    dataSourceName _ = "ClickHouseDataSource"

instance DataSource u ClickHouseQuery where
    fetch (Settings settings) _flags _usrenv = SyncFetch $ \blockedFetches->do
       printf "Fetching %d queries.\n" (length blockedFetches)
       res <- mapConcurrently (fetchData settings) blockedFetches
       case res of
         [()] -> return ()

instance StateKey ClickHouseQuery where
    data State ClickHouseQuery = Settings ClickHouseConnection


settings :: ClickHouseConnection -> Haxl.Core.State ClickHouseQuery
settings = Settings

-- | fetch function
fetchData :: ClickHouseConnection --Connection configuration
             ->BlockedFetch ClickHouseQuery --fetched data
             ->IO()
fetchData settings fetches = do
    let (queryType, var) = case fetches of
            BlockedFetch (FetchJSON query) var -> (repl (query ++ " FORMAT JSON"), var)
            BlockedFetch (FetchCSV query) var -> (repl (query ++ " FORMAT CSV"), var)
            BlockedFetch (FetchByteString query) var -> (query, var)
    e <- Control.Exception.try $ do
        manager <- newManager defaultManagerSettings
        let url = genUrl settings queryType
        req <- parseRequest url
        ans <- responseBody <$> httpLbs req manager
        return ans
    either (putFailure var) (putSuccess var) (e :: Either SomeException LBS.ByteString)

-- | Default environment
defaultEnv :: ClickHouseConnection -> IO (Env () w)
defaultEnv csetting = initEnv (stateSet (settings csetting) stateEmpty) ()


-- | Fetch data from ClickHouse client in the text format.
getByteString :: String->GenHaxl u w LBS.ByteString
getByteString = dataFetch . FetchByteString

getText :: String->GenHaxl u w Text
getText cmd = fmap decodeUtf8 (getByteString cmd)

-- | Fetch data from ClickHouse client in the JSON format.
getJSON ::String->GenHaxl u w QueryResult
getJSON cmd = fmap extract (dataFetch $ FetchJSON cmd)

-- | Fetch data from ClickHouse client in the CSV format.
getCSV :: String->GenHaxl u w CSVResult
getCSV cmd =  fmap CP.parseCSV (dataFetch $ FetchCSV cmd)

-- | Fetch data from Clickhouse client with commands warped in a Traversable monad.
getTextM :: (Monad m, Traversable m)=>m String->GenHaxl u w (m Text)
getTextM = mapM getText

getCsvM :: (Monad m, Traversable m)=>m String->GenHaxl u w (m CSVResult)
getCsvM = mapM getCSV

getJsonM :: (Monad m, Traversable m)=>m String->GenHaxl u w (m QueryResult)
getJsonM = mapM getJSON

-- | rename runHaxl function.
runQuery = runHaxl