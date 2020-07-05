{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeSynonymInstances       #-}

module ClickHouseDriver.Query (
    settings,
    setupEnv,
    runQuery,
    getByteString,
    getJSON,
    getText,
    getTextM,
    getJsonM,
) where


import           ClickHouseDriver.Helpers
import           ClickHouseDriver.Types
import           Control.Concurrent.Async
import           Control.Exception
import qualified Data.Aeson                as JP
import qualified Data.Attoparsec.Lazy      as AP
import qualified Data.Attoparsec.Lazy      as DAL
import qualified Data.ByteString.Lazy      as LBS
import qualified Data.ByteString           as BS
import           Data.Hashable
import qualified Data.HashMap.Strict       as HM
import           Data.Text.Internal.Lazy   (Text)
import qualified Data.Text                 as T
import           Data.Text.Encoding
import           Data.Typeable
import           Haxl.Core
import           Network.HTTP.Client       (Manager, defaultManagerSettings,
                                            httpLbs, newManager, parseRequest,
                                            responseBody)
import           Network.HTTP.Types.Status (statusCode)
import           Text.Parsec
import           Text.Printf
import           Network.Socket            (SockAddr, Socket)
import           Network.Simple.TCP

{-Implementation in Haxl-}
-- 
data ClickHouseQuery a where
    FetchByteString :: String -> ClickHouseQuery (Maybe BS.ByteString)
    FetchJSON       :: String -> ClickHouseQuery (Maybe BS.ByteString)
    FetchCSV        :: String -> ClickHouseQuery (Maybe BS.ByteString)
    FetchText       :: String -> ClickHouseQuery (Maybe BS.ByteString)
deriving instance Show (ClickHouseQuery a)
deriving instance Typeable ClickHouseQuery
deriving instance Eq (ClickHouseQuery a)
instance ShowP ClickHouseQuery where showp = show
instance Hashable (ClickHouseQuery a) where
    hashWithSalt salt (FetchByteString cmd) = hashWithSalt salt cmd
    hashWithSalt salt (FetchJSON cmd)       = hashWithSalt salt cmd
    hashWithSalt salt (FetchCSV cmd)        = hashWithSalt salt cmd
instance DataSourceName ClickHouseQuery where
    dataSourceName _ = "ClickhouseDataSource"
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
            BlockedFetch (FetchJSON query) var' -> (repl (query ++ " FORMAT JSON"), var')
            BlockedFetch (FetchCSV query) var' -> (repl (query ++ " FORMAT CSV"), var')
            BlockedFetch (FetchByteString query) var' -> (query, var')
    e <- Control.Exception.try $ do
        case settings of
            HttpConnection _ _ _ _ mng->do
                let url = genUrl settings queryType              
                req <- parseRequest url
                ans <- responseBody <$> httpLbs req mng
                return $ Just $ toStrict ans
            TCPConnection host port _ _->do
                let prot = genUrl settings queryType
                (sock, sockaddr) <- connectSock host port
                send sock prot
                res <- recv sock 1000
                return res
    either (putFailure var) (putSuccess var) 
     (e :: Either SomeException (Maybe BS.ByteString))

-- | Fetch data from ClickHouse client in the text format.
getByteString :: String->GenHaxl u w (Maybe BS.ByteString)
getByteString = dataFetch . FetchByteString

getText :: String->GenHaxl u w (Maybe T.Text)
getText cmd = fmap (fmap decodeUtf8) (getByteString cmd)

-- | Fetch data from ClickHouse client in the JSON format.
getJSON ::String->GenHaxl u w (Maybe JSONResult)
getJSON cmd = fmap (fmap extract) (dataFetch $ FetchJSON cmd)

-- | Fetch data from Clickhouse client with commands warped in a Traversable monad.
getTextM :: (Monad m, Traversable m)=>m String->GenHaxl u w (m (Maybe T.Text))
getTextM = mapM getText

getJsonM :: (Monad m, Traversable m)=>m String->GenHaxl u w (m (Maybe JSONResult))
getJsonM = mapM getJSON

-- | Default environment
setupEnv :: ClickHouseConnection -> IO (Env () w)
setupEnv csetting = initEnv (stateSet (settings csetting) stateEmpty) ()

-- | rename runHaxl function.
runQuery :: Env u w -> GenHaxl u w a -> IO a
runQuery = runHaxl