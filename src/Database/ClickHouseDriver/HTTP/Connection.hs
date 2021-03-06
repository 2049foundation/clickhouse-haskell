-- Copyright (c) 2020-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Connection pool for HTTP connection. User should import Database.ClickHouseDriver.HTTP instead
module Database.ClickHouseDriver.HTTP.Connection
  ( httpConnect,
    httpConnectDb,
    defaultHttpConnection,
    HttpConnection (..),
    createHttpPool,
  )
where

import Data.Default.Class (Default (..))
import Data.Pool (Pool, createPool)
import Data.Time.Clock (NominalDiffTime)
import Database.ClickHouseDriver.HTTP.Types
  ( HttpConnection (..),
    HttpParams
      ( HttpParams,
        httpDatabase,
        httpHost,
        httpPassword,
        httpPort,
        httpUsername
      ),
  )
import Network.HTTP.Client
  ( defaultManagerSettings,
    newManager,
  )

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""
--TODO change default password to ""

defaultHttpConnection :: IO (HttpConnection)
defaultHttpConnection = httpConnect DEFAULT_USERNAME DEFAULT_PASSWORD 8123 DEFAULT_HOST_NAME

instance Default HttpParams where
  def =
    HttpParams
      { httpHost = DEFAULT_HOST_NAME,
        httpPassword = DEFAULT_PASSWORD,
        httpPort = 8123,
        httpUsername = DEFAULT_USERNAME,
        httpDatabase = Nothing
      }

createHttpPool ::
  HttpParams ->
  Int ->
  NominalDiffTime ->
  Int ->
  IO (Pool HttpConnection)
createHttpPool
  HttpParams
    { httpHost = host,
      httpPassword = password,
      httpPort = port,
      httpUsername = user,
      httpDatabase = db
    } =
    createPool
      ( do
          httpConnectDb user password port host db
      )
      (\_ -> return ())

httpConnect :: String -> String -> Int -> String -> IO (HttpConnection)
httpConnect user password port host =
  httpConnectDb user password port host Nothing

httpConnectDb :: String -> String -> Int -> String -> Maybe String -> IO (HttpConnection)
httpConnectDb user password port host database = do
  mng <- newManager defaultManagerSettings
  return
    HttpConnection
      { httpParams =
          HttpParams
            { httpHost = host,
              httpPassword = password,
              httpPort = port,
              httpUsername = user,
              httpDatabase = database
            },
        httpManager = mng
      }