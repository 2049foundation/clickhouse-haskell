-- | Definition of types

module Database.ClickHouseDriver.HTTP.Types
  ( JSONResult (..),
    Cmd,
    Haxl,
    Format(..),
    HttpConnection(..),
    HttpParams(..)
  )
where

import           Data.Aeson           (Value)
import           Data.ByteString      (ByteString)
import           Data.HashMap.Strict  (HashMap)
import           Data.Text            (Text)
import           Haxl.Core            (GenHaxl)           
import           Network.HTTP.Client ( Manager )        


type JSONResult = Either ByteString [HashMap Text Value]

type Cmd = String

type Haxl a = GenHaxl () a

data Format = CSV | JSON | TUPLE
    deriving Eq

data HttpParams 
  = HttpParams
      {
        httpHost :: !String,
        httpPort :: {-# UNPACK #-}  !Int,
        httpUsername :: !String,
        httpPassword :: !String,
        httpDatabase :: Maybe String
      }

data HttpConnection
  = HttpConnection
      { httpParams :: !HttpParams,
        -- ^ basic parameters
        httpManager ::  !Manager
        -- ^ http manager 
      }