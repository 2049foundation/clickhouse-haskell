{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ClickHouseDriver.HTTP.Helpers
  ( extract,
    replace,
    genURL
  )
where

import ClickHouseDriver.HTTP.Connection
import ClickHouseDriver.HTTP.Types
import qualified Data.Aeson as JP
import Data.Attoparsec.ByteString
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Internal as BI
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Internal as BLI
import qualified Data.HashMap.Strict as HM
import Data.Hashable
import Data.Text (pack)
import qualified Data.Text as T
import Data.Text.Internal.Lazy (Text)
import Data.Text.Lazy.Encoding
import Data.Typeable
import Data.Vector (toList)
import Foreign.ForeignPtr
import Foreign.Ptr

-- | Trim JSON data
extract :: C8.ByteString -> JSONResult
extract val = getData $ parse JP.json val
  where
    getData (Fail e _ _)           = Left e
    getData (Done _ (JP.Object x)) = Right $ getData' x
    getData _                      = Right []

    getData' = map getObject . maybeArrToList . HM.lookup (pack "data")

    maybeArrToList Nothing = []
    maybeArrToList (Just x) = toList . getArray $ x

    getArray (JP.Array arr) = arr
    getObject (JP.Object x) = x

-- | replace spaces with "%20"
replace :: String -> String
replace"" = ""
replace (' ' : xs) = "%20" ++ replace xs
replace (x : xs) = x : replace xs

genURL :: HttpConnection->Cmd->String
genURL HttpConnection {httpHost = host, httpPassword = pw, httpPort = port, httpUsername = usr} cmd =
    let basic = "http://" ++ usr ++ ":" ++ pw ++ "@" ++ host ++ ":" ++ (show port) ++ "/?query="
        res = basic ++ (replace cmd)
    in res
