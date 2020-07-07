{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances    #-}
module ClickHouseDriver.Helpers (
    extract,
    repl,
    toStrict,
    UrlType (..)
) where 

import           ClickHouseDriver.Types 
import qualified Data.Aeson                as JP
import           Data.Attoparsec.ByteString     
import qualified Data.ByteString           as B
import qualified Data.ByteString.Lazy      as BL
import           Data.Hashable
import qualified Data.HashMap.Strict       as HM
import qualified Data.Text                 as T
import           Data.Text                 (pack)
import           Data.Text.Internal.Lazy   (Text)
import           Data.Text.Lazy.Encoding
import           Data.Typeable
import           Data.Vector               (toList)
import qualified Data.ByteString.Lazy.Internal as BLI
import qualified Data.ByteString.Internal      as BI
import           Foreign.ForeignPtr
import           Foreign.Ptr
import qualified Data.ByteString.Char8         as C8

-- | Trim JSON data
extract :: C8.ByteString->JSONResult
extract val = do
    let res = (parse JP.json (val))
    case res of
        Fail e1 e2 e3-> Left e1
        Done txt datas->Right $ getData res where
            getData :: Result JP.Value->[HM.HashMap T.Text JP.Value]
            getData (Done _ (JP.Object x)) = do
                let values = HM.lookup (pack "data") x
                    values' = fmap (\(JP.Array arr)->toList arr) values
                    values'' = case values' of
                            Nothing  -> []
                            Just arr -> arr
                    values''' = fmap (\(JP.Object x)->x) values'' in
                    values'''
            getData _ = []

-- | replace spaces with "%20"
repl :: String->String
repl []       = []
repl (' ':cs) = "%20" ++ repl cs
repl (x:xs)   = x: repl xs

toStrict :: BL.ByteString -> B.ByteString
toStrict BLI.Empty = B.empty
toStrict (BLI.Chunk c BLI.Empty) = c
toStrict lb = BI.unsafeCreate len $ go lb
    where
    len = BLI.foldlChunks (\l sb -> l + B.length sb) 0 lb
    go  BLI.Empty _   = return ()
    go (BLI.Chunk (BI.PS fp s l) r) ptr =
        withForeignPtr fp $ \p -> do
            BI.memcpy ptr (p `plusPtr` s) (fromIntegral l)
            go r (ptr `plusPtr` l)

genURL :: ClickHouseConnection->Cmd->String
genURL HttpConnection{httpHost=host,httpPassword=pw,httpPort=port,httpUsername=usr} cmd =
    let basic = "http://" ++ usr ++ ":" ++ pw ++"@"++ host ++ ":" ++ (show port) ++ "/?query="
        res = basic ++ (repl cmd) in
    res

class UrlType a where
    genUrl :: ClickHouseConnection->Cmd->a
    genTcp :: ClickHouseConnection->Cmd->a
-- TODO
instance UrlType String where
    genUrl = genURL
-- TODO
instance UrlType C8.ByteString where
    genUrl http cmd = C8.pack $ genURL http cmd