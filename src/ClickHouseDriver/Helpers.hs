module ClickHouseDriver.Helpers (
    extract,
    repl,
    genUrl
) where 

import           ClickHouseDriver.Types 
import qualified Data.Aeson                as JP
import qualified Data.Attoparsec.Lazy      as AP
import qualified Data.Attoparsec.Lazy      as DAL
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as LBS
import           Data.Hashable
import qualified Data.HashMap.Strict       as HM
import qualified Data.Text                 as T
import           Data.Text.Internal.Lazy   (Text)
import           Data.Text.Lazy.Encoding
import           Data.Typeable
import           Data.Vector               (toList)


-- | Trim JSON data
extract :: LBS.ByteString->QueryResult
extract val = do
    let res = (AP.parse JP.json (val))
    case res of
        DAL.Fail e1 e2 e3-> Err e1
        DAL.Done txt datas->OK $ getData res where
            getData :: DAL.Result JP.Value->[HM.HashMap T.Text JP.Value]
            getData (DAL.Done _ (JP.Object x)) = do
                let values = HM.lookup (T.pack "data") x
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

-- | generate url
genUrl :: ClickHouseConnection->Cmd->Url
genUrl ClickHouseConnectionSettings{ciHost=host,ciPassword=pw,ciPort=port,ciUsername=usr} cmd =
    let basic = "http://" ++ usr ++ ":" ++ pw ++"@"++ host ++ ":" ++ (show port) ++ "/?query="
        res = basic ++ (repl cmd) in
    res
