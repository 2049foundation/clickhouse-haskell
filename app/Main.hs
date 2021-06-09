{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedStrings #-}
import Database.ClickHouseDriver
    ( ClickhouseType(CKInt16, CKString),
      defaultClient,
      query,
      insertOneRow,
      ping)
import qualified Data.List as L

main :: IO ()
main = do
    conn <- defaultClient
    ping conn
    --query conn ("CREATE TABLE IF NOT EXISTS big " ++ 
     --       "(`str` String, `int` Int16, `fix` FixedString(3))" ++ "ENGINE = Memory")
    --insertOneRow conn "INSERT INTO big VALUES" [CKString "cacd", CKInt16 16, CKString "124"]
    Right q <- query conn "SELECT SADDRESS, SCITY FROM supplier LIMIT 10"
    print $ L.transpose q