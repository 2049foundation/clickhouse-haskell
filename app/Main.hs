{-# LANGUAGE OverloadedStrings #-}
import Database.ClickHouseDriver
    ( ClickhouseType(CKInt16, CKString),
      defaultClient,
      query,
      insertOneRow,
      ping )

main :: IO ()
main = do
    conn <- defaultClient
    ping conn
    --query conn ("CREATE TABLE IF NOT EXISTS big " ++ 
     --       "(`str` String, `int` Int16, `fix` FixedString(3))" ++ "ENGINE = Memory")
    insertOneRow conn "INSERT INTO big VALUES" [CKString "cacd", CKInt16 16, CKString "124"]
    q <- query conn "SELECT * FROM big"
    print q