import Database.ClickHouseDriver ( query, defaultClient )

main :: IO ()
main = do
    conn <- defaultClient
    --query conn ("CREATE TABLE IF NOT EXISTS str_and_int_suite " ++ 
    --        "(`str` String, `int` Int16, `fix` FixedString(3))" ++ "ENGINE = Memory")
    q <- query conn "SELECT * FROM big"
    print q