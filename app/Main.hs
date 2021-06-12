{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedStrings #-}
import Database.ClickHouseDriver
import qualified Data.List as L
import Data.Default.Class ( Default(def) ) 

main :: IO ()
main = withCKConnected def $ \env -> do
    x <- query env "SHOW TABLES"
    putStrLn "query result is :"
    print x
