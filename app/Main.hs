{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
import Database.ClickHouseDriver
import qualified Data.List as L
import Data.Default.Class ( Default(def) ) 

import Database.ClickHouseDriver.IO.BufferedWriter
import Database.ClickHouseDriver.IO.BufferedReader

import Z.Data.Vector hiding (map, take, takeWhile, foldl')
import Z.Data.Parser hiding (take, takeWhile)
import Z.Data.Builder
import Data.Maybe 
import qualified Streaming.Prelude as S

main :: IO ()
main = withCKConnected def $ \env -> do
    insertOneRow env "INSERT INTO test VALUES" 
      [CKInt32 1232,
       CKString "world",
       CKString "not null",
       CKString "12345",
       CKArray [CKArray [CKInt64 123, CKInt64 124]], CKArray [], CKArray[],
       CKArray [CKArray [CKString "ahaha", CKNull]],
       CKTuple [CKString "Clickhosue", CKInt32 2123],
       CKString "summer",
       CKString "lowcard"]
    table <- query env "SELECT nullArray FROM test"
    print table
    putStrLn "done!"
        
    
