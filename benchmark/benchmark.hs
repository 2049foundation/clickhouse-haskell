{-# LANGUAGE OverloadedStrings #-}
module Benchmark (runBench) where

import ClickHouseDriver.Core
import Data.Default.Class (def)
import Data.ByteString hiding (putStrLn)
import Criterion.Main

runBench :: IO()
runBench = defaultMain [
           bgroup "queryBench" [ bench "q1" benchmark1


                               ]

                       ]

benchmark1 :: IO()
benchmark1 = do
    putStrLn "Start benchmark for Clickhouse-Haskell"
    let params = def :: ConnParams
    conn <- createClient params{password'="12345612341"}
    query conn "SELECT * FROM customer LIMIT 10"
    putStrLn "success!"