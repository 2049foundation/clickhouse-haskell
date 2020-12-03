module Main where

import           Test.ColumnSpec
import           Test.HTTPSpec
import           Test.IO

main :: IO()
main = do
    runTests
    columnSpec
    httpSpec
    
