{-# OPTIONS_GHC -F -pgmF hspec-discover #-}

module Spec where

import           Test.Column
import           Test.HTTP

main :: IO()
main = do
    spec
    columnSpec