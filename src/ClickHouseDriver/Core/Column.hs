module ClickHouseDriver.Core.Column where

import Data.Binary
import Data.ByteString
import Data.Vector
import Control.Monad.State.Lazy
import Data.ByteString.Builder
import ClickHouseDriver.IO.BufferedWriter
import ClickHouseDriver.IO.BufferedReader
import Data.Word
import Data.Int

data BaseColumn = BaseColumn {
    ch_type :: Maybe ByteString,
    hs_type :: Maybe ByteString,
    null_value :: Int
}



class Column a where
    writeData :: Writer a
    readData :: Reader a
    readStatePrefix :: a->Reader Word16
    writeStatePrefix :: a->Writer Word
