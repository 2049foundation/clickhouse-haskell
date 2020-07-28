{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances#-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ExistentialQuantification #-}
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
import qualified Data.Vector as V
import Data.Traversable


data BaseColumn = BaseColumn {
    ch_type :: Maybe ByteString,
    hs_type :: Maybe ByteString,
    null_value :: Int
}

class Sequential (m :: * -> *) where
    length :: forall a. m a -> Int
    gen :: forall a. Int->(Int->a)->m a

data FixedLengthString = StringLen Int ByteString

data CKNull = CKNull 

class CKType a where
 --   writeOut :: a->IOWriter w

instance (Num a)=>CKType a

instance CKType ByteString

instance CKType Bool

instance CKType FixedLengthString where
    
instance CKType CKNull

instance (CKType a)=>CKType (Vector a)

instance Sequential [] where
    length = Prelude.length
    gen n f = f <$> [1..n]

instance Sequential Vector where
    length = V.length
    gen n f = V.generate n f

class (CKType a, Sequential t, Monoid (t a))=>Column (t :: * -> *) a where
    writeData :: a->IOWriter (t a)
    writeStatePrefix :: a->Word->IOWriter (t a)

readStatePrefix :: Reader Word64
readStatePrefix = do
    n <- readBinaryUInt64
    return n

readColumn :: (Column t a)=>Int->ByteString->ByteString-> t a
readColumn n_rows spec source
    | "String" `isPrefixOf` spec = undefined
    | "Array" `isPrefixOf` spec = undefined
    | "FixedString" `isPrefixOf` spec = undefined
    | "DateTime" `isPrefixOf` spec = undefined
    | "Tuple" `isPrefixOf` spec = undefined
    | "Nullable" `isPrefixOf` spec = undefined
    | "LowCardinality" `isPrefixOf` spec = undefined
    | "Decimal" `isPrefixOf` spec = undefined
    | "SimpleAggregateFunction" `isPrefixOf` spec = undefined
    | "Enum" `isPrefixOf` spec = undefined
    | "Int" `isPrefixOf` spec = readIntColumn n_rows spec
    | "UInt" `isPrefixOf` spec = readIntColumn n_rows spec
    | "Float" `isPrefixOf` spec = undefined
    | otherwise = error "Unknown Type"

readIntColumn :: (Column t a)=>Int->ByteString->t a
readIntColumn n_rows "Int8" = undefined
readIntColumn n_rows "Int16" = undefined
readIntColumn n_rows "Int32" = undefined
readIntColumn n_rows "Int64" = undefined
readIntColumn n_rows "UInt8" = undefined
readIntColumn n_rows "UInt16" = undefined
readIntColumn n_rows "UInt32" = undefined
readIntColumn n_rows "UInt64" = undefined
readIntColumn _ _ = error "Not integer type"