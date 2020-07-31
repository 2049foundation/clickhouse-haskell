{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

module ClickHouseDriver.Core.Column where

import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Control.Monad.State.Lazy
import Data.Binary
import qualified Data.ByteString as BS
import Data.ByteString (ByteString, isPrefixOf)
import Data.ByteString.Builder
import Data.ByteString.Char8 (readInt)
import Data.Int
import Data.Traversable
import Data.Vector
import qualified Data.Vector as V
import Data.Word

class (Foldable m) => Sequential (m :: * -> *) where
  length :: forall a. m a -> Int
  gen :: forall a. Int -> (Int -> a) -> m a
  iterateM :: (Monad f) => forall a. Int -> f a -> f (m a)

data ClickhouseType
  = CKBool Bool
  | CKInt8 Int8
  | CKInt16 Int16
  | CKInt32 Int32
  | CKInt64 Int64
  | CKUInt8 Word8
  | CKUInt16 Word16
  | CKUInt32 Word32
  | CKUInt64 Word64
  | CKString ByteString
  | CKFixedLengthString Int ByteString
  | CKArray (Vector ClickhouseType)
  | CKDecimal Float
  | CKDateTime
  | Nullable ClickhouseType
  | CKNothing
  | CKNull
  deriving (Show, Eq)

instance Sequential [] where
  length = Prelude.length
  gen n f = f <$> [1 .. n]
  iterateM n s = V.toList <$> V.replicateM n s

instance Sequential Vector where
  length = V.length
  gen n f = V.generate n f
  iterateM n s = V.replicateM n s

readStatePrefix :: Reader Word64
readStatePrefix = readBinaryUInt64

getColumnWithSpec :: (Sequential t) => Int -> ByteString -> Reader (t ClickhouseType)
getColumnWithSpec n_rows spec
  | "String" `isPrefixOf` spec = iterateM n_rows (CKString <$> readBinaryStr)
  | "Array" `isPrefixOf` spec = undefined
  | "FixedString" `isPrefixOf` spec = do
    let l = BS.length spec
    let strnumber = BS.take (l - 13) (BS.drop 12 spec)
    let number = case readInt strnumber of
          Nothing -> 0 -- This can't happen
          Just (x, _) -> x
    result <- iterateM n_rows (readFixedLengthString number)
    return result
  | "DateTime" `isPrefixOf` spec = undefined
  | "Tuple" `isPrefixOf` spec = undefined
  | "Nullable" `isPrefixOf` spec = undefined
  | "LowCardinality" `isPrefixOf` spec = undefined
  | "Decimal" `isPrefixOf` spec = undefined
  | "SimpleAggregateFunction" `isPrefixOf` spec = undefined
  | "Enum" `isPrefixOf` spec = undefined
  | "Int" `isPrefixOf` spec = readIntColumn n_rows spec
  | "UInt" `isPrefixOf` spec = readIntColumn n_rows spec
  | otherwise = error "Unknown Type"

readIntColumn :: (Sequential t) => Int -> ByteString -> Reader (t ClickhouseType)
readIntColumn n_rows "Int8" = iterateM n_rows (CKInt8 <$> readBinaryInt8)
readIntColumn n_rows "Int16" = iterateM n_rows (CKInt16 <$> readBinaryInt16)
readIntColumn n_rows "Int32" = iterateM n_rows (CKInt32 <$> readBinaryInt32)
readIntColumn n_rows "Int64" = iterateM n_rows (CKInt64 <$> readBinaryInt64)
readIntColumn n_rows "UInt8" = iterateM n_rows (CKUInt8 <$> readBinaryUInt8)
readIntColumn n_rows "UInt16" = iterateM n_rows (CKUInt16 <$> readBinaryUInt16)
readIntColumn n_rows "UInt32" = iterateM n_rows (CKUInt32 <$> readBinaryUInt32)
readIntColumn n_rows "UInt64" = iterateM n_rows (CKUInt64 <$> readBinaryUInt64)
readIntColumn _ _ = error "Not integer type"

readFixedLengthString :: Int -> Reader ClickhouseType
readFixedLengthString strlen = (CKFixedLengthString strlen) <$> (readBinaryStrWithLength strlen)