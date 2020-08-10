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
import qualified Data.ByteString.Char8 as C8
import Data.ByteString (ByteString, isPrefixOf)
import Data.ByteString.Builder
import Data.ByteString.Char8 (readInt)
import Data.Int
import Data.Traversable
import Data.Vector (Vector, (!))
import qualified Data.Vector as V
import Data.Word

--Debug 
import Debug.Trace 

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
  | CKNothing
  | CKNull
  deriving (Show, Eq)

readStatePrefix :: Reader Word64
readStatePrefix = readBinaryUInt64

readNull :: Reader Word8
readNull = readBinaryUInt8

getColumnWithSpec ::  Int -> ByteString -> Reader (Vector ClickhouseType)
getColumnWithSpec n_rows spec
  | "String" `isPrefixOf` spec = V.replicateM n_rows (CKString <$> readBinaryStr)
  | "Array" `isPrefixOf` spec = readArray n_rows spec
  | "FixedString" `isPrefixOf` spec = do
    let l = BS.length spec
    let strnumber = BS.take (l - 13) (BS.drop 12 spec)
    let number = case readInt strnumber of
          Nothing -> 0 -- This can't happen
          Just (x, _) -> x
    result <- V.replicateM n_rows (readFixedLengthString number)
    return result
  | "DateTime" `isPrefixOf` spec = undefined
  | "Tuple" `isPrefixOf` spec = undefined
  | "Nullable" `isPrefixOf` spec = readNullable n_rows spec
  
  | "LowCardinality" `isPrefixOf` spec = undefined
  | "Decimal" `isPrefixOf` spec = undefined
  | "SimpleAggregateFunction" `isPrefixOf` spec = undefined
  | "Enum" `isPrefixOf` spec = undefined
  | "Int" `isPrefixOf` spec = readIntColumn n_rows spec
  | "UInt" `isPrefixOf` spec = readIntColumn n_rows spec
  | otherwise = error ("Unknown Type: " Prelude.++ C8.unpack spec)

readIntColumn ::  Int -> ByteString -> Reader (Vector ClickhouseType)
readIntColumn n_rows "Int8" = V.replicateM n_rows (CKInt8 <$> readBinaryInt8)
readIntColumn n_rows "Int16" = V.replicateM n_rows (CKInt16 <$> readBinaryInt16)
readIntColumn n_rows "Int32" = V.replicateM n_rows (CKInt32 <$> readBinaryInt32)
readIntColumn n_rows "Int64" = V.replicateM n_rows (CKInt64 <$> readBinaryInt64)
readIntColumn n_rows "UInt8" = V.replicateM n_rows (CKUInt8 <$> readBinaryUInt8)
readIntColumn n_rows "UInt16" = V.replicateM n_rows (CKUInt16 <$> readBinaryUInt16)
readIntColumn n_rows "UInt32" = V.replicateM n_rows (CKUInt32 <$> readBinaryUInt32)
readIntColumn n_rows "UInt64" = V.replicateM n_rows (CKUInt64 <$> readBinaryUInt64)
readIntColumn _ _ = error "Not an integer type"

readFixedLengthString :: Int -> Reader ClickhouseType
readFixedLengthString strlen = (CKFixedLengthString strlen) <$> (readBinaryStrWithLength strlen)

readDateTime ::  Int -> ByteString -> Reader (Vector ClickhouseType)
readDateTime n_rows spec
          | "DateTime64" `isPrefixOf` spec = undefined
          |  otherwise = undefined 

readNullable :: Int->ByteString->Reader (Vector ClickhouseType)
readNullable n_rows spec = do
    let l = BS.length spec
    let cktype = BS.take (l - 10) (BS.drop 9 spec) -- Read Clickhouse type inside the bracket after the 'Nullable' spec.
    config <- readNullableConfig n_rows spec
    items <- getColumnWithSpec n_rows cktype
    let result = V.generate n_rows (\i->if config ! i == 1 then CKNull else items ! i)
    return result
      where
        {-
          Informal description for this config:
          (\Null | \SOH)^{n_rows}
        -}
        readNullableConfig :: Int->ByteString->Reader (Vector Word8)
        readNullableConfig n_rows spec = do
          config <- readBinaryStrWithLength n_rows
          (return . V.fromList . BS.unpack) config

{-
  Format:
     One element of array of arrays can be represented as tree:
      (0 depth)          [[3, 4], [5, 6]]
                        |               |
      (1 depth)      [3, 4]           [5, 6]
                    |    |           |    |
      (leaf)        3     4          5     6
      Offsets (sizes) written in breadth-first search order. In example above
      following sequence of offset will be written: 4 -> 2 -> 4
      1) size of whole array: 4
      2) size of array 1 in depth=1: 2
      3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4
      After sizes info comes flatten data: 3 -> 4 -> 5 -> 6

      Quoted from https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/columns/arraycolumn.py
-}

readArray :: Int->ByteString->Reader (Vector ClickhouseType)
readArray n_rows spec = do
  specs@(lastSpec, x:xs) <- genSpecs spec [V.fromList [fromIntegral n_rows]]
  let numElem = fromIntegral $ V.sum x
  elems <- getColumnWithSpec numElem lastSpec
  let result = case (combine elems (x:xs)) ! 0 of
             CKArray arr -> arr
             _ -> error "wrong type. This cannot happen"
  return result
  where
    combine :: Vector ClickhouseType -> [Vector Word64] -> Vector ClickhouseType
    combine elems [] = elems
    combine elems (config:configs) = combine embed configs
      where
        intervals = intervalize (fromIntegral <$> config)
        embed = (\(l, r)->cut (l, r - l + 1)) <$> intervals
        cut (a, b) = CKArray $ V.take b (V.drop a elems)

    intervalize :: Vector Int -> Vector (Int, Int)
    intervalize vec = V.drop 1 (V.scanl' (\(a, b) v->(b+1, v+b)) (-1, -1) vec)

    readArraySpec :: Vector Word64->Reader (Vector Word64)
    readArraySpec sizeArr = do
      let arrSum = (fromIntegral . V.sum) sizeArr
      offsets <- V.replicateM arrSum readBinaryUInt64
      let offsets' = V.cons 0 (V.take (arrSum - 1) offsets)
      let sizes = V.zipWith (-) offsets offsets'
      return sizes

    genSpecs :: ByteString->[Vector Word64]-> Reader (ByteString, [Vector Word64])
    genSpecs spec (x:xs) = do
      let l = BS.length spec
      let cktype = BS.take (l - 7) (BS.drop 6 spec)
      if "Array" `isPrefixOf` spec
        then do 
          next <- readArraySpec x
          genSpecs cktype (next:x:xs)
        else
          return (spec, x:xs)


readTuple :: Int->ByteString->Reader (Vector ClickhouseType)
readTuple = undefined