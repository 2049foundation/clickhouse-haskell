{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad.Random
  ( MonadRandom (getRandom, getRandomR),
    Rand,
    RandomGen,
    StdGen,
    evalRandIO,
    replicateM,
  )
import Data.Default.Class (Default (def))
import Data.Int (Int8)
import qualified Data.List as L
import Data.Maybe
import Database.ClickHouseDriver
import Database.ClickHouseDriver.IO.BufferedReader
import Database.ClickHouseDriver.IO.BufferedWriter
import Z.Data.Builder
import Z.Data.Parser hiding (take, takeWhile)
import Z.Data.Vector hiding (foldl', map, take, takeWhile)
import qualified ListT as LT

ckInt32 :: RandomGen g => Rand g ClickhouseType
ckInt32 = CKInt32 <$> getRandomR (-100, 100)

ckInt64 :: RandomGen g => Rand g ClickhouseType
ckInt64 = CKInt64 <$> getRandomR (-1000, 1000)

ckString :: RandomGen g => Rand g ClickhouseType
ckString = do
  size <- getRandomR (0, 7)
  x <- replicateM size getRandom
  let packed = pack x
  return $ CKString packed

ckFixedString :: RandomGen g => Rand g ClickhouseType
ckFixedString = do
  x <- replicateM 5 getRandom
  let packed = pack x
  return $ CKString packed

ckArrayInt64 :: RandomGen g => Rand g ClickhouseType
ckArrayInt64 = do
  size <- getRandomR (0, 6)
  arr <- replicateM size ckInt64
  return $ CKArray $ pack arr

ckInt642Array :: RandomGen g => Rand g ClickhouseType
ckInt642Array = do
  size <- getRandomR (0, 4)
  arr <- replicateM size ckArrayInt64
  return $ CKArray $ pack arr

ckNullableString :: RandomGen g => Rand g ClickhouseType
ckNullableString = do
  isnull <- getRandom
  if isnull
    then ckString
    else return CKNull

ckNullableStringArray :: RandomGen g => Rand g ClickhouseType
ckNullableStringArray = do
  size <- getRandomR (0, 4)
  res <- replicateM size ckNullableString
  return $ CKArray $ pack res

ckNullbleString2Array :: RandomGen g => Rand g ClickhouseType
ckNullbleString2Array = do
  size <- getRandomR (0, 4)
  res <- replicateM size ckNullableStringArray
  return $ CKArray $ pack res

ckTuple :: RandomGen g => Rand g ClickhouseType
ckTuple = do
  str <- ckString
  i32 <- ckInt32
  return $ CKTuple $ pack [str, i32]

ckEnum :: Rand StdGen ClickhouseType
ckEnum = do
  (season :: Int8) <- getRandomR (1, 4)
  case season of
    1 -> return $ CKString "spring"
    2 -> return $ CKString "summer"
    3 -> return $ CKString "autumn"
    4 -> return $ CKString "winter"

ckRow :: Rand StdGen [ClickhouseType]
ckRow = do
  int32 <- ckInt32
  str <- ckString
  nullstr <- ckNullableString
  fixstr <- ckFixedString

  int64arr <- ckInt642Array
  nullarr <- ckNullbleString2Array

  tuple <- ckTuple
  enum <- ckEnum
  lowcard <- ckString

  return [int32, str, nullstr, fixstr, int64arr, nullarr, tuple ,enum, lowcard]

ckRows :: Int -> Rand StdGen [[ClickhouseType]]
ckRows n = replicateM n ckRow

main2 :: IO ()
main2 = do
  let inf :: (LT.ListT Maybe Int) = LT.repeat 1
  let trans = LT.traverse Just inf
  let take5 = LT.take 5 inf
  let res = LT.toList take5

  print res

  putStrLn "done!"

main :: IO ()
main = withCKConnected def $ \env -> do
  rows <- evalRandIO  $ ckRows 5
  insertMany
   env
   "INSERT INTO test VALUES"
   rows
  table <- query env "SELECT enum FROM test"
  print table
  putStrLn "done!"
