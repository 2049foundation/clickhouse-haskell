-------------------------------------------------------------------------
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

-- | This module contains the implementations of
--   serialization and deserialization of Clickhouse data types.
module Database.ClickHouseDriver.Column where

import Database.ClickHouseDriver.Types (ClickhouseType (..), Context (..), ServerInfo (..))
import Database.ClickHouseDriver.IO.BufferedReader
  (
    readBinaryInt16,
    readBinaryInt32,
    readBinaryInt64,
    readBinaryInt8,
    readBinaryStr,
    readBinaryUInt128,
    readBinaryUInt16,
    readBinaryUInt32,
    readBinaryUInt64,
    readBinaryUInt8,
  )
import Database.ClickHouseDriver.IO.BufferedWriter
  (
    writeBinaryFixedLengthStr,
    writeBinaryInt16,
    writeBinaryInt32,
    writeBinaryInt64,
    writeBinaryInt8,
    writeBinaryStr,
    writeBinaryUInt128,
    writeBinaryUInt16,
    writeBinaryUInt32,
    writeBinaryUInt64,
    writeBinaryUInt8,
    writeVarUInt,
  )
import Data.Binary (Word64, Word8)
import Data.Bits (shift, (.&.), (.|.))
import qualified Z.Data.Builder as B
import Z.Data.Vector (Bytes)
import qualified Data.HashMap.Strict as Map
import Data.Hashable (Hashable (hash))
import Data.Int (Int32, Int64)
import qualified Data.List as List
import Data.Maybe (fromJust, fromMaybe)
import Data.Time
  ( TimeZone (..),
    addDays,
    diffDays,
    fromGregorian,
    getCurrentTimeZone,
    toGregorian,
  )
import Data.UUID as UUID
  ( fromString,
    fromWords,
    toString,
    toWords,
  )
import Z.Data.Vector (Vector)
import qualified Z.Data.Vector as V
import Foreign.C (CString)
import Network.IP.Addr
  ( IP4 (..),
    IP6 (..),
    ip4FromOctets,
    ip4ToOctets,
    ip6FromWords,
    ip6ToWords,
  )
import Data.List (foldl', scanl')
import Control.Monad ( replicateM, forM, replicateM_, zipWithM_, join)
import qualified Z.Data.Parser as P
import qualified Z.Data.Vector as Z
import qualified Z.Foreign as Z
import Z.Data.ASCII
import Debug.Trace

-- Auxiliary
(.$) :: a -> (a -> c) -> c
(.$) = flip ($)


--Debug
--import Debug.Trace

-- Notice: Codes in this file might be difficult to read.
---------------------------------------------------------------------------------------
---Readers
readColumn ::
  -- | Server information is needed in case of some parameters are missing
  ServerInfo ->
  -- | number of rows
  Int ->
  -- | data type
  Bytes ->
  P.Parser [ClickhouseType]
readColumn server_info n_rows spec
  | "String" `Z.isPrefixOf` spec = replicateM n_rows $! (CKString <$> readBinaryStr)
  | "Array" `Z.isPrefixOf` spec = readArray server_info n_rows spec
  | "FixedString" `Z.isPrefixOf` spec = readFixed n_rows spec
  | "DateTime" `Z.isPrefixOf` spec = readDateTime server_info n_rows spec
  | "Date" `Z.isPrefixOf` spec = readDate n_rows
  | "Tuple" `Z.isPrefixOf` spec = readTuple server_info n_rows spec
  | "Nullable" `Z.isPrefixOf` spec = readNullable server_info n_rows spec
  | "LowCardinality" `Z.isPrefixOf` spec = readLowCardinality server_info n_rows spec
  | "Decimal" `Z.isPrefixOf` spec = readDecimal n_rows spec
  | "Enum" `Z.isPrefixOf` spec = readEnum n_rows spec
  | "Int" `Z.isPrefixOf` spec = readIntColumn n_rows spec
  | "UInt" `Z.isPrefixOf` spec = readIntColumn n_rows spec
  | "IPv4" `Z.isPrefixOf` spec = readIPv4 n_rows
  | "IPv6" `Z.isPrefixOf` spec = readIPv6 n_rows
  | "SimpleAggregateFunction" `Z.isPrefixOf` spec = readSimpleAggregateFunction server_info n_rows spec
  | "UUID" `Z.isPrefixOf` spec = readUUID n_rows
  | otherwise = error $ show ("Unknown Type: " <> spec)

writeColumn ::
  -- | context contains client information and server information
  Context ->
  -- | column name
  Bytes ->
  -- | column type (String, Int, etc)
  Bytes ->
  -- | items to be serialized.
  [ClickhouseType] ->
  -- | result wrapped in a customized Writer Monad used for concatenating string builders.
  B.Builder ()
writeColumn ctx col_name cktype items
  | "String" `Z.isPrefixOf` cktype = writeStringColumn col_name items
  | "FixedString(" `Z.isPrefixOf` cktype = writeFixedLengthString col_name cktype items
  | "Int" `Z.isPrefixOf` cktype = writeIntColumn col_name cktype items
  | "UInt" `Z.isPrefixOf` cktype = writeUIntColumn col_name cktype items
  | "Nullable(" `Z.isPrefixOf` cktype = writeNullable ctx col_name cktype items
  | "Tuple" `Z.isPrefixOf` cktype = writeTuple ctx col_name cktype items
  | "Enum" `Z.isPrefixOf` cktype = writeEnum col_name cktype items
  | "Array" `Z.isPrefixOf` cktype = writeArray ctx col_name cktype items
  | "UUID" `Z.isPrefixOf` cktype = writeUUID col_name items
  | "IPv4" `Z.isPrefixOf` cktype = writeIPv4 col_name items
  | "IPv6" `Z.isPrefixOf` cktype = writeIPv6 col_name items
  | "Date" `Z.isPrefixOf` cktype = writeDate col_name items
  | "LowCardinality" `Z.isPrefixOf` cktype = writeLowCardinality ctx col_name cktype items
  | "DateTime" `Z.isPrefixOf` cktype = writeDateTime col_name cktype items
  | "Decimal" `Z.isPrefixOf` cktype = writeDecimal col_name cktype items
  | otherwise = error $ show ("Unknown Type in the column: " <> col_name)
---------------------------------------------------------------------------------------------
readFixed :: Int -> Bytes -> P.Parser [ClickhouseType]
readFixed n_rows spec = do
  let l = Z.length spec
  let str_number = Z.take (l - 13) (Z.drop 12 spec)
  let number = readInt str_number
  replicateM n_rows (readFixedLengthString number)

readFixedLengthString :: Int -> P.Parser ClickhouseType
readFixedLengthString str_len = CKString <$> P.take str_len

writeStringColumn :: Bytes -> [ClickhouseType] -> B.Builder ()
writeStringColumn col_name =
  mapM_
    ( \case
        CKString s -> writeBinaryStr s
        CKNull -> writeVarUInt 0
        _ -> error (typeMismatchError col_name)
    )

writeFixedLengthString :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeFixedLengthString col_name spec items = do
  let l = Z.length spec
  let len= readInt $ Z.take (l - 13) (Z.drop 12 spec)
  mapM_
    ( \case
        CKString s -> writeBinaryFixedLengthStr (fromIntegral len) s
        CKNull -> () <$ replicateM (fromIntegral len) (writeVarUInt 0)
        x -> error (typeMismatchError col_name ++ " got: " ++ show x)
    )
    items

---------------------------------------------------------------------------------------------

-- | read data in format of Bytes into format of haskell type.
readIntColumn :: Int -> Bytes -> P.Parser [ClickhouseType]
readIntColumn n_rows "Int8" = replicateM n_rows (CKInt8 <$> readBinaryInt8)
readIntColumn n_rows "Int16" = replicateM n_rows (CKInt16 <$> readBinaryInt16)
readIntColumn n_rows "Int32" = replicateM n_rows (CKInt32 <$> readBinaryInt32)
readIntColumn n_rows "Int64" = replicateM n_rows (CKInt64 <$> readBinaryInt64)
readIntColumn n_rows "UInt8" = replicateM n_rows (CKUInt8 <$> readBinaryUInt8)
readIntColumn n_rows "UInt16" = replicateM n_rows (CKUInt16 <$> readBinaryUInt16)
readIntColumn n_rows "UInt32" = replicateM n_rows (CKUInt32 <$> readBinaryUInt32)
readIntColumn n_rows "UInt64" = replicateM n_rows (CKUInt64 <$> readBinaryUInt64)
readIntColumn _ x = error ("expect an integer but got: " ++ show x)

writeIntColumn :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeIntColumn col_name spec items = do
  let indicator = readInt $ Z.drop 3 spec -- indicator indicates which integer type, is it Int8 or Int64 etc.
  writeIntColumn' indicator col_name items
  where
    writeIntColumn' :: Int -> Bytes -> [ClickhouseType] -> B.Builder ()
    writeIntColumn' indicator col_name =
      case indicator of
        8 ->
          mapM_ -- mapM_ acts like for-loop in this context, since it repeats monadic actions.
            ( \case
                CKInt8 x -> writeBinaryInt8 x
                CKNull -> writeBinaryInt8 0
                _ -> error (typeMismatchError col_name)
            )
        16 ->
          mapM_
            ( \case
                CKInt16 x -> writeBinaryInt16 x
                CKNull -> writeBinaryInt16 0
                _ -> error (typeMismatchError col_name)
            )
        32 ->
          mapM_
            ( \case
                CKInt32 x -> writeBinaryInt32 x
                CKNull -> writeBinaryInt32 0
                _ -> error (typeMismatchError col_name)
            )
        64 ->
          mapM_
            ( \case
                CKInt64 x -> writeBinaryInt64 x
                CKNull -> writeBinaryInt64 0
                _ -> error (typeMismatchError col_name)
            )
        x -> error $ "Unknow Int type: " ++ show x

writeUIntColumn :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeUIntColumn col_name spec items = do
  let indicator = readInt $ Z.drop 4 spec
  writeUIntColumn' indicator col_name items
  where
    writeUIntColumn' :: Int -> Bytes -> [ClickhouseType] -> B.Builder ()
    writeUIntColumn' indicator col_name =
      case indicator of
        8 ->
          mapM_
            ( \case
                CKUInt8 x -> writeBinaryUInt8 x
                CKNull -> writeBinaryUInt8 0
                _ -> error (typeMismatchError col_name)
            )
        16 ->
          mapM_
            ( \case
                CKUInt16 x -> writeBinaryInt16 $ fromIntegral x
                CKNull -> writeBinaryInt16 0
                _ -> error (typeMismatchError col_name)
            )
        32 ->
          mapM_
            ( \case
                CKUInt32 x -> writeBinaryInt32 $ fromIntegral x
                CKNull -> writeBinaryInt32 0
                _ -> error (typeMismatchError col_name)
            )
        64 ->
          mapM_
            ( \case
                CKUInt64 x -> writeBinaryInt64 $ fromIntegral x
                CKNull -> writeBinaryInt64 0
                _ -> error (typeMismatchError col_name)
            )
        x -> error $ "Unknow indicator" ++ show x

---------------------------------------------------------------------------------------------

{-
  There are two types of Datetime
  DateTime(TZ) or DateTime64(precision,TZ)
  server information is required if TZ parameter is missing.
-}
readDateTime :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readDateTime server_info n_rows spec = do
  let (scale, spc) = readTimeSpec spec
  case spc of
    Nothing -> readDateTimeWithSpec server_info n_rows scale ""
    Just tz_name -> readDateTimeWithSpec server_info n_rows scale tz_name

readTimeSpec :: Bytes -> (Maybe Int, Maybe Bytes)
readTimeSpec spec'
  | "DateTime64" `Z.isPrefixOf` spec' = do
    let l = Z.length spec'
    let inner_specs = Z.take (l - 12) (Z.drop 11 spec')
    let split = getSpecs inner_specs
    case split of
      [] -> (Nothing, Nothing)
      [x] -> (Just $ readInt x, Nothing)
      [x, y] -> (Just $ readInt x, Just y)
  | otherwise = do
    let l = Z.length spec'
    let inner_specs = Z.take (l - 12) (Z.drop 10 spec')
    (Nothing, Just inner_specs)

readDateTimeWithSpec :: ServerInfo -> Int -> Maybe Int -> Bytes -> P.Parser [ClickhouseType]
readDateTimeWithSpec = undefined


writeDateTime :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeDateTime = undefined

------------------------------------------------------------------------------------------------
readLowCardinality :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readLowCardinality _ 0 _ = return []
readLowCardinality server_info n spec = do
  readBinaryUInt64 --state prefix
  let l = Z.length spec
  let inner = Z.take (l - 16) (Z.drop 15 spec)
  serialization_type <- readBinaryUInt64
  -- Lowest bytes contains info about key type.
  let key_type = serialization_type .&. 0xf
  index_size <- readBinaryUInt64
  -- Strip the 'Nullable' tag to avoid null map reading.
  index' <- readColumn server_info (fromIntegral index_size) (stripNullable inner)
  let indices :: Vector ClickhouseType = V.pack index'
  readBinaryUInt64 -- #keys
  keys <- case key_type of
    0 -> map fromIntegral <$> replicateM n readBinaryUInt8
    1 -> map fromIntegral <$> replicateM n readBinaryUInt16
    2 -> map fromIntegral <$> replicateM n readBinaryUInt32
    3 -> map fromIntegral <$> replicateM n readBinaryUInt64
  if "Nullable" `Z.isPrefixOf` inner
    then do
      let nullable = keys .$ fmap \k -> if k == 0 then CKNull else Z.index indices k
      return nullable
    else return $ fmap (Z.index indices) keys
  where
    stripNullable :: Bytes -> Bytes
    stripNullable spec
      | "Nullable" `Z.isPrefixOf` spec = Z.take (l - 10) (Z.drop 9 spec)
      | otherwise = spec
    l = Z.length spec

writeLowCardinality :: Context -> Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeLowCardinality ctx col_name spec items = do
  let inner = Z.take (Z.length spec - 16) (Z.drop 15 spec)
  (keys, index) <-
    if "Nullable" `Z.isPrefixOf` inner
      then do
        --let null_inner_spec = Z.take (Z.length inner - 10) (Z.drop 9 spec)
        let hashedItem = hashItems True items
        let key_by_index_element = foldl' insertKeys Map.empty hashedItem
        let keys = hashedItem .$ map \k -> key_by_index_element Map.! k + 1
        -- First element is NULL if column is nullable
        let index :: Vector Int = V.pack $ 0 : Map.keys key_by_index_element
        return (keys, index)
      else do
        let hashedItem = hashItems False items
        let key_by_index_element = foldl' insertKeys Map.empty hashedItem
        let keys = map (key_by_index_element Map.!) hashedItem
        let index = V.pack $ Map.keys key_by_index_element
        return (keys, index)
  if V.length index == 0
    then return ()
    else do
      let int_type = floor $ logBase 2 (fromIntegral $ V.length index) / 8 :: Int64
      let has_additional_keys_bit = 1 `shift` 9
      let need_update_dictionary = 1 `shift` 10
      let serialization_type =
            has_additional_keys_bit
              .|. need_update_dictionary
              .|. int_type
      let nullsInner =
            if "Nullable" `Z.isPrefixOf` inner
              then Z.take (Z.length inner - 10) (Z.drop 9 spec)
              else inner
      writeBinaryUInt64 1 --state prefix
      writeBinaryInt64 serialization_type
      writeBinaryInt64 $ fromIntegral $ V.length index
      writeColumn ctx col_name nullsInner items
      writeBinaryInt64 $ fromIntegral $ length items
      case int_type of
        0 -> mapM_ (writeBinaryUInt8 . fromIntegral) keys
        1 -> mapM_ (writeBinaryUInt16 . fromIntegral) keys
        2 -> mapM_ (writeBinaryUInt32 . fromIntegral) keys
        3 -> mapM_ (writeBinaryUInt64 . fromIntegral) keys
  where
    insertKeys :: (Hashable a, Eq a) => Map.HashMap a Int -> a -> Map.HashMap a Int
    insertKeys m a = if Map.member a m then m else Map.insert a (Map.size m) m

    hashItems :: Bool -> [ClickhouseType] -> [Int]
    hashItems isNullable items =
      map
        ( \case
            CKInt16 x -> hash x
            CKInt8 x -> hash x
            CKInt32 x -> hash x
            CKInt64 x -> hash x
            CKUInt8 x -> hash x
            CKUInt16 x -> hash x
            CKUInt32 x -> hash x
            CKUInt64 x -> hash x
            CKString str -> hash str
            CKNull ->
              if isNullable
                then hash (0 :: Int)
                else error $ typeMismatchError col_name
            _ -> error $ typeMismatchError col_name
        )
        items

---------------------------------------------------------------------------------------------------------------------------------
{-
          Informal description (in terms of regular expression form) for this config:
          (\Null | \SOH)^{n_rows}
          \Null means null and \SOH which equals 1 means not null. The `|` in the middle means or.
-}
readNullable :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readNullable server_info n_rows spec = do
  let l = Z.length spec
  let cktype = Z.take (l - 10) (Z.drop 9 spec) -- Read Clickhouse type inside the bracket after the 'Nullable' spec.
  config <- readNullableConfig n_rows
  items' <- readColumn server_info n_rows cktype
  let items :: Vector ClickhouseType = V.pack items'
  let result = [if Z.index config i == 1 then CKNull else Z.index items i | i <- [0..n_rows-1]]
  return result
  where
    readNullableConfig :: Int -> P.Parser (Vector Word8)
    readNullableConfig n_rows = do
      config <- P.take n_rows
      (return . V.pack . Z.unpack) config

writeNullable :: Context -> Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeNullable ctx col_name spec items = do
  let l = Z.length spec
  let inner = Z.take (l - 10) (Z.drop 9 spec)
  writeNullsMap items
  writeColumn ctx col_name inner items
  where
    writeNullsMap :: [ClickhouseType] -> B.Builder ()
    writeNullsMap =
      mapM_
        ( \case
            CKNull -> writeBinaryInt8 1
            _ -> writeBinaryInt8 0
        )

---------------------------------------------------------------------------------------------------------------------------------
{-
  Format:
  "
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
  "
      Quoted from https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/columns/arraycolumn.py

Here, classical BFS algorithm is not implemented as it is not natural to do so with purely
functional syntax; instead, we apply a new method that can be implemented easily in this case as described in the following:
In each iteration, first off, we compute the array of integer in which elements are integers 
representing the size of subarrays (call them spec arrays which will update at each iteration by popping out from a queue)
, where the we need the functions `readArraySpec`, `cut`, and `intervalize`.
Next, we manipulate the array which is in its way becoming the final result.
We cut this array into pieces according to the lastest updated spec array and in each piece, we group elements 
together to form a new array in place.
Then, we pop out the spec array on top.
Repeat this process until all spec arrays are gone, and finally return the result.

For example:
The input array is [3, 4, 5, 6], with the spec arrays:
[2] [2,2]
The array on the right hand side of `|` is array of atomic elements.
The algorithm can be visualized as followed:
-> [2] [2,2] | [3,4,5,6]
->       [2] | [[3,4],[5,6]]
->           | [[3,4],[5,6]]

For another example:
-> [3] [2,2,1] [2,1,1,3,2] | ["Alex","Bob","John","Jane","Steven","Mike","Sarah","Hello","world"]
->             [3] [2,2,1] | [["Alex","Bob"],["John"],["Jane"],["Steven","Mike","Sarah"],["Hello","world"]]
->                     [3] | [[["Alex","Bob"],["John"]],[["Jane"],["Steven","Mike","Sarah"]],[["Hello","world"]]]
->                         | [[["Alex","Bob"],["John"]],[["Jane"],["Steven","Mike","Sarah"]],[["Hello","world"]]]

-}
readArray :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readArray server_info n_rows spec = do
  (lastSpec, specs@(x : xs)) <- genSpecs spec [[fromIntegral n_rows]]
  --  lastSpec which is not `Array`
  --  x:xs is the
  let numElem = fromIntegral $ sum x -- number of elements in the nested array.
  elems <- readColumn server_info numElem lastSpec
  let total = foldl' combine (Z.pack elems) (Z.pack <$> specs)
  let CKArray arr = Z.index total 0
  return $ Z.unpack arr
  where
    combine :: Vector ClickhouseType -> Vector Word64 -> Vector ClickhouseType
    combine elems config =
      let intervals = intervalize (fromIntegral <$> config)
          cut :: (Int, Int)->ClickhouseType
          cut (!a, !b) = CKArray $ Z.take b (Z.drop a elems)
          embed = intervals .$ fmap \(l, r) -> cut (l, r - l + 1)
      in embed

    intervalize :: Vector Int -> Vector (Int, Int)
    intervalize vec = Z.drop 1 (vec .$ Z.scanl' (\(_, b) v -> (b + 1, v + b)) (-1, -1)) -- drop the first tuple (-1,-1)

readArraySpec :: [Word64] -> P.Parser [Word64]
readArraySpec sizeArr = do
  let arrSum = (fromIntegral . sum) sizeArr
  offsets <- replicateM arrSum readBinaryUInt64
  let offsets' = 0 : take (arrSum - 1) offsets
  let sizes = zipWith (-) offsets offsets'
  return sizes

genSpecs :: Bytes -> [[Word64]] -> P.Parser (Bytes, [[Word64]])
genSpecs spec rest@(x : _) = do
  let l = Z.length spec
  let cktype = Z.take (l - 7) (Z.drop 6 spec)
  if "Array" `Z.isPrefixOf` spec
    then do
      next <- readArraySpec x
      genSpecs cktype (next : rest)
    else return (spec, rest)

writeArray :: Context -> Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeArray ctx col_name spec items = do
  let lens =
        scanl'
          ( \total ->
              ( \case
                  (CKArray xs) -> total + length xs
                  x ->
                    error $
                      "unexpected type in the column: "
                        ++ show col_name
                        ++ " with data"
                        ++ show x
              )
          )
          0
          items
  mapM_ (writeBinaryInt64 . fromIntegral) (tail lens)
  let innerSpec = Z.take (Z.length spec - 7) (Z.drop 6 spec)
  let innerVector = map (\case CKArray xs -> Z.unpack xs) items
  let flattenVector = join innerVector
  writeColumn ctx col_name innerSpec flattenVector

--------------------------------------------------------------------------------------
readTuple :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readTuple server_info n_rows spec = do
  let l = Z.length spec
  let innerSpecString = Z.take (l - 7) (Z.drop 6 spec) -- Tuple(...) the dots represent innerSpecString
  let arr = getSpecs innerSpecString
  datas <- mapM (readColumn server_info n_rows) arr
  let transposed = List.transpose datas
  return $ CKTuple . Z.pack <$> transposed

writeTuple :: Context -> Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeTuple ctx col_name spec items = do
  let inner = Z.take (Z.length spec - 7) (Z.drop 6 spec)
  let spec_arr = getSpecs inner
  let transposed =
        List.transpose
          ( map
              ( \case
                  CKTuple tupleVec -> Z.unpack tupleVec
                  other ->
                    error
                      ( "expected type: " ++ show other
                          ++ " in the column: "
                          ++ (w2c <$> Z.unpack col_name)
                      )
              )
              items
          )
  if length spec_arr /= length transposed
    then
      error $
        "length of the given array does not match, column name = "
          ++ show col_name
    else
      zipWithM_ (writeColumn ctx col_name) spec_arr transposed

--------------------------------------------------------------------------------------
readEnum :: Int -> Bytes -> P.Parser [ClickhouseType]
readEnum n_rows spec = do
  let l = Z.length spec
      innerSpec =
        if "Enum8" `Z.isPrefixOf` spec
          then Z.take (l - 7) (Z.drop 6 spec)
          else Z.take (l - 8) (Z.drop 7 spec) -- otherwise it is `Enum16`
      pres_pecs = getSpecs innerSpec
      specs =
        (\(name, n) -> (n, name))
          <$> ((\[x, y] -> (x, readInt y)) . Z.splitWith (== 61) <$> pres_pecs) --61 means '='
      specsMap = Map.fromList specs
  if "Enum8" `Z.isPrefixOf` spec
    then do
      values <- replicateM n_rows readBinaryInt8
      return $ CKString . (specsMap Map.!) . fromIntegral <$> values
    else do
      values <- replicateM n_rows readBinaryInt16
      return $ CKString . (specsMap Map.!) . fromIntegral <$> values

writeEnum :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeEnum col_name spec items = do
  let l = Z.length spec
      innerSpec =
        if "Enum8" `Z.isPrefixOf` spec
          then Z.take (l - 7) (Z.drop 6 spec)
          else Z.take (l - 8) (Z.drop 7 spec)
      pres_pecs = getSpecs innerSpec
      specs =
        (\(name, n) -> (name, n))
          <$> ((\[x, y] -> (x, readInt y)) . Z.splitWith (== 61) . Z.filter (/= 39) <$> pres_pecs) --61 is '='
      specsMap = Map.fromList specs
  mapM_
    ( \case
        CKString str ->
          ( if "Enum8" `Z.isPrefixOf` spec
              then writeBinaryInt8 $ fromIntegral $ specsMap Map.! str
              else writeBinaryInt16 $ fromIntegral $ specsMap Map.! str
          )
        CKNull ->
          if "Enum8" `Z.isPrefixOf` spec
            then writeBinaryInt8 0
            else writeBinaryInt16 0
        _ -> error $ typeMismatchError col_name
    )
    items

----------------------------------------------------------------------
readDate :: Int -> P.Parser [ClickhouseType]
readDate n_rows = do
  let epoch_start = fromGregorian 1970 1 1
  days <- replicateM n_rows readBinaryUInt16
  let dates = map (\x -> addDays (fromIntegral x) epoch_start) days
      toTriple = map toGregorian dates
      toCK = map (\(y, m, d) -> CKDate y m d) toTriple
  return toCK

writeDate :: Bytes -> [ClickhouseType] -> B.Builder ()
writeDate col_name items = do
  let epoch_start = fromGregorian 1970 1 1
  let serialize =
        map
          ( \case
              CKDate y m d -> diffDays (fromGregorian y m d) epoch_start
              _ ->
                error $
                  "unexpected type in the column: " ++ show col_name
                    ++ " whose type should be Date"
          )
          items
  mapM_ (writeBinaryInt16 . fromIntegral) serialize

--------------------------------------------------------------------------------------
readDecimal :: Int -> Bytes -> P.Parser [ClickhouseType]
readDecimal n_rows spec = do
  let l = Z.length spec
  let inner_spec = getSpecs $ Z.take (l - 9) (Z.drop 8 spec)
  let (specific, scale) = case inner_spec of
        [] -> error "No spec"
        [scale'] ->
          if "Decimal32" `Z.isPrefixOf` spec
            then (readDecimal32, readInt scale')
            else
              if "Decimal64" `Z.isPrefixOf` spec
                then (readDecimal64, readInt scale')
                else (readDecimal128, readInt scale')
        [precision', scale'] -> do
          let precision = readInt precision'
          if precision <= 9 || "Decimal32" `Z.isPrefixOf` spec
            then (readDecimal32, readInt scale')
            else
              if precision <= 18 || "Decimal64" `Z.isPrefixOf` spec
                then (readDecimal64, readInt scale')
                else (readDecimal128, readInt scale')
  raw <- specific n_rows
  let final = fmap (trans scale) raw
  return final
  where
    readDecimal32 :: Int -> P.Parser [ClickhouseType]
    readDecimal32 n_rows = readIntColumn n_rows "Int32"

    readDecimal64 :: Int -> P.Parser [ClickhouseType]
    readDecimal64 n_rows = readIntColumn n_rows "Int64"

    readDecimal128 :: Int -> P.Parser [ClickhouseType]
    readDecimal128 n_rows =
      replicateM n_rows $ do
        lo <- readBinaryUInt64
        CKUInt128 lo <$> readBinaryUInt64

    trans :: Int -> ClickhouseType -> ClickhouseType
    trans scale (CKInt32 x) = CKDecimal32 (fromIntegral x / fromIntegral (10 ^ scale))
    trans scale (CKInt64 x) = CKDecimal64 (fromIntegral x / fromIntegral (10 ^ scale))
    trans scale (CKUInt128 lo hi) = CKDecimal128 (word128_division hi lo scale)

writeDecimal :: Bytes -> Bytes -> [ClickhouseType] -> B.Builder ()
writeDecimal col_name spec items = do
  let l = Z.length spec
  let inner_specs = getSpecs $ Z.take (l - 9) (Z.drop 8 spec)
  let (specific, pre_scale) = case inner_specs of
        [] -> error "No spec"
        [scale'] ->
          if "Decimal32" `Z.isPrefixOf` spec
            then (writeDecimal32, readInt scale')
            else
              if "Decimal64" `Z.isPrefixOf` spec
                then (writeDecimal64, readInt scale')
                else (writeDecimal128, readInt scale')
        [precision', scale'] -> do
          let precision = readInt precision'
          if precision <= 9 || "Decimal32" `Z.isPrefixOf` spec
            then (writeDecimal32, readInt scale')
            else
              if precision <= 18 || "Decimal64" `Z.isPrefixOf` spec
                then (writeDecimal64, readInt scale')
                else (writeDecimal128, readInt scale')
  let scale = 10 ^ pre_scale
  specific scale items
  where
    writeDecimal32 :: Int -> [ClickhouseType] -> B.Builder ()
    writeDecimal32 scale vec =
      mapM_
        ( \case
            CKDecimal32 f32 -> writeBinaryInt32 $ fromIntegral $ floor (f32 * fromIntegral scale)
            _ -> error $ typeMismatchError col_name
        )
        vec
    writeDecimal64 :: Int -> [ClickhouseType] -> B.Builder ()
    writeDecimal64 scale vec =
      mapM_
        ( \case
            CKDecimal64 f64 -> writeBinaryInt32 $ fromIntegral $ floor (f64 * fromIntegral scale)
            _ -> error $ typeMismatchError col_name
        )
        vec
    writeDecimal128 :: Int -> [ClickhouseType] -> B.Builder ()
    writeDecimal128 scale items =
      mapM_
      ( \case
          CKDecimal128 x -> do
            if x >= 0
              then do
                writeBinaryUInt64 $ low_bits_128 x scale
                writeBinaryUInt64 $ hi_bits_128 x scale
              else do
                writeBinaryUInt64 $ low_bits_negative_128 (- x) scale
                writeBinaryUInt64 $ hi_bits_negative_128 (- x) scale
      )
      items

foreign import ccall unsafe "bigint.h word128_division" word128_division :: Word64 -> Word64 -> Int -> Double

foreign import ccall unsafe "bigint.h low_bits_128" low_bits_128 :: Double -> Int -> Word64

foreign import ccall unsafe "bigint.h hi_bits_128" hi_bits_128 :: Double -> Int -> Word64

foreign import ccall unsafe "bigint.h low_bits_negative_128" low_bits_negative_128 :: Double -> Int -> Word64

foreign import ccall unsafe "bigint.h hi_bits_negative_128" hi_bits_negative_128 :: Double -> Int -> Word64
----------------------------------------------------------------------------------------------
readIPv4 :: Int -> P.Parser [ClickhouseType]
readIPv4 n_rows = replicateM n_rows (CKIPv4 . ip4ToOctets . IP4 <$> readBinaryUInt32)

readIPv6 :: Int -> P.Parser [ClickhouseType]
readIPv6 n_rows = replicateM n_rows (CKIPv6 . ip6ToWords . IP6 <$> readBinaryUInt128)

writeIPv4 :: Bytes -> [ClickhouseType] -> B.Builder ()
writeIPv4 col_name =
  mapM_
    ( \case
        CKIPv4 (w1, w2, w3, w4) ->
          writeBinaryUInt32 $
            unIP4 $
              ip4FromOctets w1 w2 w3 w4
        CKNull -> writeBinaryInt32 0
        _ -> error $ typeMismatchError col_name
    )

writeIPv6 :: Bytes -> [ClickhouseType] -> B.Builder ()
writeIPv6 col_name =
  mapM_
    ( \case
        CKIPv6 (w1, w2, w3, w4, w5, w6, w7, w8) ->
          writeBinaryUInt128 $
            unIP6 $
              ip6FromWords w1 w2 w3 w4 w5 w6 w7 w8
        CKNull -> writeBinaryUInt64 0
        _ -> error $ typeMismatchError col_name
    )

----------------------------------------------------------------------------------------------
readSimpleAggregateFunction :: ServerInfo -> Int -> Bytes -> P.Parser [ClickhouseType]
readSimpleAggregateFunction server_info n_rows spec = do
  let l = Z.length spec
  let [func, cktype] = getSpecs $ Z.take (l - 25) (Z.drop 24 spec)
  readColumn server_info n_rows cktype
----------------------------------------------------------------------------------------------
readUUID :: Int -> P.Parser [ClickhouseType]
readUUID n_rows =
  replicateM n_rows $ do
  w2 <- readBinaryUInt32
  w1 <- readBinaryUInt32
  w3 <- readBinaryUInt32
  w4 <- readBinaryUInt32
  let res' = UUID.toString $ UUID.fromWords w1 w2 w3 w4
  let res = Z.pack [c2w x | x <- res']
  return $ CKString res


writeUUID :: Bytes -> [ClickhouseType] -> B.Builder ()
writeUUID col_name =
  mapM_
    ( \case
        CKString uuid_str -> do
          case UUID.fromString $ w2c <$> Z.unpack uuid_str of
            Nothing ->
              error $
                "UUID parsing error in the column"
                  ++ show col_name
                  ++ " wrong data: "
                  ++ show uuid_str
            Just uuid -> do
              let (w2, w1, w3, w4) = UUID.toWords uuid
              writeBinaryUInt32 w1
              writeBinaryUInt32 w2
              writeBinaryUInt32 w3
              writeBinaryUInt32 w4
        CKNull -> do
          writeBinaryUInt64 0
          writeBinaryUInt64 0
        other -> error $ "Column " 
          ++ show col_name 
          ++ " does not match the required type: UUID"
    )

----------------------------------------------------------------------------------------------
---Helpers
readInt :: Bytes->Int
readInt b =
  let (_,Right res) = P.parse P.int b
  in res

-- | Get rid of commas and spaces
getSpecs :: Bytes -> [Bytes]
getSpecs str = Z.splitWith (== 44) (Z.filter (/= 32) str)

getIthRow :: Int -> Vector (Vector ClickhouseType) -> Maybe (Vector ClickhouseType)
getIthRow i items
  | i < 0 = Nothing
  | V.length items == 0 = Nothing
  | i >= V.length (V.index items 0) = Nothing
  | otherwise = Just $ V.map (`Z.index` i) items

typeMismatchError :: Bytes -> String
typeMismatchError col_name = "Type mismatch in the column " ++ show col_name
