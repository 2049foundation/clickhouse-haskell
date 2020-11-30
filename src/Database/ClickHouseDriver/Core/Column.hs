-------------------------------------------------------------------------
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module contains the implementations of
--   serialization and deserialization of Clickhouse data types.
module Database.ClickHouseDriver.Core.Column
  ( -- * Serialize and deserialize
    ClickhouseType (..),
    readColumn,
    writeColumn,

    -- * Operations on ClickhouseType
    transpose,
    Database.ClickHouseDriver.Core.Column.putStrLn,
  )
where

import Database.ClickHouseDriver.Core.Types (ClickhouseType (..), Context (..), ServerInfo (..))
import Database.ClickHouseDriver.IO.BufferedReader
  ( Reader,
    readBinaryInt16,
    readBinaryInt32,
    readBinaryInt64,
    readBinaryInt8,
    readBinaryStr,
    readBinaryStrWithLength,
    readBinaryUInt128,
    readBinaryUInt16,
    readBinaryUInt32,
    readBinaryUInt64,
    readBinaryUInt8,
  )
import Database.ClickHouseDriver.IO.BufferedWriter
  ( Writer,
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
import Control.Monad.State.Lazy (MonadIO (..))
import Data.Binary (Word64, Word8)
import Data.Bits (shift, (.&.), (.|.))
import Data.ByteString (ByteString, isPrefixOf)
import qualified Data.ByteString as BS
  ( drop,
    filter,
    intercalate,
    length,
    splitWith,
    take,
    unpack,
  )
import Data.ByteString.Builder (Builder)
import Data.ByteString.Char8 (readInt)
import qualified Data.ByteString.Char8 as C8
import Data.ByteString.Unsafe
  ( unsafePackCString,
    unsafeUseAsCStringLen,
  )
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
import Data.Vector (Vector, (!))
import qualified Data.Vector as V
  ( cons,
    drop,
    foldl',
    fromList,
    generate,
    length,
    map,
    mapM,
    mapM_,
    replicateM,
    scanl',
    sum,
    take,
    toList,
    zipWith,
    zipWithM_,
  )
import Foreign.C (CString)
import Network.IP.Addr
  ( IP4 (..),
    IP6 (..),
    ip4FromOctets,
    ip4ToOctets,
    ip6FromWords,
    ip6ToWords,
  )
#define EQUAL 61
#define COMMA 44
#define SPACE 32
#define QUOTE 39
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
  ByteString ->
  Reader (Vector ClickhouseType)
readColumn server_info n_rows spec
  | "String" `isPrefixOf` spec = V.replicateM n_rows (CKString <$> readBinaryStr)
  | "Array" `isPrefixOf` spec = readArray server_info n_rows spec
  | "FixedString" `isPrefixOf` spec = readFixed n_rows spec
  | "DateTime" `isPrefixOf` spec = readDateTime server_info n_rows spec
  | "Date" `isPrefixOf` spec = readDate n_rows
  | "Tuple" `isPrefixOf` spec = readTuple server_info n_rows spec
  | "Nullable" `isPrefixOf` spec = readNullable server_info n_rows spec
  | "LowCardinality" `isPrefixOf` spec = readLowCardinality server_info n_rows spec
  | "Decimal" `isPrefixOf` spec = readDecimal n_rows spec
  | "Enum" `isPrefixOf` spec = readEnum n_rows spec
  | "Int" `isPrefixOf` spec = readIntColumn n_rows spec
  | "UInt" `isPrefixOf` spec = readIntColumn n_rows spec
  | "IPv4" `isPrefixOf` spec = readIPv4 n_rows
  | "IPv6" `isPrefixOf` spec = readIPv6 n_rows
  | "SimpleAggregateFunction" `isPrefixOf` spec = readSimpleAggregateFunction server_info n_rows spec
  | "UUID" `isPrefixOf` spec = readUUID n_rows
  | otherwise = error ("Unknown Type: " Prelude.++ C8.unpack spec)

writeColumn ::
  -- | context contains client information and server information
  Context ->
  -- | column name
  ByteString ->
  -- | column type (String, Int, etc)
  ByteString ->
  -- | items to be serialized.
  Vector ClickhouseType ->
  -- | result wrapped in a customized Writer Monad used for concatenating string builders.
  Writer Builder
writeColumn ctx col_name cktype items
  | "String" `isPrefixOf` cktype = writeStringColumn col_name items
  | "FixedString(" `isPrefixOf` cktype = writeFixedLengthString col_name cktype items
  | "Int" `isPrefixOf` cktype = writeIntColumn col_name cktype items
  | "UInt" `isPrefixOf` cktype = writeUIntColumn col_name cktype items
  | "Nullable(" `isPrefixOf` cktype = writeNullable ctx col_name cktype items
  | "Tuple" `isPrefixOf` cktype = writeTuple ctx col_name cktype items
  | "Enum" `isPrefixOf` cktype = writeEnum col_name cktype items
  | "Array" `isPrefixOf` cktype = writeArray ctx col_name cktype items
  | "UUID" `isPrefixOf` cktype = writeUUID col_name items
  | "IPv4" `isPrefixOf` cktype = writeIPv4 col_name items
  | "IPv6" `isPrefixOf` cktype = writeIPv6 col_name items
  | "Date" `isPrefixOf` cktype = writeDate col_name items
  | "LowCardinality" `isPrefixOf` cktype = writeLowCardinality ctx col_name cktype items
  | "DateTime" `isPrefixOf` cktype = writeDateTime col_name cktype items
  | "Decimal" `isPrefixOf` cktype = writeDecimal col_name cktype items
  | otherwise = error ("Unknown Type in the column: " Prelude.++ C8.unpack col_name)
---------------------------------------------------------------------------------------------
readFixed :: Int -> ByteString -> Reader (Vector ClickhouseType)
readFixed n_rows spec = do
  let l = BS.length spec
  let str_number = BS.take (l - 13) (BS.drop 12 spec)
  let number = case readInt str_number of
        Nothing -> 0 -- This can't happen
        Just (x, _) -> x
  V.replicateM n_rows (readFixedLengthString number)

readFixedLengthString :: Int -> Reader ClickhouseType
readFixedLengthString str_len = CKString <$> readBinaryStrWithLength str_len

writeStringColumn :: ByteString -> Vector ClickhouseType -> Writer Builder
writeStringColumn col_name =
  V.mapM_
    ( \case
        CKString s -> writeBinaryStr s
        CKNull -> writeVarUInt 0
        _ -> error (typeMismatchError col_name)
    )

writeFixedLengthString :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeFixedLengthString col_name spec items = do
  let l = BS.length spec
  let Just (len, _) = readInt $ BS.take (l - 13) (BS.drop 12 spec)
  V.mapM_
    ( \case
        CKString s -> writeBinaryFixedLengthStr (fromIntegral len) s
        CKNull -> const () <$> V.replicateM (fromIntegral len) (writeVarUInt 0)
        x -> error (typeMismatchError col_name ++ " got: " ++ show x)
    )
    items

---------------------------------------------------------------------------------------------

-- | read data in format of bytestring into format of haskell type.
readIntColumn :: Int -> ByteString -> Reader (Vector ClickhouseType)
readIntColumn n_rows "Int8" = V.replicateM n_rows (CKInt8 <$> readBinaryInt8)
readIntColumn n_rows "Int16" = V.replicateM n_rows (CKInt16 <$> readBinaryInt16)
readIntColumn n_rows "Int32" = V.replicateM n_rows (CKInt32 <$> readBinaryInt32)
readIntColumn n_rows "Int64" = V.replicateM n_rows (CKInt64 <$> readBinaryInt64)
readIntColumn n_rows "UInt8" = V.replicateM n_rows (CKUInt8 <$> readBinaryUInt8)
readIntColumn n_rows "UInt16" = V.replicateM n_rows (CKUInt16 <$> readBinaryUInt16)
readIntColumn n_rows "UInt32" = V.replicateM n_rows (CKUInt32 <$> readBinaryUInt32)
readIntColumn n_rows "UInt64" = V.replicateM n_rows (CKUInt64 <$> readBinaryUInt64)
readIntColumn _ x = error ("expect an integer but got: " ++ show x)

writeIntColumn :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeIntColumn col_name spec items = do
  let Just (indicator, _) = readInt $ BS.drop 3 spec -- indicator indicates which integer type, is it Int8 or Int64 etc.
  writeIntColumn' indicator col_name items
  where
    writeIntColumn' :: Int -> ByteString -> Vector ClickhouseType -> Writer Builder
    writeIntColumn' indicator col_name =
      case indicator of
        8 ->
          V.mapM_ -- mapM_ acts like for-loop in this context, since it repeats monadic actions.
            ( \case
                CKInt8 x -> writeBinaryInt8 x
                CKNull -> writeBinaryInt8 0
                _ -> error (typeMismatchError col_name)
            )
        16 ->
          V.mapM_
            ( \case
                CKInt16 x -> writeBinaryInt16 x
                CKNull -> writeBinaryInt16 0
                _ -> error (typeMismatchError col_name)
            )
        32 ->
          V.mapM_
            ( \case
                CKInt32 x -> writeBinaryInt32 x
                CKNull -> writeBinaryInt32 0
                _ -> error (typeMismatchError col_name)
            )
        64 ->
          V.mapM_
            ( \case
                CKInt64 x -> writeBinaryInt64 x
                CKNull -> writeBinaryInt64 0
                _ -> error (typeMismatchError col_name)
            )

writeUIntColumn :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeUIntColumn col_name spec items = do
  let Just (indicator, _) = readInt $ BS.drop 4 spec
  writeUIntColumn' indicator col_name items
  where
    writeUIntColumn' :: Int -> ByteString -> Vector ClickhouseType -> Writer Builder
    writeUIntColumn' indicator col_name =
      case indicator of
        8 ->
          V.mapM_
            ( \case
                CKUInt8 x -> writeBinaryUInt8 x
                CKNull -> writeBinaryUInt8 0
                _ -> error (typeMismatchError col_name)
            )
        16 ->
          V.mapM_
            ( \case
                CKUInt16 x -> writeBinaryInt16 $ fromIntegral x
                CKNull -> writeBinaryInt16 0
                _ -> error (typeMismatchError col_name)
            )
        32 ->
          V.mapM_
            ( \case
                CKUInt32 x -> writeBinaryInt32 $ fromIntegral x
                CKNull -> writeBinaryInt32 0
                _ -> error (typeMismatchError col_name)
            )
        64 ->
          V.mapM_
            ( \case
                CKUInt64 x -> writeBinaryInt64 $ fromIntegral x
                CKNull -> writeBinaryInt64 0
                _ -> error (typeMismatchError col_name)
            )

---------------------------------------------------------------------------------------------

{-
  There are two types of Datetime
  DateTime(TZ) or DateTime64(precision,TZ)
  server information is required if TZ parameter is missing.
-}
readDateTime :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readDateTime server_info n_rows spec = do
  let (scale, spc) = readTimeSpec spec
  case spc of
    Nothing -> readDateTimeWithSpec server_info n_rows scale ""
    Just tz_name -> readDateTimeWithSpec server_info n_rows scale tz_name
    
readTimeSpec :: ByteString -> (Maybe Int, Maybe ByteString)
readTimeSpec spec'
  | "DateTime64" `isPrefixOf` spec' = do
    let l = BS.length spec'
    let inner_specs = BS.take (l - 12) (BS.drop 11 spec')
    let split = getSpecs inner_specs
    case split of
      [] -> (Nothing, Nothing)
      [x] -> (Just $ fst $ fromJust $ readInt x, Nothing)
      [x, y] -> (Just $ fst $ fromJust $ readInt x, Just y)
  | otherwise = do
    let l = BS.length spec'
    let inner_specs = BS.take (l - 12) (BS.drop 10 spec')
    (Nothing, Just inner_specs)
    
readDateTimeWithSpec :: ServerInfo -> Int -> Maybe Int -> ByteString -> Reader (Vector ClickhouseType)
readDateTimeWithSpec ServerInfo {timezone = maybe_zone} n_rows Nothing tz_name = do
  data32 <- readIntColumn n_rows "Int32"
  let tz_to_send =
        if tz_name /= ""
          then "TZ=" <> tz_name
          else fromMaybe "" maybe_zone
  let toDateTimeStringM =
        V.mapM
          ( \(CKInt32 x) -> do
              c_str <-
                unsafeUseAsCStringLen
                  tz_to_send
                  (uncurry (c_convert_time (fromIntegral x)))
              unsafePackCString c_str
          )
          data32
  toDateTimeString <- liftIO $ toDateTimeStringM
  return $ V.map CKString toDateTimeString

readDateTimeWithSpec ServerInfo {timezone = maybe_zone} n_rows (Just scl) tz_name = do
  data64 <- readIntColumn n_rows "Int64"
  let scale = 10 ^ fromIntegral scl
  let tz_to_send =
        if tz_name /= ""
          then "TZ=" <> tz_name
          else fromMaybe "" maybe_zone
  let toDateTimeStringM =
        V.mapM
          ( \(CKInt64 x) -> do
              c_str <-
                unsafeUseAsCStringLen
                  tz_to_send
                  (uncurry (c_convert_time64 (fromIntegral x / scale)))
              unsafePackCString c_str
          )
          data64
  toDateTimeString <- liftIO $ toDateTimeStringM
  return $ V.map CKString toDateTimeString

writeDateTime :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeDateTime col_name spec items = do
  let (scale', spc) = readTimeSpec spec
  case scale' of
    Nothing -> do
      case spc of
        Nothing -> do
          TimeZone {timeZoneName = tz'} <- liftIO $ getCurrentTimeZone
          writeDateTimeWithSpec $ C8.pack tz'
        Just spec -> writeDateTimeWithSpec spec
    Just _ -> do
      --TODO: Can someone implement this? I am so pissed off!
      undefined
  where
    writeDateTimeWithSpec :: ByteString -> Writer Builder
    writeDateTimeWithSpec tz_name = do
      V.mapM_
        ( \case
            (CKInt32 i32) -> do
              converted <- liftIO $ convert_time_from_int32 i32
              writeBinaryInt32 converted
            (CKString time_str) -> do
              converted <-
                liftIO $
                  unsafeUseAsCStringLen
                    tz_name
                    ( \(tz, l) ->
                        unsafeUseAsCStringLen
                          time_str
                          (\(time_str, l2) -> c_write_time time_str tz l l2)
                    )
              writeBinaryInt32 converted
            _ -> error (typeMismatchError col_name)
        )
        items

foreign import ccall unsafe "datetime.h convert_time" c_convert_time :: Int64 -> CString -> Int -> IO CString

foreign import ccall unsafe "datetime.h convert_time" c_convert_time64 :: Float -> CString -> Int -> IO CString

foreign import ccall unsafe "datetime.h parse_time" c_write_time :: CString -> CString -> Int -> Int -> IO Int32

foreign import ccall unsafe "datetime.h convert_time_from_int32" convert_time_from_int32 :: Int32 -> IO Int32

------------------------------------------------------------------------------------------------
readLowCardinality :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readLowCardinality _ 0 _ = return (V.fromList [])
readLowCardinality server_info n spec = do
  readBinaryUInt64 --state prefix
  let l = BS.length spec
  let inner = BS.take (l - 16) (BS.drop 15 spec)
  serialization_type <- readBinaryUInt64
  -- Lowest bytes contains info about key type.
  let key_type = serialization_type .&. 0xf
  index_size <- readBinaryUInt64
  -- Strip the 'Nullable' tag to avoid null map reading.
  index <- readColumn server_info (fromIntegral index_size) (stripNullable inner)
  readBinaryUInt64 -- #keys
  keys <- case key_type of
    0 -> V.map fromIntegral <$> V.replicateM n readBinaryUInt8
    1 -> V.map fromIntegral <$> V.replicateM n readBinaryUInt16
    2 -> V.map fromIntegral <$> V.replicateM n readBinaryUInt32
    3 -> V.map fromIntegral <$> V.replicateM n readBinaryUInt64
  if "Nullable" `isPrefixOf` inner
    then do
      let nullable = fmap (\k -> if k == 0 then CKNull else index ! k) keys
      return nullable
    else return $ fmap (index !) keys
  where
    stripNullable :: ByteString -> ByteString
    stripNullable spec
      | "Nullable" `isPrefixOf` spec = BS.take (l - 10) (BS.drop 9 spec)
      | otherwise = spec
    l = BS.length spec

writeLowCardinality :: Context -> ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeLowCardinality ctx col_name spec items = do
  let inner = BS.take (BS.length spec - 16) (BS.drop 15 spec)
  (keys, index) <-
    if "Nullable" `isPrefixOf` inner
      then do
        --let null_inner_spec = BS.take (BS.length inner - 10) (BS.drop 9 spec)
        let hashedItem = hashItems True items
        let key_by_index_element = V.foldl' insertKeys Map.empty hashedItem
        let keys = V.map (\k -> key_by_index_element Map.! k + 1) hashedItem
        -- First element is NULL if column is nullable
        let index = V.fromList $ 0 : (Map.keys $ key_by_index_element)
        return (keys, index)
      else do
        let hashedItem = hashItems False items
        let key_by_index_element = V.foldl' insertKeys Map.empty hashedItem
        let keys = V.map (key_by_index_element Map.!) hashedItem
        let index = V.fromList $ Map.keys $ key_by_index_element
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
            if "Nullable" `isPrefixOf` inner
              then BS.take (BS.length inner - 10) (BS.drop 9 spec)
              else inner
      writeBinaryUInt64 1 --state prefix
      writeBinaryInt64 serialization_type
      writeBinaryInt64 $ fromIntegral $ V.length index
      writeColumn ctx col_name nullsInner items
      writeBinaryInt64 $ fromIntegral $ V.length items
      case int_type of
        0 -> V.mapM_ (writeBinaryUInt8 . fromIntegral) keys
        1 -> V.mapM_ (writeBinaryUInt16 . fromIntegral) keys
        2 -> V.mapM_ (writeBinaryUInt32 . fromIntegral) keys
        3 -> V.mapM_ (writeBinaryUInt64 . fromIntegral) keys
  where
    insertKeys :: (Hashable a, Eq a) => Map.HashMap a Int -> a -> Map.HashMap a Int
    insertKeys m a = if Map.member a m then m else Map.insert a (Map.size m) m

    hashItems :: Bool -> Vector ClickhouseType -> Vector Int
    hashItems isNullable items =
      V.map
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
readNullable :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readNullable server_info n_rows spec = do
  let l = BS.length spec
  let cktype = BS.take (l - 10) (BS.drop 9 spec) -- Read Clickhouse type inside the bracket after the 'Nullable' spec.
  config <- readNullableConfig n_rows
  items <- readColumn server_info n_rows cktype
  let result = V.generate n_rows (\i -> if config ! i == 1 then CKNull else items ! i)
  return result
  where
    readNullableConfig :: Int -> Reader (Vector Word8)
    readNullableConfig n_rows = do
      config <- readBinaryStrWithLength n_rows
      (return . V.fromList . BS.unpack) config

writeNullable :: Context -> ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeNullable ctx col_name spec items = do
  let l = BS.length spec
  let inner = BS.take (l - 10) (BS.drop 9 spec)
  writeNullsMap items
  writeColumn ctx col_name inner items
  where
    writeNullsMap :: Vector ClickhouseType -> Writer Builder
    writeNullsMap =
      V.mapM_
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

Here we don't implement in the form of BFS; instead, we use bottom up method:
First off, we compute the array of integer in which elements represent the size of subarrays (we call them spec arrays)
, where the function `readArraySpec`, `cut`, and `intervalize` do their jobs.
Second, we place the array of atomic elements (or flatten data) at the last position.
Then we cut the array of atomic elements according to the last spec array and form nested a nested array.
Finally we pop out the last spec array.
Repeat this process until all spec arrays are gone.

For example:
The target array is [[3, 4], [5, 6]]
The array on the right hand side of `|` is array of atomic elements.
The algorithm would be like this:
[2] [2,2] | [3,4,5,6]
-> [2] | [[3,4],[5,6]]
-> [[3,4],[5,6]]

For another example:
   target [[["Alex","Bob"], ["John"]],[["Jane"],["Steven","Mike","Sarah"]],[["Hello","world"]]]
   [3] [2,2,1] [2,1,1,3,2] | ["Alex","Bob","John","Jane","Steven","Mike","Sarah","Hello","world"]
-> [3] [2,2,1] | [["Alex","Bob"],["John"],["Jane"],["Steven","Mike","Sarah"],["Hello","world"]]
-> [3] | [[["Alex","Bob"],["John"]],[["Jane"],["Steven","Mike","Sarah"]],[["Hello","world"]]]
-> [[["Alex","Bob"],["John"]],[["Jane"],["Steven","Mike","Sarah"]],[["Hello","world"]]]

-}
readArray :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readArray server_info n_rows spec = do
  (lastSpec, x : xs) <- genSpecs spec [V.fromList [fromIntegral n_rows]]
  --  lastSpec which is not `Array`
  --  x:xs is the
  let numElem = fromIntegral $ V.sum x -- number of elements in the nested array.
  elems <- readColumn server_info numElem lastSpec
  let result' = foldl combine elems (x : xs)
  let CKArray arr = result' ! 0
  return arr
  where
    combine :: Vector ClickhouseType -> Vector Word64 -> Vector ClickhouseType
    combine elems config =
      let intervals = intervalize (fromIntegral <$> config)
          cut (a, b) = CKArray $ V.take b (V.drop a elems) -- cut element array
          embed = (\(l, r) -> cut (l, r - l + 1)) <$> intervals
       in embed
    intervalize :: Vector Int -> Vector (Int, Int)
    intervalize vec = V.drop 1 $ V.scanl' (\(_, b) v -> (b + 1, v + b)) (-1, -1) vec -- drop the first tuple (-1,-1)

readArraySpec :: Vector Word64 -> Reader (Vector Word64)
readArraySpec sizeArr = do
  let arrSum = (fromIntegral . V.sum) sizeArr
  offsets <- V.replicateM arrSum readBinaryUInt64
  let offsets' = V.cons 0 (V.take (arrSum - 1) offsets)
  let sizes = V.zipWith (-) offsets offsets'
  return sizes

genSpecs :: ByteString -> [Vector Word64] -> Reader (ByteString, [Vector Word64])
genSpecs spec rest@(x : _) = do
  let l = BS.length spec
  let cktype = BS.take (l - 7) (BS.drop 6 spec)
  if "Array" `isPrefixOf` spec
    then do
      next <- readArraySpec x
      genSpecs cktype (next : rest)
    else return (spec, rest)

writeArray :: Context -> ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeArray ctx col_name spec items = do
  let lens =
        V.scanl'
          ( \total ->
              ( \case
                  (CKArray xs) -> total + V.length xs
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
  V.mapM_ (writeBinaryInt64 . fromIntegral) (V.drop 1 lens)
  let innerSpec = BS.take (BS.length spec - 7) (BS.drop 6 spec)
  let innerVector = V.map (\case CKArray xs -> xs) items
  let flattenVector =
        innerVector >>= \v -> do v
  writeColumn ctx col_name innerSpec flattenVector

--------------------------------------------------------------------------------------
readTuple :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readTuple server_info n_rows spec = do
  let l = BS.length spec
  let innerSpecString = BS.take (l - 7) (BS.drop 6 spec) -- Tuple(...) the dots represent innerSpecString
  let arr = V.fromList (getSpecs innerSpecString)
  datas <- V.mapM (readColumn server_info n_rows) arr
  let transposed = transpose datas
  return $ CKTuple <$> transposed

writeTuple :: Context -> ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeTuple ctx col_name spec items = do
  let inner = BS.take (BS.length spec - 7) (BS.drop 6 spec)
  let spec_arr = V.fromList $ getSpecs inner
  let transposed =
        transpose
          ( V.map
              ( \case
                  CKTuple tupleVec -> tupleVec
                  other ->
                    error
                      ( "expected type: " ++ show other
                          ++ "in the column:"
                          ++ show col_name
                      )
              )
              items
          )
  if V.length spec_arr /= V.length transposed
    then
      error $
        "length of the given array does not match, column name = "
          ++ show col_name
    else do
      V.zipWithM_ (writeColumn ctx col_name) spec_arr transposed

--------------------------------------------------------------------------------------
readEnum :: Int -> ByteString -> Reader (Vector ClickhouseType)
readEnum n_rows spec = do
  let l = BS.length spec
      innerSpec =
        if "Enum8" `isPrefixOf` spec
          then BS.take (l - 7) (BS.drop 6 spec)
          else BS.take (l - 8) (BS.drop 7 spec) -- otherwise it is `Enum16`
      pres_pecs = getSpecs innerSpec
      specs =
        (\(name, Just (n, _)) -> (n, name))
          <$> ((\[x, y] -> (x, readInt y)) . BS.splitWith (== EQUAL) <$> pres_pecs) --61 means '='
      specsMap = Map.fromList specs
  if "Enum8" `isPrefixOf` spec
    then do
      values <- V.replicateM n_rows readBinaryInt8
      return $ CKString . (specsMap Map.!) . fromIntegral <$> values
    else do
      values <- V.replicateM n_rows readBinaryInt16
      return $ CKString . (specsMap Map.!) . fromIntegral <$> values

writeEnum :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeEnum col_name spec items = do
  let l = BS.length spec
      innerSpec =
        if "Enum8" `isPrefixOf` spec
          then BS.take (l - 7) (BS.drop 6 spec)
          else BS.take (l - 8) (BS.drop 7 spec)
      pres_pecs = getSpecs innerSpec
      specs =
        (\(name, Just (n, _)) -> (name, n))
          <$> ((\[x, y] -> (x, readInt y)) . BS.splitWith (== EQUAL) . BS.filter (/= QUOTE) <$> pres_pecs) --61 is '='
      specsMap = Map.fromList specs
  V.mapM_
    ( \case
        CKString str ->
          ( if "Enum8" `isPrefixOf` spec
              then writeBinaryInt8 $ fromIntegral $ specsMap Map.! str
              else writeBinaryInt16 $ fromIntegral $ specsMap Map.! str
          )
        CKNull ->
          if "Enum8" `isPrefixOf` spec
            then writeBinaryInt8 0
            else writeBinaryInt16 0
        _ -> error $ typeMismatchError col_name
    )
    items

----------------------------------------------------------------------
readDate :: Int -> Reader (Vector ClickhouseType)
readDate n_rows = do
  let epoch_start = fromGregorian 1970 1 1
  days <- V.replicateM n_rows readBinaryUInt16
  let dates = V.map (\x -> addDays (fromIntegral x) epoch_start) days
      toTriple = V.map toGregorian dates
      toCK = V.map (\(y, m, d) -> CKDate y m d) toTriple
  return toCK

writeDate :: ByteString -> Vector ClickhouseType -> Writer Builder
writeDate col_name items = do
  let epoch_start = fromGregorian 1970 1 1
  let serialize =
        V.map
          ( \case
              CKDate y m d -> diffDays (fromGregorian y m d) epoch_start
              _ ->
                error $
                  "unexpected type in the column: " ++ show col_name
                    ++ " whose type should be Date"
          )
          items
  V.mapM_ (writeBinaryInt16 . fromIntegral) serialize

--------------------------------------------------------------------------------------
readDecimal :: Int -> ByteString -> Reader (Vector ClickhouseType)
readDecimal n_rows spec = do
  let l = BS.length spec
  let inner_spec = getSpecs $ BS.take (l - 9) (BS.drop 8 spec)
  let (specific, Just (scale, _)) = case inner_spec of
        [] -> error "No spec"
        [scale'] ->
          if "Decimal32" `isPrefixOf` spec
            then (readDecimal32, readInt scale')
            else
              if "Decimal64" `isPrefixOf` spec
                then (readDecimal64, readInt scale')
                else (readDecimal128, readInt scale')
        [precision', scale'] -> do
          let Just (precision, _) = readInt precision'
          if precision <= 9 || "Decimal32" `isPrefixOf` spec
            then (readDecimal32, readInt scale')
            else
              if precision <= 18 || "Decimal64" `isPrefixOf` spec
                then (readDecimal64, readInt scale')
                else (readDecimal128, readInt scale')
  raw <- specific n_rows
  let final = fmap (trans scale) raw
  return final
  where
    readDecimal32 :: Int -> Reader (Vector ClickhouseType)
    readDecimal32 n_rows = readIntColumn n_rows "Int32"
    
    readDecimal64 :: Int -> Reader (Vector ClickhouseType)
    readDecimal64 n_rows = readIntColumn n_rows "Int64"

    readDecimal128 :: Int -> Reader (Vector ClickhouseType)
    readDecimal128 n_rows =
      V.replicateM n_rows $ do
        lo <- readBinaryUInt64
        hi <- readBinaryUInt64
        return $ CKUInt128 lo hi

    trans :: Int -> ClickhouseType -> ClickhouseType
    trans scale (CKInt32 x) = CKDecimal32 (fromIntegral x / fromIntegral (10 ^ scale))
    trans scale (CKInt64 x) = CKDecimal64 (fromIntegral x / fromIntegral (10 ^ scale))
    trans scale (CKUInt128 lo hi) = CKDecimal128 (word128_division hi lo scale)

writeDecimal :: ByteString -> ByteString -> Vector ClickhouseType -> Writer Builder
writeDecimal col_name spec items = do
  let l = BS.length spec
  let inner_specs = getSpecs $ BS.take (l - 9) (BS.drop 8 spec)
  let (specific, Just (pre_scale, _)) = case inner_specs of
        [] -> error "No spec"
        [scale'] ->
          if "Decimal32" `isPrefixOf` spec
            then (writeDecimal32, readInt scale')
            else
              if "Decimal64" `isPrefixOf` spec
                then (writeDecimal64, readInt scale')
                else (writeDecimal128, readInt scale')
        [precision', scale'] -> do
          let Just (precision, _) = readInt precision'
          if precision <= 9 || "Decimal32" `isPrefixOf` spec
            then (writeDecimal32, readInt scale')
            else
              if precision <= 18 || "Decimal64" `isPrefixOf` spec
                then (writeDecimal64, readInt scale')
                else (writeDecimal128, readInt scale')
  let scale = 10 ^ pre_scale
  specific scale items
  where
    writeDecimal32 :: Int -> Vector ClickhouseType -> Writer Builder
    writeDecimal32 scale vec =
      V.mapM_
        ( \case
            CKDecimal32 f32 -> writeBinaryInt32 $ fromIntegral $ floor $ (f32 * fromIntegral scale)
            _ -> error $ typeMismatchError col_name
        )
        vec
    writeDecimal64 :: Int -> Vector ClickhouseType -> Writer Builder
    writeDecimal64 scale vec =
      V.mapM_
        ( \case
            CKDecimal64 f64 -> writeBinaryInt32 $ fromIntegral $ floor $ (f64 * fromIntegral scale)
            _ -> error $ typeMismatchError col_name
        )
        vec
    writeDecimal128 :: Int -> Vector ClickhouseType -> Writer Builder
    writeDecimal128 scale items = do
      V.mapM_
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
readIPv4 :: Int -> Reader (Vector ClickhouseType)
readIPv4 n_rows = V.replicateM n_rows (CKIPv4 . ip4ToOctets . IP4 <$> readBinaryUInt32)

readIPv6 :: Int -> Reader (Vector ClickhouseType)
readIPv6 n_rows = V.replicateM n_rows (CKIPv6 . ip6ToWords . IP6 <$> readBinaryUInt128)

writeIPv4 :: ByteString -> Vector ClickhouseType -> Writer Builder
writeIPv4 col_name =
  V.mapM_
    ( \case
        CKIPv4 (w1, w2, w3, w4) ->
          writeBinaryUInt32 $
            unIP4 $
              ip4FromOctets w1 w2 w3 w4
        CKNull -> writeBinaryInt32 0
        _ -> error $ typeMismatchError col_name
    )

writeIPv6 :: ByteString -> Vector ClickhouseType -> Writer Builder
writeIPv6 col_name =
  V.mapM_
    ( \case
        CKIPv6 (w1, w2, w3, w4, w5, w6, w7, w8) ->
          writeBinaryUInt128 $
            unIP6 $
              ip6FromWords w1 w2 w3 w4 w5 w6 w7 w8
        CKNull -> writeBinaryUInt64 0
        _ -> error $ typeMismatchError col_name
    )

----------------------------------------------------------------------------------------------
readSimpleAggregateFunction :: ServerInfo -> Int -> ByteString -> Reader (Vector ClickhouseType)
readSimpleAggregateFunction server_info n_rows spec = do
  let l = BS.length spec
  let [func, cktype] = getSpecs $ BS.take (l - 25) (BS.drop 24 spec)
  readColumn server_info n_rows cktype
----------------------------------------------------------------------------------------------
readUUID :: Int -> Reader (Vector ClickhouseType)
readUUID n_rows = do
  V.replicateM n_rows $ do
    w2 <- readBinaryUInt32
    w1 <- readBinaryUInt32
    w3 <- readBinaryUInt32
    w4 <- readBinaryUInt32
    return $
      CKString $
        C8.pack $
          UUID.toString $ UUID.fromWords w1 w2 w3 w4

writeUUID :: ByteString -> Vector ClickhouseType -> Writer Builder
writeUUID col_name =
  V.mapM_
    ( \case
        CKString uuid_str -> do
          case UUID.fromString $ C8.unpack uuid_str of
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
    )

----------------------------------------------------------------------------------------------
---Helpers
#define COMMA 44
#define SPACE 32

-- | Get rid of commas and spaces
getSpecs :: ByteString -> [ByteString]
getSpecs str = BS.splitWith (== COMMA) (BS.filter (/= SPACE) str)

transpose :: Vector (Vector ClickhouseType) -> Vector (Vector ClickhouseType)
transpose = rotate 
  where
    rotate matrix =
      let transposedList = List.transpose (V.toList <$> V.toList matrix)
          toVector = V.fromList <$> V.fromList transposedList
       in toVector

typeMismatchError :: ByteString -> String
typeMismatchError col_name = "Type mismatch in the column " ++ show col_name

-- | print in format
putStrLn :: Vector (Vector ClickhouseType) -> IO ()
putStrLn v = C8.putStrLn $ BS.intercalate "\n" $ V.toList $ V.map to_str v
  where
    to_str :: Vector ClickhouseType -> ByteString
    to_str row = BS.intercalate "," $ V.toList $ V.map help row

    help :: ClickhouseType -> ByteString
    help (CKString s) = s
    help (CKDecimal64 n) = C8.pack $ show n
    help (CKDecimal32 n) = C8.pack $ show n
    help (CKDecimal n) = C8.pack $ show n
    help (CKInt8 n) = C8.pack $ show n
    help (CKInt16 n) = C8.pack $ show n
    help (CKInt32 n) = C8.pack $ show n
    help (CKInt64 n) = C8.pack $ show n
    help (CKUInt8 n) = C8.pack $ show n
    help (CKUInt16 n) = C8.pack $ show n
    help (CKUInt32 n) = C8.pack $ show n
    help (CKUInt64 n) = C8.pack $ show n
    help (CKTuple xs) = "(" <> to_str xs <> ")"
    help (CKArray xs) = "[" <> to_str xs <> "]"
    help  CKNull = "null"
    help (CKIPv4 ip4) = C8.pack $ show ip4
    help (CKIPv6 ip6) = C8.pack $ show ip6
    help (CKDate y m d) =
      C8.pack (show y)
        <> "-"
        <> C8.pack (show m)
        <> "-"
        <> C8.pack (show d)
