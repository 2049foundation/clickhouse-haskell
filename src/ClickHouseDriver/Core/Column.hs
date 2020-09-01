{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BlockArguments #-}
module ClickHouseDriver.Core.Column(
  readColumn,
  ClickhouseType(..),
  transpose,
  writeColumn
) where

import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Data.Binary
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import Data.ByteString (ByteString, isPrefixOf)
import Data.ByteString.Char8 (readInt)
import Data.Int
import Data.Vector (Vector, (!), (!?))
import qualified Data.Vector as V
import Data.Word
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import Data.Time (Day, addDays, fromGregorian, toGregorian)
import Network.IP.Addr (IP4(..), IP6(..))
--Debug 
import Debug.Trace
import Data.Bits
import Data.UnixTime (formatUnixTimeGMT, UnixTime(..), webDateFormat)
import Foreign.C.Types (CTime(..))
import Data.Maybe (fromJust)
import Data.Int
import ClickHouseDriver.Core.Types
import Data.ByteString.Builder (Builder)
---------------------------------------------------------------------------------------
---Readers 

readColumn ::  Int -> ByteString -> Reader (Vector ClickhouseType)
readColumn n_rows spec
  | "String" `isPrefixOf` spec = V.replicateM n_rows (CKString <$> readBinaryStr)
  | "Array" `isPrefixOf` spec = readArray n_rows spec
  | "FixedString" `isPrefixOf` spec = readFixed n_rows spec
  | "DateTime" `isPrefixOf` spec = readDateTime n_rows spec
  | "Date"     `isPrefixOf` spec = readDate n_rows spec
  | "Tuple" `isPrefixOf` spec = readTuple n_rows spec
  | "Nullable" `isPrefixOf` spec = readNullable n_rows spec
  | "LowCardinality" `isPrefixOf` spec = readLowCadinality n_rows spec
  | "Decimal" `isPrefixOf` spec = readDecimal n_rows spec
  | "Enum" `isPrefixOf` spec = readEnum n_rows spec
  | "Int" `isPrefixOf` spec = readIntColumn n_rows spec
  | "UInt" `isPrefixOf` spec = readIntColumn n_rows spec
  | "IPv4" `isPrefixOf` spec = readIPv4 n_rows
  | "IPv6" `isPrefixOf` spec = readIPv6 n_rows
  | "SimpleAggregateFunction" `isPrefixOf` spec = readSimpleAggregateFunction n_rows spec
  | otherwise = error ("Unknown Type: " Prelude.++ C8.unpack spec)

writeColumn :: Context
             -- ^ context
             ->ByteString
             -- ^ column name
             ->ByteString
             -- ^ column type
             ->Vector ClickhouseType
             -- ^ items
             ->IOWriter Builder
writeColumn ctx col_name "String" items = writeStringColumn col_name items
writeColumn ctx col_name fix items
  | "FixedString(" `isPrefixOf` fix = do
    let l = BS.length fix
    let Just (len, _) = readInt $ BS.take(l - 13) (BS.drop 12 fix)
    writeFixedLengthString col_name (fromIntegral len) items
writeColumn ctx col_name int items
  | "Int" `isPrefixOf` int || "UInt" `isPrefixOf` int = do
    let Just (indicator, _) = readInt $ BS.drop 3 int
    writeIntColumn indicator col_name items
writeColumn ctx col_name null items
  | "Nullable(" `isPrefixOf` null = do
    let l = BS.length null
    let inner = BS.take (l - 10) (BS.drop 9 null)
    writeNullsMap items
    let filterNulls =
          V.filter
            ( \case
                CKNull -> True
                _ -> False
            )
            items
    writeColumn ctx col_name inner filterNulls
writeColumn _ _ _ _ = undefined

---------------------------------------------------------------------------------------------
readFixed :: Int -> ByteString -> Reader (Vector ClickhouseType)
readFixed n_rows spec = do
  let l = BS.length spec
  let strnumber = BS.take (l - 13) (BS.drop 12 spec)
  let number = case readInt strnumber of
        Nothing -> 0 -- This can't happen
        Just (x, _) -> x
  result <- V.replicateM n_rows (readFixedLengthString number)
  return result

readFixedLengthString :: Int -> Reader ClickhouseType
readFixedLengthString strlen = (CKString) <$> (readBinaryStrWithLength strlen)

writeStringColumn :: ByteString->Vector ClickhouseType->IOWriter Builder
writeStringColumn col_name = V.mapM_ 
  (\case CKString s -> writeBinaryStr s; 
          _ -> error (typeMismatchError col_name))

writeFixedLengthString :: ByteString->Word->Vector ClickhouseType->IOWriter Builder
writeFixedLengthString col_name len items = do
  V.mapM_ (\case CKString s->writeBinaryFixedLengthStr len s
                 _ -> error (typeMismatchError col_name)) items
---------------------------------------------------------------------------------------------
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

writeIntColumn :: Int->ByteString->Vector ClickhouseType -> IOWriter Builder
writeIntColumn indicator col_name =
  case indicator of 
    8 -> V.mapM_ 
        (\case CKInt8 x -> writeBinaryInt8 x
               _ -> error (typeMismatchError col_name)
        )
    16 -> V.mapM_ 
        (\case CKInt16 x -> writeBinaryInt16 x
               _ -> error (typeMismatchError col_name)
        )
    32 -> V.mapM_ 
        (\case CKInt32 x -> writeBinaryInt32 x
               _ -> error (typeMismatchError col_name)
        )
    64 -> V.mapM_ 
        (\case CKInt64 x -> writeBinaryInt64 x
               _ -> error (typeMismatchError col_name)
        )
  
writeUIntColumn :: Int->ByteString->Vector ClickhouseType -> IOWriter Builder
writeUIntColumn indicator col_name =
  case indicator of 
    8 -> V.mapM_ 
        (\case CKUInt8 x -> writeBinaryUInt8 x
               _ -> error (typeMismatchError col_name)
        )
    16 -> V.mapM_ 
        (\case CKUInt16 x -> writeBinaryInt16 $ fromIntegral x
               _ -> error (typeMismatchError col_name)
        )
    32 -> V.mapM_ 
        (\case CKUInt32 x -> writeBinaryInt32 $ fromIntegral x
               _ -> error (typeMismatchError col_name)
        )
    64 -> V.mapM_ 
        (\case CKUInt64 x -> writeBinaryInt64 $ fromIntegral x
               _ -> error (typeMismatchError col_name)
        )
---------------------------------------------------------------------------------------------
readDateTime :: Int -> ByteString -> Reader (Vector ClickhouseType)
readDateTime n_rows spec = do
  let (scale, spc) = readTimeSpec spec
  case spc of
    Nothing -> error "Error : can't read localzone"
    Just tz_name -> readDateTimeWithSpec n_rows scale tz_name
  where
    readTimeSpec :: ByteString -> (Maybe Int, Maybe ByteString)
    readTimeSpec spec'
      | "DateTime64" `isPrefixOf` spec' = do
        let l = BS.length spec'
        let innerspecs = BS.take (l - 11) (BS.drop 10 spec')
        let splited = getSpecs innerspecs
        case splited of
          [] -> (Nothing, Nothing)
          [x] -> (Just $ fst $ fromJust $ readInt x, Just "")
          [x, y] -> (Just $ fst $ fromJust $ readInt x, Just y)
      | otherwise = do
        let l = BS.length spec'
        let innerspecs = BS.take (l - 9) (BS.drop 8 spec')
        (Nothing, Just innerspecs)
    readDateTimeWithSpec :: Int -> Maybe Int -> ByteString -> Reader (Vector ClickhouseType)
    readDateTimeWithSpec n_rows Nothing tz_name = do
      data32 <- readIntColumn n_rows "Int32"
      let toDateTimeString =
            V.map
              ( \(CKInt32 x) ->
                  formatUnixTimeGMT webDateFormat $
                    UnixTime (CTime $ fromIntegral x) 0
              )
              data32
      return $ V.map CKString toDateTimeString
    readDateTimeWithSpec n_rows (Just scl) tz_name = do
      data64 <- readIntColumn n_rows "Int64"
      let scale = 10 ^ fromIntegral scl
      let toDateTimeString =
            V.map
              ( \(CKInt64 x) ->
                  formatUnixTimeGMT webDateFormat $
                    UnixTime (CTime $ x `div` scale) 0
              )
              data64
      return $ V.map CKString toDateTimeString

readLowCadinality :: Int -> ByteString -> Reader (Vector ClickhouseType)
readLowCadinality 0 _ = return (V.fromList [])
readLowCadinality n spec = do
  readBinaryUInt64 --prefix
  let l = BS.length spec
  let inner = BS.take (l - 16) (BS.drop 15 spec)
  serialization_type <- readBinaryUInt64
  -- Lowest bytes contains info about key type.
  let key_type = serialization_type .&. 0xf
  index_size <- readBinaryUInt64
  -- Strip the 'Nullable' tag to avoid null map reading.
  index <- readColumn (fromIntegral index_size) (stripNullable inner)
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
    else return $ fmap (\k -> index ! k) keys
  where
    stripNullable :: ByteString -> ByteString
    stripNullable spec
      | "Nullable" `isPrefixOf` spec = BS.take (l - 10) (BS.drop 9 spec)
      | otherwise = spec
    l = BS.length spec
---------------------------------------------------------------------------------------------------------------------------------
{-
          Informal description for this config:
          (\Null | \SOH)^{n_rows}
-}
readNullable :: Int->ByteString->Reader (Vector ClickhouseType)
readNullable n_rows spec = do
  let l = BS.length spec
  let cktype = BS.take (l - 10) (BS.drop 9 spec) -- Read Clickhouse type inside the bracket after the 'Nullable' spec.
  config <- readNullableConfig n_rows spec
  items <- readColumn n_rows cktype
  let result = V.generate n_rows (\i->if config ! i == 1 then CKNull else items ! i)
  return result
    where
      readNullableConfig :: Int->ByteString->Reader (Vector Word8)
      readNullableConfig n_rows spec = do
        config <- readBinaryStrWithLength n_rows
        (return . V.fromList . BS.unpack) config

writeNullsMap :: Vector ClickhouseType -> IOWriter Builder
writeNullsMap = V.mapM_ 
  (\case CKNull-> writeBinaryInt8 1
         _ -> writeBinaryInt8 0)
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
-}

readArray :: Int -> ByteString -> Reader (Vector ClickhouseType)
readArray n_rows spec = do
  (lastSpec, x : xs) <- genSpecs spec [V.fromList [fromIntegral n_rows]]
  let numElem = fromIntegral $ V.sum x
  elems <- readColumn numElem lastSpec
  let result' = foldl combine elems (x : xs)
  let result = case (result' ! 0) of
        CKArray arr -> arr
        _ -> error "wrong type. This cannot happen"
  return result
  where
    combine :: Vector ClickhouseType -> Vector Word64 -> Vector ClickhouseType
    combine elems config =
      let intervals = intervalize (fromIntegral <$> config)
          cut (a, b) = CKArray $ V.take b (V.drop a elems)
          embed = (\(l, r) -> cut (l, r - l + 1)) <$> intervals
       in embed
    intervalize :: Vector Int -> Vector (Int, Int)
    intervalize vec = V.drop 1 $ V.scanl' (\(a, b) v -> (b + 1, v + b)) (-1, -1) vec
    readArraySpec :: Vector Word64 -> Reader (Vector Word64)
    readArraySpec sizeArr = do
      let arrSum = (fromIntegral . V.sum) sizeArr
      offsets <- V.replicateM arrSum readBinaryUInt64
      let offsets' = V.cons 0 (V.take (arrSum - 1) offsets)
      let sizes = V.zipWith (-) offsets offsets'
      return sizes
    genSpecs :: ByteString -> [Vector Word64] -> Reader (ByteString, [Vector Word64])
    genSpecs spec rest@(x : xs) = do
      let l = BS.length spec
      let cktype = BS.take (l - 7) (BS.drop 6 spec)
      if "Array" `isPrefixOf` spec
        then do
          next <- readArraySpec x
          genSpecs cktype (next : rest)
        else return (spec, rest)

readTuple :: Int->ByteString->Reader (Vector ClickhouseType)
readTuple n_rows spec = do
  let l = BS.length spec
  let innerSpecString = BS.take(l - 7) (BS.drop 6 spec)
  let arr = V.fromList (getSpecs innerSpecString) 
  datas <- V.mapM (readColumn n_rows) arr
  let transposed = transpose datas
  return $ CKTuple <$> transposed

readEnum :: Int -> ByteString -> Reader (Vector ClickhouseType)
readEnum n_rows spec = do
  let l = BS.length spec
      innerSpec =
        if "Enum8" `isPrefixOf` spec
          then BS.take (l - 7) (BS.drop 6 spec)
          else BS.take (l - 8) (BS.drop 7 spec)
      prespecs = getSpecs innerSpec
      specs = (\(name, Just (n, _)) -> (n, name)) <$> ((toTuple . BS.splitWith (== 61)) <$> prespecs) --61 is '='
      specsMap = Map.fromList specs
  if "Enum8" `isPrefixOf` spec
    then do
      vals <- V.replicateM n_rows readBinaryInt8
      return $ (CKString . (specsMap Map.!) . fromIntegral) <$> vals
    else do
      vals <- V.replicateM n_rows readBinaryInt16
      return $ (CKString . (specsMap Map.!) . fromIntegral) <$> vals
  where
    toTuple [x, y] = (x, readInt y)

readDate :: Int->ByteString->Reader(Vector ClickhouseType)
readDate n_rows spec = do
  let epoch_start = fromGregorian 1970 1 1
  days <- V.replicateM n_rows readBinaryUInt16
  let dates = fmap (\x->addDays (fromIntegral x) epoch_start) days
      toTriple = fmap toGregorian dates
      toCK = fmap (\(y, m, d)->CKDate y m d) toTriple
  return toCK

readDecimal :: Int->ByteString->Reader(Vector ClickhouseType)
readDecimal n_rows spec = do
  let l = BS.length spec 
  let [precision', scale'] = getSpecs $ BS.take(l - 9) (BS.drop 8 spec)
  let (Just (precision,_), Just (scale,_)) = (readInt precision', readInt scale')
  let specific = 
        if precision <= 9
          then readDecimal32
          else if precision <= 18
            then readDecimal64
            else readDecimal128
  
  raw <- specific n_rows
  let final = fmap (trans scale) raw
  return final
  where
    readDecimal32 n_rows = readIntColumn n_rows "Int32"
    readDecimal64 n_rows = readIntColumn n_rows "Int64"
    readDecimal128 n_rows = undefined
    trans :: Int->ClickhouseType->ClickhouseType
    trans scale (CKInt32 x) = CKDecimal32 (fromIntegral x / fromIntegral scale)
    trans scale (CKInt64 x) = CKDecimal64 (fromIntegral x / fromIntegral scale)

readIPv4 :: Int->Reader (Vector ClickhouseType)
readIPv4 n_rows = V.replicateM n_rows (CKIPv4 . IP4 <$> readBinaryUInt32)

readIPv6 :: Int->Reader (Vector ClickhouseType)
readIPv6 n_rows = V.replicateM n_rows (CKIPv6 . IP6  <$> readBinaryUInt128)

readSimpleAggregateFunction :: Int->ByteString->Reader (Vector ClickhouseType)
readSimpleAggregateFunction n_rows spec = do
   let l = BS.length spec
   let [func, cktype] = getSpecs $ BS.take(l - 25) (BS.drop 24 spec)
   readColumn n_rows cktype

---------------------------------------------------------------------------------------------
---Helpers 

-- | Get rid of commas and spaces
getSpecs :: ByteString -> [ByteString]
getSpecs str = BS.splitWith (==44) (BS.filter ( /= 32) str) 

transpose :: Vector (Vector ClickhouseType) -> Vector (Vector ClickhouseType)
transpose cdata =
  rotate cdata
  where
    rotate matrix =
      let transposedList = List.transpose (V.toList <$> V.toList matrix)
          toVector = V.fromList <$> (V.fromList transposedList)
       in toVector

typeMismatchError :: ByteString->String
typeMismatchError col_name = "Type mismatch in the column " ++ (show col_name)
-- | print in format
{-
format :: Vector (Vector ClickhouseType) -> ByteString
format v = foldl1 () V.map tostr v
  where
    tostr :: Vector ClickhouseType -> ByteString
    tostr row = V.map help row

    help :: ClickhouseType->ByteString
    help (CKString s) = s
    help (CKInt8 n) = C8.pack $ show n
    help (CKInt16 n) = C8.pack $ show n
    help (CKInt32 n) = C8.pack $ show n
    help (CKInt64 n) = C8.pack $ show n
    help (CKUInt8 n) = C8.pack $ show n
    help (CKUInt16 n) = C8.pack $ show n
    help (CKUInt32 n) = C8.pack $ show n
    help (CKUInt64 n) = C8.pack $ show n
    help (CKFixedLengthString _ s) = s
    help _ = ""
-}