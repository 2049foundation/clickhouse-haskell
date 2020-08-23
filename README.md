**Clickhouse-haskell**
======================
ClickHouse Haskell Driver with HTTP and native (TCP) interface support.
However, the native interface does not support insertion, i.e. read only.

**Features**
========

* External Data for query processing
* Types support
    * [U]Int8/16/32/64
    * String/FixedString(N)
    * Array(T)
    * Nullable(T)
    * Decimal
    * SimpleAggregateFunction(F, T)
    * Tuple(T1, T2, ...)
    * Date/DateTime('timezone')/DateTime64('timezone')
    * Enum8/16
    * Nested
* Query progress information.
* Block by block results streaming.
* Reading query profile info.
* Receiving server logs.

**Usage of the HTTP Client**
=====

In the HTTP client, data can be fetched from Clickhouse server in the format of pure text string, and structured JSON in which user can read the value according to a given key.

## **Example of data fetch using http client**
```Haskell
import ClickHouseDriver.Core
import qualified Data.Text.IO as TIO

main :: IO()
main = do
    env <- httpClient "default" "" --username and password
    showtables <- runQuery env (getText "SHOW TABLES")
    TIO.putStr showtables
```
Result
```
    Fetching 1 queries.
    array0
    array1
    array_t
    array_test
    array_test2
    big
    cardin
    crd
    crd2
    dt
    int_test
    ip
    t
    tande
    tande2
    tande3
    test_table
    test_table2
    test_table3
    test_table4
    test_table5
    test_table6
    tuple
    tuple2
```
```Haskell

    puretext <- runQuery env (getText "SELECT * FROM test_table")
    TIO.putStr puretext
```
stdout: 
```
    Fetching 1 queries.
    9987654321      Suzuki  12507   [667]
    9987654321      Suzuki  12507   [667]
    0000000001      JOHN    1557    [45,45,45]
    1234567890      CONNOR  533     [1,2,3,4]
    3543364534      MARRY   220     [0,1,2,3,121,2]
    2258864346      JAME    4452    [42,-10988,66,676,0]
    0987654321      Connan  9984    [24]
    0987654321      Connan  9984    [24]
    9987654321      Suzuki  12507   [667]
```
```Haskell 
    json <- runQuery env (getJSON "SELECT * FROM test_table")
    print json
```
stdout:
```
    Right [fromList [("numArray",Array [Number 45.0,Number 45.0,Number 45.0]),("item",   String "JOHN"),("id",String "0000000001"),("number",Number 1557.0)],fromList [("numArray",Array [Number 1.0,Number 2.0,Number 3.0,Number 4.0]),("item",String "CONNOR"),("id",String "1234567890"),("number",Number 533.0)],fromList [("numArray",Array [Number 0.0,Number 1.0,Number 2.0,Number 3.0,Number 121.0,Number 2.0]),("item",String "MARRY"),("id",String "3543364534"),("number",Number 220.0)],fromList [("numArray",Array [Number 42.0,Number -10988.0,Number 66.0,Number 676.0,Number 0.0]),("item",String "JAME"),("id",String "2258864346"),("number",Number 4452.0)],fromList [("numArray",Array [Number 24.0]),("item",String "Connan"),("id",String "0987654321"),("number",Number 9984.0)],fromList [("numArray",Array [Number 24.0]),("item",String "Connan"),("id",String "0987654321"),("number",Number 9984.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)]]
```

There is also a built-in Clickhouse type for user to send data in rows to Clickhouse server.

## **Algebraic data type for the Clickhouse types** 

```Haskell 
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
  | CKTuple (Vector ClickhouseType)
  | CKArray (Vector ClickhouseType)
  | CKDecimal32 Float
  | CKDecimal64 Float
  | CKDecimal128 Float
  | CKDateTime
  | CKIPv4 IP4
  | CKIPv6 IP6
  | CKDate {
    year :: !Integer,
    month :: !Int,
    day :: !Int 
  }
  | CKNull
  deriving (Show, Eq)
```

## **Example of sending data from memory**
```Haskell
main = do
  env <- httpClient "default" "12345612341"
  create <- exec "CREATE TABLE test (x Int32) ENGINE = Memory" env
  print create
  isSuccess <- insertOneRow "test" [CKInt32 100] env
  print isSuccess
  result <- runQuery env (getText "select * from test")
  TIO.putStr result
```
stdout:
```
Right "Inserted successfully"
Right "Inserted successfully"
Fetching 1 queries.
100
```

## **Example of sending data from CSV**
```Haskell
main :: IO()
main = do
    env <- httpClient "default" "12345612341"
    isSuccess <- insertFromFile "test_table" CSV "./test/example.csv" env
    putStr (case isSuccess of
        Right y -> y
        Left x -> CL8.unpack x)
    query <- runQuery env (getText "SELECT * FROM test_table")
    TIO.putStr query
```
where in example.csv
```CSV
0000000011,Bob,123,'[1,2,3]'
0000000012,Bob,124,'[4,5,6]'
0000000013,Bob,125,'[7,8,9,10]'
0000000014,Bob,126,'[11,12,13]'
```

stdout:

```
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000011      Bob     123     [1,2,3]
0000000012      Bob     124     [4,5,6]
0000000013      Bob     125     [7,8,9,10]
0000000014      Bob     126     [11,12,13]
```

**Usage of the Native(TCP) interface**
==========================

So far the native interface does not support insertion query. Query results using this interface will only display in the form of the ADT mentioned above. 

## **Example of showing how to make query with the native interface**
```Haskell
main :: IO ()
main = do
    env <- defaultClient --localhost 9000
    res <- execute "SHOW TABLES" env
    print res
```
stdout:
``` Haskell
[[CKString "test"],[CKString "test_table"],[CKString "test_table2"]]
```