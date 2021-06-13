SET allow_experimental_low_cardinality_type = 1;

CREATE TABLE IF NOT EXISTS customer
(
        CCUSTKEY       UInt32,
        CNAME          String,
        CADDRESS       String,
        CCITY          LowCardinality(String),
        CNATION        LowCardinality(String),
        CREGION        LowCardinality(String),
        CPHONE         String,
        CMKTSEGMENT    LowCardinality(String)
)
ENGINE = MergeTree ORDER BY (CCUSTKEY);

CREATE TABLE IF NOT EXISTS  lineorder
(
    LOORDERKEY             UInt32,
    LOLINENUMBER           UInt8,
    LOCUSTKEY              UInt32,
    LOPARTKEY              UInt32,
    LOSUPPKEY              UInt32,
    LOORDERDATE            Date,
    LOORDERPRIORITY        LowCardinality(String),
    LOSHIPPRIORITY         UInt8,
    LOQUANTITY             UInt8,
    LOEXTENDEDPRICE        UInt32,
    LOORDTOTALPRICE        UInt32,
    LODISCOUNT             UInt8,
    LOREVENUE              UInt32,
    LOSUPPLYCOST           UInt32,
    LOTAX                  UInt8,
    LOCOMMITDATE           Date,
    LOSHIPMODE             LowCardinality(String)
)
ENGINE = MergeTree PARTITION BY toYear(LOORDERDATE) ORDER BY (LOORDERDATE, LOORDERKEY);

CREATE TABLE IF NOT EXISTS  part
(
        PPARTKEY       UInt32,
        PNAME          String,
        PMFGR          LowCardinality(String),
        PCATEGORY      LowCardinality(String),
        PBRAND         LowCardinality(String),
        PCOLOR         LowCardinality(String),
        PTYPE          LowCardinality(String),
        PSIZE          UInt8,
        PCONTAINER     LowCardinality(String)
)
ENGINE = MergeTree ORDER BY PPARTKEY;

CREATE TABLE IF NOT EXISTS  supplier
(
        SSUPPKEY       UInt32,
        SNAME          String,
        SADDRESS       String,
        SCITY          LowCardinality(String),
        SNATION        LowCardinality(String),
        SREGION        LowCardinality(String),
        SPHONE         String
)
ENGINE = MergeTree ORDER BY SSUPPKEY;

CREATE TABLE IF NOT EXISTS test
(
        INT32          Int32,
        Name           String,
        null_name      Nullable(String),
        fix            FixedString(5),
        array          Array(Array(Int64)),
        nullArray      Array(Array(Nullable(String))),
        tuple          Tuple(String, Int32),
        enum           Enum8('spring' = 1, 'summer'= 2, 'autumn' = 3, 'winter' = 4),
        lowcard        LowCardinality(String)     
)
ENGINE = Memory;

INSERT INTO test VALUES 
 (53341, 'Hello', null, 'abcde', [[1,2,3],[1,2],[4,5,6]], [['hel','ola', null], [null]], ('Hello', 3252), 'spring', 'lowcard');
