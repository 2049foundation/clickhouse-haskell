CREATE TABLE IF NOT EXISTS int_test (
    `id` Nullable(FixedString(10)),
    `item` Nullable(String),
    `number` Int16,
    `nullInt` Nullable(Int16)
) 
ENGINE = MergeTree()
ORDER BY tuple();

--INSERT INTO test_table2 (`id`, `item`,`number`,`Strings`) VALUES ('0000000001', Null,1557,['Hello','World','Darling']),('1234567890', 'CONNOR',533,['my', 'dear']),('3543364534', 'MARRY',220,['I', 'Love', 'You']),('2258864346', 'JAME',4452,['who', 'are', 'you'])

INSERT INTO int_test (`id`, `item`,`number`,`nullInt`) VALUES (null, 'Kanade', 14, null),('0000000002', 'John', 14, 1), (null, 'Wang', 22, null), (null, 'Wang', 11, 12)

