CREATE TABLE IF NOT EXISTS default.test_table2 (
    `id` FixedString(10),
    `item` String,
    `number` Int16,
    `Strings` Array(String)
) ENGINE = MergeTree()
ORDER BY tuple();




INSERT INTO default.test_table2 (`id`, `item`,`number`,`Strings`) VALUES ('0000000001', 'JOHN',1557,['Hello','World','Darling']),('1234567890', 'CONNOR',533,['my', 'dear']),('3543364534', 'MARRY',220,['I', 'Love', 'You']),('2258864346', 'JAME',4452,['who', 'are', 'you'])
