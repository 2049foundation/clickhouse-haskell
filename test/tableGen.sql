CREATE TABLE IF NOT EXISTS array_t (
    `id` Nullable(FixedString(3)),
    `items` Array(Array(Int16))
) 
ENGINE = MergeTree()
ORDER BY tuple();

--INSERT INTO test_table2 (`id`, `item`,`number`,`Strings`) VALUES ('0000000001', Null,1557,['Hello','World','Darling']),('1234567890', 'CONNOR',533,['my', 'dear']),('3543364534', 'MARRY',220,['I', 'Love', 'You']),('2258864346', 'JAME',4452,['who', 'are', 'you'])

INSERT INTO array_t (`id`, `items`) VALUES ('abc', [[1,2],[3,4]]), ('edf', [[1],[2]]), ('xyz', [[1,1,2],[3,3],[4,4,6],[9]]), ('uvw', [[1024]])

