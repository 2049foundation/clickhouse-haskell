CREATE TABLE IF NOT EXISTS tuple (
    `id` Nullable(FixedString(3)),
    `items` Tuple(Int16, String)
) 
ENGINE = MergeTree()
ORDER BY tuple();

--INSERT INTO test_table2 (`id`, `item`,`number`,`Strings`) VALUES ('0000000001', Null,1557,['Hello','World','Darling']),('1234567890', 'CONNOR',533,['my', 'dear']),('3543364534', 'MARRY',220,['I', 'Love', 'You']),('2258864346', 'JAME',4452,['who', 'are', 'you'])

INSERT INTO tuple (`id`, `items`) VALUES ('abc', (1, 'John')), ('edf', (2, 'Kanade')), ('xyz', (10, 'Emilia')), ('uvw', (20, 'rem'))

