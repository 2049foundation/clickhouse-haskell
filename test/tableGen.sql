CREATE TABLE IF NOT EXISTS tande (
    `id` Nullable(FixedString(3)),
    `items` Tuple(Int16, String, String, Int8),
    `x` Enum('hello' = 1, 'world' = 2)
) 
ENGINE = MergeTree()
ORDER BY tuple();

--INSERT INTO test_table2 (`id`, `item`,`number`,`Strings`) VALUES ('0000000001', Null,1557,['Hello','World','Darling']),('1234567890', 'CONNOR',533,['my', 'dear']),('3543364534', 'MARRY',220,['I', 'Love', 'You']),('2258864346', 'JAME',4452,['who', 'are', 'you'])

INSERT INTO tande (`id`, `items`, `x`) VALUES ('abc', (1, 'John', 'good',0), 'hello'), ('edf', (2, 'Kanade', 'bad', 7),'world'), ('xyz', (10, 'Emilia', 'violent', 55),'hello'), ('uvw', (20, 'rem', 'waifu', -12),'world')


