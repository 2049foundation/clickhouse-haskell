CREATE TABLE IF NOT EXISTS simple_table (
    `id` FixedString(10),
    `item` String,
    `number` Int16
) 
ENGINE = MergeTree()
ORDER BY tuple();



INSERT INTO simple_table (`id`, `item`,`number`) VALUES ('0000000001', 'JOHN',1557),('1234567890', 'CONNOR',533),('3543364534', 'MARRY',220),('2258864346', 'JAME',4452)
