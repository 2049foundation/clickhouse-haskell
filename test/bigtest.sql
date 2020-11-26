CREATE TABLE IF NOT EXISTS big (
    `id` FixedString(3),
    `item` String,
    `number` Int16,
    `numArray` Array(Int16),
    `nullInt16` Nullable(Int16),
    `arrayString` Array(String),
    `enum` Enum('Hello' = 1, 'World' = 2, 'clickhouse' = 3, 'driver' = 4),
    `nullenum` Nullable(Enum('Hello' = 1, 'World' = 2, 'clickhouse' = 3, 'driver' = 4)),
    `Tuple` Tuple(Int16, Int8, String, FixedString(5))
) ENGINE = MergeTree()
ORDER BY tuple();


INSERT INTO big (`id`, `item`,`number`,`numArray`, `nullInt16`, `arrayString`, `enum`, `nullenum`, `Tuple`)
VALUES ('abc', 'New York', 100, [1,2,3,4,5], null, ['abc', 'efg', 'xyzss'],'clickhouse', 'clickhouse', (500, 2, 'string', 'abcde')), ('abc', 'New York', 100, [1,2,3,4,5], null, ['abc', 'efg', 'xyzss'],'clickhouse', null, (500, 2, 'string', 'abcde')), ('abc', 'New York', 100, [1,2,3,4,5], null, ['abc', 'efg', 'xyzss'],'clickhouse', null, (500, 2, 'string', 'abcde')), ('abc', 'New York', 100, [1,2,3,4,5], null, ['abc', 'efg', 'xyzss'],'clickhouse', null, (500, 2, 'string', 'abcde')), ('abc', 'New York', 100, [1,2,3,4,5], null, ['abc', 'efg', 'xyzss'],'clickhouse', null, (500, 2, 'string', 'abcde'))