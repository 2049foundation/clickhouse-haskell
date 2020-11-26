<<<<<<< HEAD
CREATE TABLE IF NOT EXISTS default.test_table (
    `id` FixedString(10),
    `item` String,
    `number` Int16,
    `numArray` Array(Int16)
) ENGINE = MergeTree()
ORDER BY tuple();



=======
CREATE TABLE IF NOT EXISTS nulls_table (
    `id` Nullable(FixedString(10)),
    `item` Nullable(String),
    `number` Nullable(Int16)
) 
ENGINE = MergeTree()
ORDER BY tuple();

>>>>>>> 380b061d9d16ed42724f5da6b8a080b8ade86777

