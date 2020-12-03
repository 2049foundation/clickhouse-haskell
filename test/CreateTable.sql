CREATE TABLE IF NOT EXISTS nulls_table (
    `id` Nullable(FixedString(10)),
    `item` Nullable(String),
    `number` Nullable(Int16)
) 
ENGINE = MergeTree()
ORDER BY tuple();


