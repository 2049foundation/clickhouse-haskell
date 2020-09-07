CREATE TABLE IF NOT EXISTS array_nulls (
    `id` Nullable(FixedString(10)),
    `arr` Array(Array(Nullable(String)))
) 
ENGINE = MergeTree()
ORDER BY tuple();

