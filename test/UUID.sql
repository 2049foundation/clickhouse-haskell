CREATE TABLE IF NOT EXISTS UUID_test (
    `myid` Nullable(UUID)
) 
ENGINE = MergeTree()
ORDER BY tuple();