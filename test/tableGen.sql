CREATE TABLE IF NOT EXISTS crd2 (
    `id` Nullable(FixedString(3)),
    `card` LowCardinality(String),
    `cardn` LowCardinality(Nullable(String))
) 
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO crd2 VALUES ('abc', 'myString', null), ('xyz', 'Noctis', 'Ross')