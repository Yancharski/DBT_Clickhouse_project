CREATE TABLE IF NOT EXISTS raw.astros_raw
(
    _inserted_at DateTime64(3, 'UTC'),
    payload JSON
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(_inserted_at)
ORDER BY (_inserted_at);

CREATE TABLE IF NOT EXISTS core.people
(
    craft String,
    name String,
    _inserted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_inserted_at)
PARTITION BY tuple()
ORDER BY (craft, name);
