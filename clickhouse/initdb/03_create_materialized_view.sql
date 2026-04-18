CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_astros_to_people
TO core.people
AS
SELECT
    craft,
    name,
    _inserted_at
FROM
(
    SELECT
        JSONExtractString(person, 'craft') AS craft,
        JSONExtractString(person, 'name') AS name,
        _inserted_at
    FROM raw.astros_raw
    ARRAY JOIN JSONExtractArrayRaw(toJSONString(payload), 'people') AS person
)
WHERE craft != ''
  AND name != '';
