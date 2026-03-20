CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_astros_to_people
TO core.people
AS
SELECT
    craft,
    name,
    _inserted_at
FROM raw.astros_raw
ARRAY JOIN
    payload.people[].craft.:String AS craft,
    payload.people[].name.:String  AS name;
