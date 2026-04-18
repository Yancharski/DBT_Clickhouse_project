# DBT + ClickHouse Project

This repo is a small local data pipeline around the Open Notify API:

- Docker runs ClickHouse with users, passwords, and persistent storage.
- A Python script downloads `http://api.open-notify.org/astros.json`.
- The raw API response is stored in ClickHouse as `JSON`.
- A materialized view parses that raw payload into a `people` table.
- dbt runs in a separate Docker container and builds analytics models on top.

The full flow looks like this:

```text
Open Notify API
    -> src/main.py
    -> raw.astros_raw
    -> raw.mv_astros_to_people
    -> core.people
    -> analytics.people_latest_incremental
    -> analytics.craft_summary
```

## Why `clickhouse-connect`

For this project, `clickhouse-connect` is a really nice fit:

- it works over HTTP on port `8123`, which is convenient for Docker and local dev;
- it works well on macOS ARM without extra native setup pain;
- it lets us insert into a ClickHouse `JSON` column with `JSONEachRow`;
- for runtime ingestion we stay in an insert-only approach, which matches the task requirements.

## Project layout

```text
.
├── clickhouse/initdb
│   ├── 01_create_databases.sql
│   ├── 02_create_tables.sql
│   ├── 03_create_materialized_view.sql
│   └── 04_create_users_and_grants.sh
├── dbt
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles/profiles.yml
│   └── models
├── docker-compose.yml
├── src
│   ├── main.py
│   └── requirements.txt
└── .env.example
```

## What is happening in ClickHouse

There are 3 main layers here:

- `raw.astros_raw`
Stores every API call as a separate row. This table is append-only and keeps the original payload in a `JSON` column.

- `raw.mv_astros_to_people`
A materialized view that reads raw payloads and extracts people from `payload.people`.

- `core.people`
Stores parsed rows with columns:
`craft`, `name`, `_inserted_at`

Deduplication is handled by ClickHouse itself with `ReplacingMergeTree(_inserted_at)`.
The natural key is `(craft, name)`.
After each Python ingestion run, the script executes:

```sql
OPTIMIZE TABLE core.people FINAL
```

So the latest version for each `(craft, name)` wins.

## Before you start

1. Check your `.env`.

There is already a local `.env` in the repo, and `.env.example` is here as a reference.

2. Start ClickHouse:

```bash
docker compose up -d clickhouse
```

If you already have an older ClickHouse volume and want a clean reset, run:

```bash
docker compose down -v
docker compose up -d clickhouse
```

3. Install Python dependencies locally:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r src/requirements.txt
```

## Run ingestion

```bash
python src/main.py
```

What the script does:

- calls `astros.json`;
- retries on `429`, `5xx`, timeout, and connection errors;
- respects `Retry-After` when it is present;
- uses exponential backoff with jitter;
- makes up to 5 attempts total, including the first one;
- inserts exactly 1 row into `raw.astros_raw` on success;
- runs `OPTIMIZE TABLE core.people FINAL` right after the insert.

## Logging and error handling

The Python script writes simple JSON logs to stdout.

You will see events like:

- start
- http success
- retry scheduled
- payload received
- clickhouse insert complete
- done

If all 5 attempts fail, the script raises an error as required.

## Quick ClickHouse checks

Count raw events:

```bash
docker compose exec clickhouse \
  clickhouse-client \
  --user admin \
  --password admin_password \
  --query "SELECT count() AS raw_rows FROM raw.astros_raw"
```

See parsed people:

```bash
docker compose exec clickhouse \
  clickhouse-client \
  --user admin \
  --password admin_password \
  --query "SELECT craft, name, _inserted_at FROM core.people FINAL ORDER BY craft, name"
```

See the latest raw payload:

```bash
docker compose exec clickhouse \
  clickhouse-client \
  --user admin \
  --password admin_password \
  --query "SELECT _inserted_at, toJSONString(payload) FROM raw.astros_raw ORDER BY _inserted_at DESC LIMIT 1"
```

## dbt in Docker

Build the dbt image:

```bash
docker compose build dbt
```

Check dbt connection:

```bash
docker compose run --rm dbt debug --profiles-dir /root/.dbt --project-dir /opt/dbt
```

Build models and run tests:

```bash
docker compose run --rm dbt build --profiles-dir /root/.dbt --project-dir /opt/dbt
```

Generate docs:

```bash
docker compose run --rm dbt docs generate --profiles-dir /root/.dbt --project-dir /opt/dbt
```

Serve docs at [http://localhost:8081](http://localhost:8081):

```bash
docker compose run --rm --service-ports dbt docs serve --profiles-dir /root/.dbt --project-dir /opt/dbt --host 0.0.0.0 --port 8081
```

## dbt models

### `analytics.people_latest_incremental`

This is an incremental model on top of `core.people`.
It keeps the flow insert-only and stores the latest known rows in the analytics layer.

### `analytics.craft_summary`

This is a simple mart with:

- `craft_family`
- `craft`
- `people_count`
- `latest_loaded_at`

It gives a quick summary of how many people are assigned to each craft and when that craft was last seen in the pipeline.

## Tests

The dbt project includes:

- `sources` for `raw.astros_raw` and `core.people`;
- `not_null` tests;
- an `accepted_values` test for `craft_family`.

## A couple of useful notes

- `raw.astros_raw` keeps every API call, even if the payload is the same as before.
- No Python-side deduplication is used.
- Deduplication is done by ClickHouse with `ReplacingMergeTree` plus `OPTIMIZE FINAL`.
- For reading the final deduplicated state right after ingestion, using `FINAL` is the safest option.
- If you ever want to run Python from a container instead of locally, you can point it to `CH_HOST=clickhouse`.

## Verified locally

This setup was checked with:

- successful ClickHouse startup in Docker;
- successful live ingestion from the Open Notify API;
- raw append-only behavior;
- parsed/deduplicated output in `core.people FINAL`;
- successful `dbt debug`;
- successful `dbt build`;
- successful `dbt docs generate`.
