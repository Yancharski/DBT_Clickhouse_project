#!/usr/bin/env bash
set -euo pipefail


echo "[initdb] Creating users and grants..."

clickhouse-client \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --multiquery <<SQL

-- Create ingest user
CREATE USER IF NOT EXISTS "${CH_INGEST_USER}"
IDENTIFIED WITH sha256_password BY '${CH_INGEST_PASSWORD}';

-- Minimal rights for ingestion
GRANT INSERT ON raw.astros_raw TO "${CH_INGEST_USER}";
GRANT OPTIMIZE ON core.people TO "${CH_INGEST_USER}";

-- (Optional for manual verification under ingest)
GRANT SELECT ON raw.astros_raw TO "${CH_INGEST_USER}";
GRANT SELECT ON core.people TO "${CH_INGEST_USER}";

-- Create dbt user
CREATE USER IF NOT EXISTS "${CH_DBT_USER}"
IDENTIFIED WITH sha256_password BY '${CH_DBT_PASSWORD}';

-- dbt reads raw/core and can fully manage analytics objects in local dev
GRANT SELECT ON raw.* TO "${CH_DBT_USER}";
GRANT SELECT ON core.* TO "${CH_DBT_USER}";
GRANT SELECT ON system.* TO "${CH_DBT_USER}";
GRANT ALL ON analytics.* TO "${CH_DBT_USER}";

SQL

echo "[initdb] Users and grants created."
