#!/usr/bin/env bash
set -euo pipefail


echo "[initdb] Creating users and grants..."

clickhouse-client \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --multiquery <<SQL

-- Create ingest user
CREATE USER IF NOT EXISTS ${CH_INGEST_USER:Identifier}
IDENTIFIED WITH sha256_password BY '${CH_INGEST_PASSWORD}';

-- Minimal rights for ingestion
GRANT INSERT ON raw.astros_raw TO ${CH_INGEST_USER:Identifier};
GRANT OPTIMIZE ON core.people TO ${CH_INGEST_USER:Identifier};

-- (Optional for manual verification under ingest)
GRANT SELECT ON raw.astros_raw TO ${CH_INGEST_USER:Identifier};
GRANT SELECT ON core.people TO ${CH_INGEST_USER:Identifier};

-- Create dbt user
CREATE USER IF NOT EXISTS ${CH_DBT_USER:Identifier}
IDENTIFIED WITH sha256_password BY '${CH_DBT_PASSWORD}';

-- dbt reads raw/core
GRANT SELECT ON raw.* TO ${CH_DBT_USER:Identifier};
GRANT SELECT ON core.* TO ${CH_DBT_USER:Identifier};

-- dbt creates models in analytics
GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, INSERT ON analytics.* TO ${CH_DBT_USER:Identifier};

SQL

echo "[initdb] Users and grants created."
