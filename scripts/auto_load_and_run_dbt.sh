#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PARQUET_GLOB="$ROOT_DIR/output/**/*.parquet"
RUN_TESTS=1
FULL_REFRESH=0
LOAD_MODE="replace"

usage() {
  cat <<'USAGE'
Usage: scripts/auto_load_and_run_dbt.sh [options]

Options:
  --parquet-glob <glob>  Parquet glob to load into raw.filelens_lines
                         (default: output/**/*.parquet)
  --append               Append to raw.filelens_lines instead of replacing it
  --full-refresh         Run dbt models with --full-refresh
  --no-tests             Skip dbt test step
  -h, --help             Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --parquet-glob)
      PARQUET_GLOB="${2:-}"
      shift 2
      ;;
    --full-refresh)
      FULL_REFRESH=1
      shift
      ;;
    --append)
      LOAD_MODE="append"
      shift
      ;;
    --no-tests)
      RUN_TESTS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

for cmd in duckdb dbt; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

CONN="host=$(sql_escape "$PGHOST") port=$(sql_escape "$PGPORT") dbname=$(sql_escape "$PGDATABASE") user=$(sql_escape "$PGUSER")"
if [[ -n "${PGPASSWORD:-}" ]]; then
  CONN="$CONN password=$(sql_escape "$PGPASSWORD")"
fi

PARQUET_GLOB_SQL="$(sql_escape "$PARQUET_GLOB")"

if ! duckdb -c "select 1 from read_parquet('$PARQUET_GLOB_SQL', union_by_name=true) limit 1;" >/dev/null 2>&1; then
  echo "No readable parquet files matched: $PARQUET_GLOB" >&2
  exit 1
fi

POSTGRES_EXT_SQL="LOAD postgres;"
if ! duckdb -c "$POSTGRES_EXT_SQL SELECT 1;" >/dev/null 2>&1; then
  POSTGRES_EXT_SQL="INSTALL postgres; LOAD postgres;"
fi

if [[ "$LOAD_MODE" == "append" ]]; then
  LOAD_SQL="
CREATE TABLE IF NOT EXISTS pg.raw.filelens_lines AS
SELECT
  t.*,
  now()::timestamp AS _ingested_at,
  filename AS _source_file,
  NULL::varchar AS _file_hash
FROM read_parquet('$PARQUET_GLOB_SQL', union_by_name=true, filename=true) t
WHERE 1 = 0;

INSERT INTO pg.raw.filelens_lines
SELECT
  t.*,
  now()::timestamp AS _ingested_at,
  filename AS _source_file,
  NULL::varchar AS _file_hash
FROM read_parquet('$PARQUET_GLOB_SQL', union_by_name=true, filename=true) t;
"
else
  LOAD_SQL="
CREATE OR REPLACE TABLE pg.raw.filelens_lines AS
SELECT
  t.*,
  now()::timestamp AS _ingested_at,
  filename AS _source_file,
  NULL::varchar AS _file_hash
FROM read_parquet('$PARQUET_GLOB_SQL', union_by_name=true, filename=true) t;
"
fi

echo "Loading parquet into raw.filelens_lines from: $PARQUET_GLOB (mode: $LOAD_MODE)"
duckdb -c "
$POSTGRES_EXT_SQL
ATTACH '$CONN' AS pg (TYPE postgres);
CREATE SCHEMA IF NOT EXISTS pg.raw;
$LOAD_SQL
"

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT_DIR/dbt}"
DBT_RUN_FLAGS=()
if [[ "$FULL_REFRESH" -eq 1 ]]; then
  DBT_RUN_FLAGS+=(--full-refresh)
fi

echo "Running dbt staging models"
DBT_PROFILES_DIR="$DBT_PROFILES_DIR" dbt run "${DBT_RUN_FLAGS[@]}" --select stg_procurement_records stg_clinical_records stg_filelens_records

echo "Running dbt mart models"
DBT_PROFILES_DIR="$DBT_PROFILES_DIR" dbt run "${DBT_RUN_FLAGS[@]}" --select \
  fct_procurement_records \
  fct_clinical_records \
  idx_filelens_records \
  fct_record_attributes \
  fct_procurement_lines \
  fct_fhir_resources \
  fct_naaccr_cases

if [[ "$RUN_TESTS" -eq 1 ]]; then
  echo "Running dbt tests"
  DBT_PROFILES_DIR="$DBT_PROFILES_DIR" dbt test --select \
    fct_procurement_records \
    fct_clinical_records \
    idx_filelens_records \
    fct_record_attributes \
    fct_procurement_lines \
    fct_fhir_resources \
    fct_naaccr_cases
fi

if command -v psql >/dev/null 2>&1; then
  echo "Row counts:"
  psql -qtAX -c "select 'analytics_marts.fct_procurement_lines=' || count(*) from analytics_marts.fct_procurement_lines;" || true
  psql -qtAX -c "select 'analytics_marts.fct_fhir_resources=' || count(*) from analytics_marts.fct_fhir_resources;" || true
  psql -qtAX -c "select 'analytics_marts.fct_naaccr_cases=' || count(*) from analytics_marts.fct_naaccr_cases;" || true
  psql -qtAX -c "select 'analytics_marts.fct_record_attributes=' || count(*) from analytics_marts.fct_record_attributes;" || true
fi

cat <<'NEXT'
Done.

Next queries:
  select * from analytics_marts.fct_procurement_lines limit 20;
  select * from analytics_marts.fct_fhir_resources limit 20;
  select * from analytics_marts.fct_naaccr_cases limit 20;
  select * from analytics_marts.fct_record_attributes limit 20;
NEXT
