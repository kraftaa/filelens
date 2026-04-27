# filelens dbt models

This dbt project uses one primary raw landing table and auto-syncs two split tables on every run:

- `raw.filelens_lines` (primary raw landing table you load)

- `raw_procurement.filelens_lines`
- `raw_clinical.filelens_lines`

The models include:

- `analytics_internal.stg_procurement_records` (view): canonical staging over procurement raw rows
- `analytics_internal.stg_clinical_records` (view): canonical staging over clinical raw rows (NAACCR/FHIR/HL7/CDA/RDF/tabular)
- `analytics_internal.stg_filelens_records` (view): unioned canonical staging across both families
- `analytics_internal.int_procurement_records` (incremental): internal procurement normalized table
- `analytics_internal.int_clinical_records` (incremental): internal clinical normalized table
- `analytics_registry.idx_filelens_records` (incremental): combined registry/index table with `payload_json`
- `analytics_marts.fct_procurement_lines` (view): user-facing procurement analytics table
- `analytics_marts.fct_fhir_resources` (view): user-facing FHIR analytics table
- `analytics_marts.fct_naaccr_cases` (view): user-facing NAACCR analytics table
- `analytics_marts.fct_record_attributes` (view): user-facing key/value attribute table

## 1) Configure profile

Copy:

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```

Or use the checked-in local profile directly:

```bash
export DBT_PROFILES_DIR=dbt
```

Set env vars:

```bash
export PGHOST=...
export PGPORT=5432
export PGUSER=...
export PGPASSWORD=...
export PGDATABASE=...
```

## 2) Load raw rows into Postgres

Any loader is fine. Preferred path is loading all rows into:

- `raw.filelens_lines`

You can still load split tables directly if you want:

- `raw_procurement.filelens_lines` for cXML order/invoice/quote/ship notice data
- `raw_clinical.filelens_lines` for NAACCR/FHIR/HL7/CDA/RDF/clinical tabular data

Fully automated local load + dbt run:

```bash
scripts/auto_load_and_run_dbt.sh
```

To generate parquet from examples (or any input directory) first:

```bash
scripts/convert_inputs.sh --input-dir examples/public --output-dir output/public
scripts/auto_load_and_run_dbt.sh --parquet-glob "$PWD/output/public/**/*.parquet"
```

`scripts/convert_inputs.sh` is non-strict by default (skips bad files and continues). Use `--strict` to fail fast.

Useful options:

```bash
scripts/auto_load_and_run_dbt.sh --full-refresh
scripts/auto_load_and_run_dbt.sh --parquet-glob '/absolute/path/to/output/**/*.parquet'
scripts/auto_load_and_run_dbt.sh --append
scripts/auto_load_and_run_dbt.sh --no-tests
```

Recommended default flow:

- load all parquet rows into `raw.filelens_lines`
- run `dbt run ...`
- dbt `on-run-start` auto-syncs split tables from `raw.filelens_lines` using `_source_file` patterns

Recommended metadata columns:

- `_ingested_at` (`timestamp`)
- `_source_file` (`text`)
- `_file_hash` (`text`)

Bootstrap behavior:

- `stg_procurement_records` auto-creates `raw_procurement.filelens_lines` if missing.
- `stg_clinical_records` auto-creates `raw_clinical.filelens_lines` if missing.
- You still need to load actual rows for non-empty model output.
- If `raw.filelens_lines` exists, split raw tables are refreshed automatically each dbt run.
- `scripts/auto_load_and_run_dbt.sh` replaces `raw.filelens_lines` by default; use `--append` if you want additive loads.

## 3) Run dbt

From repo root (`/Users/maria/Documents/GitHub/extra/filelens`):

```bash
dbt run --select stg_procurement_records stg_clinical_records stg_filelens_records
dbt run --select fct_procurement_records fct_clinical_records idx_filelens_records fct_procurement_lines fct_fhir_resources fct_naaccr_cases fct_record_attributes
dbt test --select fct_procurement_records fct_clinical_records idx_filelens_records fct_procurement_lines fct_fhir_resources fct_naaccr_cases fct_record_attributes
```

Full refresh when needed:

```bash
dbt run --full-refresh --select fct_procurement_records fct_clinical_records idx_filelens_records fct_procurement_lines fct_fhir_resources fct_naaccr_cases fct_record_attributes
```

## Notes

- Incremental logic is based on `ingest_ts` (derived from `_ingested_at`).
- dbt `groups` are optional and not required for incremental loading.
- Split raw schemas by source family to avoid one massive sparse table when file types diverge.
- `read_parquet(...)` is a DuckDB function, not native Postgres SQL. If you use Postgres adapter, load parquet into Postgres with an external loader before running dbt.
- After migrating from the old single-raw model to split schemas, run a full refresh on `idx_filelens_records` and the user marts once.
