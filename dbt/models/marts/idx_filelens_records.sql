{{ config(
    materialized='incremental',
    schema='registry',
    unique_key='record_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select *
  from {{ ref('stg_filelens_records') }}
),
deduped as (
  select
    source_family,
    ingest_ts,
    source_file,
    file_hash,
    canonical_payload_id,
    canonical_order_id,
    canonical_line_number,
    canonical_quantity,
    canonical_unit_price,
    canonical_line_total,
    canonical_currency,
    canonical_description,
    canonical_unit_of_measure,
    canonical_patient_id,
    canonical_tumor_record_number,
    canonical_invoice_purpose,
    canonical_notice_id,
    canonical_quote_id,
    payload_json,
    source_kind,
    record_key
  from (
    select
      *,
      row_number() over (
        partition by record_key
        order by coalesce(ingest_ts, '1900-01-01'::timestamp) desc, source_file desc
      ) as _rn
    from src
  ) ranked
  where _rn = 1
),
bounded as (
  select *
  from deduped
  {% if is_incremental() %}
  where coalesce(ingest_ts, '1900-01-01'::timestamp)
      >= (
        select coalesce(max(ingest_ts), '1900-01-01'::timestamp)
        from {{ this }}
      )
  {% endif %}
)
select * from bounded
