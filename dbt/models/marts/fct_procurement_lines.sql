{{ config(materialized='view') }}

{% set src_rel = ref('fct_procurement_records') %}
{% set src_cols = filelens_column_names(src_rel) %}

with src as (
  select *
  from {{ src_rel }}
),
with_payload as (
  select
    src.*,
    {% if 'payload_json' in src_cols %}
    src.payload_json
    {% else %}
    to_jsonb(src)
    {% endif %} as payload_json_safe
  from src
)
select
  record_key,
  ingest_ts,
  source_file,
  source_kind,
  canonical_order_id as order_id,
  canonical_line_number as line_number,
  coalesce(payload_json_safe->>'supplier_part_id', payload_json_safe->>'supplier_partid') as supplier_part_id,
  canonical_description as description,
  canonical_quantity as quantity,
  canonical_unit_price as unit_price,
  canonical_line_total as line_total,
  canonical_currency as currency,
  payload_json_safe->>'ship_to_name' as ship_to_name,
  payload_json_safe->>'bill_to_name' as bill_to_name
from with_payload
