{{ config(materialized='view') }}

{% set src_rel = ref('fct_clinical_records') %}
{% set src_cols = filelens_column_names(src_rel) %}

with src as (
  select *
  from {{ src_rel }}
  where source_kind = 'naaccr'
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
  canonical_patient_id as patient_id,
  payload_json_safe->>'patient_namefirst' as patient_first_name,
  payload_json_safe->>'patient_namelast' as patient_last_name,
  case
    when coalesce(canonical_tumor_record_number, payload_json_safe->>'tumor_tumorrecordnumber', '') ~ '^-?[0-9]+$'
      then coalesce(canonical_tumor_record_number, payload_json_safe->>'tumor_tumorrecordnumber')::bigint
    else null
  end as tumor_record_number,
  case
    when coalesce(payload_json_safe->>'tumor_grade', '') ~ '^-?[0-9]+$'
      then (payload_json_safe->>'tumor_grade')::integer
    else null
  end as tumor_grade,
  case
    when coalesce(payload_json_safe->>'patient_sex', '') ~ '^-?[0-9]+$'
      then (payload_json_safe->>'patient_sex')::integer
    else null
  end as patient_sex_code,
  payload_json_safe->>'naaccr_recordtype' as naaccr_record_type
from with_payload
