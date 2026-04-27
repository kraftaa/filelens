{{ config(materialized='view') }}

{% set src_rel = ref('fct_clinical_records') %}
{% set src_cols = filelens_column_names(src_rel) %}

with src as (
  select *
  from {{ src_rel }}
  where source_kind = 'fhir'
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
  coalesce(payload_json_safe->>'resource_type', 'FHIRResource') as resource_type,
  coalesce(canonical_payload_id, payload_json_safe->>'resource_id') as resource_id,
  coalesce(canonical_patient_id, payload_json_safe->>'patient_id', payload_json_safe->>'subject_reference') as patient_reference,
  coalesce(canonical_description, payload_json_safe->>'code_display', payload_json_safe->>'observation_display') as concept_display,
  coalesce(payload_json_safe->>'value_quantity_value', payload_json_safe->>'observation_value') as observation_value,
  coalesce(canonical_unit_of_measure, payload_json_safe->>'value_quantity_unit', payload_json_safe->>'observation_unit') as observation_unit
from with_payload
