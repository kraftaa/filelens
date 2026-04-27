{{ config(
  pre_hook=[
    "create schema if not exists raw_clinical",
    "create table if not exists raw_clinical.filelens_lines (_ingested_at timestamp, _source_file text, _file_hash text)"
  ]
) }}

{% set src_rel = source('raw_clinical', 'filelens_lines') %}
{% set src_cols = filelens_column_names(src_rel) %}

select
  'clinical'::text as source_family,
  {{ filelens_col_or_null(src_cols, '_ingested_at', 'timestamp') }} as ingest_ts,
  {{ filelens_col_or_null(src_cols, '_source_file', 'text') }} as source_file,
  {{ filelens_col_or_null(src_cols, '_file_hash', 'text') }} as file_hash,
  {{ filelens_first_non_null(src_cols, ['payload_id', 'resource_id', 'document_id', 'message_control_id'], 'text') }} as canonical_payload_id,
  {{ filelens_first_non_null(src_cols, ['order_id', 'placer_order_number', 'filler_order_number', 'encounter_id', 'subject'], 'text') }} as canonical_order_id,
  {{ filelens_first_non_null(src_cols, ['line_number', 'obx_set_id'], 'text') }} as canonical_line_number,
  {{ filelens_first_non_null(src_cols, ['quantity', 'value_quantity_value'], 'numeric') }} as canonical_quantity,
  {{ filelens_col_or_null(src_cols, 'unit_price', 'numeric') }} as canonical_unit_price,
  {{ filelens_first_non_null(src_cols, ['line_total'], 'numeric') }} as canonical_line_total,
  {{ filelens_col_or_null(src_cols, 'currency', 'text') }} as canonical_currency,
  {{ filelens_first_non_null(src_cols, ['description', 'code_display', 'observation_display', 'obx_text', 'universal_service_text', 'object'], 'text') }} as canonical_description,
  {{ filelens_first_non_null(src_cols, ['unit_of_measure', 'value_quantity_unit', 'observation_unit', 'obx_units'], 'text') }} as canonical_unit_of_measure,
  {{ filelens_first_non_null(src_cols, ['patient_patientidnumber', 'patient_id', 'patient_reference', 'subject_reference'], 'text') }} as canonical_patient_id,
  {{ filelens_col_or_null(src_cols, 'tumor_tumorrecordnumber', 'text') }} as canonical_tumor_record_number,
  {{ filelens_col_or_null(src_cols, 'invoice_purpose', 'text') }} as canonical_invoice_purpose,
  {{ filelens_col_or_null(src_cols, 'notice_id', 'text') }} as canonical_notice_id,
  {{ filelens_col_or_null(src_cols, 'quote_id', 'text') }} as canonical_quote_id,
  (to_jsonb(src) - '_ingested_at' - '_source_file' - '_file_hash') as payload_json,

  case
    when {{ filelens_col_or_null(src_cols, 'patient_patientidnumber', 'text') }} is not null then 'naaccr'
    when {{ filelens_col_or_null(src_cols, 'tumor_tumorrecordnumber', 'text') }} is not null then 'naaccr'
    when {{ filelens_col_or_null(src_cols, 'resource_type', 'text') }} is not null then 'fhir'
    when {{ filelens_col_or_null(src_cols, 'message_control_id', 'text') }} is not null then 'hl7'
    when {{ filelens_col_or_null(src_cols, 'document_id', 'text') }} is not null then 'cda'
    when {{ filelens_col_or_null(src_cols, 'predicate', 'text') }} is not null then 'rdf'
    else 'clinical_generic'
  end as source_kind,

  md5(
    'clinical|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'patient_patientidnumber', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'tumor_tumorrecordnumber', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'payload_id', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'order_id', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'line_number', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, '_source_file', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, '_file_hash', 'text') }}, '')
  ) as record_key
from {{ src_rel }} as src
