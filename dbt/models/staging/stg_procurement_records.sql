{{ config(
  pre_hook=[
    "create schema if not exists raw_procurement",
    "create table if not exists raw_procurement.filelens_lines (_ingested_at timestamp, _source_file text, _file_hash text)"
  ]
) }}

{% set src_rel = source('raw_procurement', 'filelens_lines') %}
{% set src_cols = filelens_column_names(src_rel) %}

select
  'procurement'::text as source_family,
  {{ filelens_col_or_null(src_cols, '_ingested_at', 'timestamp') }} as ingest_ts,
  {{ filelens_col_or_null(src_cols, '_source_file', 'text') }} as source_file,
  {{ filelens_col_or_null(src_cols, '_file_hash', 'text') }} as file_hash,
  {{ filelens_col_or_null(src_cols, 'payload_id', 'text') }} as canonical_payload_id,
  {{ filelens_col_or_null(src_cols, 'order_id', 'text') }} as canonical_order_id,
  {{ filelens_col_or_null(src_cols, 'line_number', 'text') }} as canonical_line_number,
  {{ filelens_col_or_null(src_cols, 'quantity', 'numeric') }} as canonical_quantity,
  {{ filelens_col_or_null(src_cols, 'unit_price', 'numeric') }} as canonical_unit_price,
  {{ filelens_col_or_null(src_cols, 'line_total', 'numeric') }} as canonical_line_total,
  {{ filelens_col_or_null(src_cols, 'currency', 'text') }} as canonical_currency,
  {{ filelens_col_or_null(src_cols, 'description', 'text') }} as canonical_description,
  {{ filelens_col_or_null(src_cols, 'unit_of_measure', 'text') }} as canonical_unit_of_measure,
  {{ filelens_col_or_null(src_cols, 'patient_patientidnumber', 'text') }} as canonical_patient_id,
  {{ filelens_col_or_null(src_cols, 'tumor_tumorrecordnumber', 'text') }} as canonical_tumor_record_number,
  {{ filelens_col_or_null(src_cols, 'invoice_purpose', 'text') }} as canonical_invoice_purpose,
  {{ filelens_col_or_null(src_cols, 'notice_id', 'text') }} as canonical_notice_id,
  {{ filelens_col_or_null(src_cols, 'quote_id', 'text') }} as canonical_quote_id,
  (to_jsonb(src) - '_ingested_at' - '_source_file' - '_file_hash') as payload_json,

  case
    when {{ filelens_col_or_null(src_cols, 'invoice_purpose', 'text') }} is not null then 'invoice'
    when {{ filelens_col_or_null(src_cols, 'notice_id', 'text') }} is not null then 'ship_notice'
    when {{ filelens_col_or_null(src_cols, 'quote_id', 'text') }} is not null then 'quote'
    when {{ filelens_col_or_null(src_cols, 'order_id', 'text') }} is not null then 'order'
    else 'procurement_generic'
  end as source_kind,

  md5(
    'procurement|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'payload_id', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'order_id', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, 'line_number', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, '_source_file', 'text') }}, '') || '|' ||
    coalesce({{ filelens_col_or_null(src_cols, '_file_hash', 'text') }}, '')
  ) as record_key
from {{ src_rel }} as src
