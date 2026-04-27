with procurement as (
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
  from {{ ref('stg_procurement_records') }}
),
clinical as (
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
  from {{ ref('stg_clinical_records') }}
)
select *
from procurement
union all
select *
from clinical
