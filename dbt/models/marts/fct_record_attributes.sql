{{ config(materialized='view') }}

with base as (
  select
    record_key,
    source_family,
    source_kind,
    ingest_ts,
    source_file,
    payload_json
  from {{ ref('idx_filelens_records') }}
),
kv as (
  select
    b.record_key,
    b.source_family,
    b.source_kind,
    b.ingest_ts,
    b.source_file,
    e.key as attribute_name,
    e.value as attribute_value
  from base b
  cross join lateral jsonb_each_text(b.payload_json) as e(key, value)
)
select
  record_key,
  source_family,
  source_kind,
  ingest_ts,
  source_file,
  case
    when source_kind = 'naaccr' and attribute_name like 'patient_%' then 'patient'
    when source_kind = 'naaccr' and attribute_name like 'tumor_%' then 'tumor'
    when source_kind = 'naaccr' and attribute_name like 'naaccr_%' then 'naaccr_meta'
    else 'record'
  end as attribute_scope,
  case
    when source_kind = 'naaccr' and attribute_name like 'patient_%' then substr(attribute_name, 9)
    when source_kind = 'naaccr' and attribute_name like 'tumor_%' then substr(attribute_name, 7)
    when source_kind = 'naaccr' and attribute_name like 'naaccr_%' then substr(attribute_name, 8)
    else attribute_name
  end as attribute_source_id,
  attribute_name,
  nullif(nullif(attribute_value, ''), 'null') as attribute_value
from kv
where attribute_value is not null
  and attribute_value <> ''
  and lower(attribute_value) <> 'null'
