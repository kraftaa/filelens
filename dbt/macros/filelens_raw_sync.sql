{% macro filelens_prepare_split_table(raw_relation, schema_name) %}
  {% set target_relation = adapter.get_relation(database=target.database, schema=schema_name, identifier='filelens_lines') %}

  {% if target_relation is none %}
    {% do run_query("create table " ~ schema_name ~ ".filelens_lines (like raw.filelens_lines including all)") %}
    {% set target_relation = adapter.get_relation(database=target.database, schema=schema_name, identifier='filelens_lines') %}
  {% elif target_relation.type == 'view' %}
    {% do run_query("drop view if exists " ~ schema_name ~ ".filelens_lines cascade") %}
    {% do run_query("create table " ~ schema_name ~ ".filelens_lines (like raw.filelens_lines including all)") %}
    {% set target_relation = adapter.get_relation(database=target.database, schema=schema_name, identifier='filelens_lines') %}
  {% endif %}

  {% if target_relation is not none %}
    {% set raw_cols = adapter.get_columns_in_relation(raw_relation) %}
    {% set target_cols = adapter.get_columns_in_relation(target_relation) %}
    {% set target_names = [] %}
    {% for col in target_cols %}
      {% do target_names.append(col.name | lower) %}
    {% endfor %}

    {% for col in raw_cols %}
      {% if col.name | lower not in target_names %}
        {% do run_query("alter table " ~ schema_name ~ ".filelens_lines add column " ~ adapter.quote(col.name) ~ " " ~ col.data_type) %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return('') }}
{% endmacro %}

{% macro filelens_sync_split_raw_tables() %}
  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% do run_query("create schema if not exists raw_procurement") %}
  {% do run_query("create schema if not exists raw_clinical") %}

  {% set raw_relation = adapter.get_relation(database=target.database, schema='raw', identifier='filelens_lines') %}

  {% if raw_relation is none %}
    {% do run_query("create table if not exists raw_procurement.filelens_lines (_ingested_at timestamp, _source_file text, _file_hash text)") %}
    {% do run_query("create table if not exists raw_clinical.filelens_lines (_ingested_at timestamp, _source_file text, _file_hash text)") %}
    {{ return('') }}
  {% endif %}

  {% do filelens_prepare_split_table(raw_relation, 'raw_procurement') %}
  {% do filelens_prepare_split_table(raw_relation, 'raw_clinical') %}

  {% set raw_cols = adapter.get_columns_in_relation(raw_relation) %}
  {% set quoted_cols = [] %}
  {% for col in raw_cols %}
    {% do quoted_cols.append(adapter.quote(col.name)) %}
  {% endfor %}
  {% set col_sql = quoted_cols | join(', ') %}

  {% set source_expr = "lower(coalesce(_source_file,''))" %}
  {% set procurement_filter = "("
      ~ source_expr ~ " ~ '\\.(cxml|xcml)(\\.gz)?(\\.parquet)?$'"
      ~ " or " ~ source_expr ~ " like '%/procurement/%'"
      ~ " or " ~ source_expr ~ " like '%/trade/%'"
      ~ ")" %}
  {% set clinical_filter = "("
      ~ source_expr ~ " ~ '\\.(csv|tsv|psv|txt|json|ndjson|hl7|msg|xml|ttl|rdf|html)(\\.gz)?(\\.parquet)?$'"
      ~ " or " ~ source_expr ~ " like '%naaccr%'"
      ~ " or " ~ source_expr ~ " like '%/clinical/%'"
      ~ " or " ~ source_expr ~ " like '%/fhir/%'"
      ~ " or " ~ source_expr ~ " like '%/hl7/%'"
      ~ " or " ~ source_expr ~ " like '%/json/%'"
      ~ " or " ~ source_expr ~ " like '%/xml/%'"
      ~ ")" %}

  {% do run_query("truncate table raw_procurement.filelens_lines") %}
  {% do run_query("truncate table raw_clinical.filelens_lines") %}

  {% do run_query("insert into raw_procurement.filelens_lines (" ~ col_sql ~ ") select " ~ col_sql ~ " from raw.filelens_lines where " ~ procurement_filter) %}
  {% do run_query("insert into raw_clinical.filelens_lines (" ~ col_sql ~ ") select " ~ col_sql ~ " from raw.filelens_lines where " ~ clinical_filter) %}
  {% do adapter.commit() %}

  {{ return('') }}
{% endmacro %}
