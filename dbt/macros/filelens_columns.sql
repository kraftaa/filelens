{% macro filelens_column_names(relation) -%}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% set names = [] %}
  {% for col in cols %}
    {% do names.append(col.name | lower) %}
  {% endfor %}
  {{ return(names) }}
{%- endmacro %}

{% macro filelens_col_or_null(columns, name, cast_type='text') -%}
  {% if name | lower in columns %}
    cast({{ adapter.quote(name) }} as {{ cast_type }})
  {% else %}
    cast(null as {{ cast_type }})
  {% endif %}
{%- endmacro %}

{% macro filelens_first_non_null(columns, names, cast_type='text') -%}
  {% set present = [] %}
  {% for name in names %}
    {% if name | lower in columns %}
      {% do present.append("cast(" ~ adapter.quote(name) ~ " as " ~ cast_type ~ ")") %}
    {% endif %}
  {% endfor %}

  {% if present | length > 0 %}
    coalesce({{ present | join(', ') }})
  {% else %}
    cast(null as {{ cast_type }})
  {% endif %}
{%- endmacro %}
