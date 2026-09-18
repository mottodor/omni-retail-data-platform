{# Layers are explicit schemas (silver/gold/analytics): use the model's
   custom schema verbatim instead of the default `<target>_<schema>` prefix. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
