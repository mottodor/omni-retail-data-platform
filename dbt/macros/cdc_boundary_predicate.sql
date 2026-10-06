{% macro cdc_boundary_predicate(topic) -%}
    {%- set boundary = var('cdc_boundary', none) -%}
    {%- if boundary is not none -%}
        {%- set expected_topics = [
            'omni.oltp.public.customers',
            'omni.oltp.public.orders',
            'omni.oltp.public.payments'
        ] -%}
        {%- if boundary is not mapping -%}
            {{ exceptions.raise_compiler_error('cdc_boundary must be a mapping') }}
        {%- endif -%}
        {%- if boundary.keys() | list | sort != expected_topics | sort -%}
            {{ exceptions.raise_compiler_error(
                'cdc_boundary must contain exactly the three configured CDC topics'
            ) }}
        {%- endif -%}
        {%- if topic not in expected_topics -%}
            {{ exceptions.raise_compiler_error('unsupported CDC boundary topic: ' ~ topic) }}
        {%- endif -%}
        {%- set position = boundary[topic] -%}
        {%- if position is not mapping
            or position.keys() | list | sort != ['offset_exclusive', 'partition'] -%}
            {{ exceptions.raise_compiler_error(
                'cdc_boundary[' ~ topic ~ '] must contain exactly partition and offset_exclusive'
            ) }}
        {%- endif -%}
        {%- if position['partition'] is not integer or position['partition'] != 0 -%}
            {{ exceptions.raise_compiler_error(
                'cdc_boundary[' ~ topic ~ '].partition must be integer 0'
            ) }}
        {%- endif -%}
        {%- if position['offset_exclusive'] is not integer
            or position['offset_exclusive'] < 0 -%}
            {{ exceptions.raise_compiler_error(
                'cdc_boundary[' ~ topic ~ '].offset_exclusive must be a non-negative integer'
            ) }}
        {%- endif %}
  and kafka_partition = {{ position['partition'] }}
  and kafka_offset < {{ position['offset_exclusive'] }}
    {%- endif -%}
{%- endmacro %}
