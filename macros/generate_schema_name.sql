{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {%- if target.name == 'regression' %}        
            {{ custom_schema_name | trim }}_regression        
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}    
    {%- endif -%}
{%- endmacro %}