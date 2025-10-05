-- Create schema,stage and push config file

{% macro push_configs(release_notes_file,regression_config_file) %}

  {% set process_schema = "VALIDATION_REGRESSION" %}
  {% set stage_name = "CONFIGS" %}  

  {% set put_command %}
    
    CREATE OR REPLACE SCHEMA {{ process_schema }};
    CREATE OR REPLACE STAGE {{ process_schema }}.{{ stage_name }} FILE_FORMAT = (TYPE = 'JSON'); 
    PUT 'file://{{ regression_config_file }}' @{{ process_schema }}.{{ stage_name }} AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
    PUT 'file://{{ release_notes_file }}' @{{ process_schema }}.{{ stage_name }} AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
    
  {% endset %}

  {% do run_query(put_command) %}

{% endmacro %}