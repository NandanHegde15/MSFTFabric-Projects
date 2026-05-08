{% macro to_datetime2(column_name) %}
    TRY_CAST({{ column_name }} AS DATETIME2(0))
{% endmacro %}