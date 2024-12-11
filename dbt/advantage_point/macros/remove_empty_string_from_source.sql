{% macro remove_empty_string_from_source(column_name) %}
    case replace({{ column_name }}, '''', '')
            when '' then null
            else replace({{ column_name }}, '''', '')
        end
{% endmacro %}
