{% macro remove_empty_string(column_name) %}
    case replace({{ column_name }}, '''', '')
            when '' then null
            else replace({{ column_name }}, '''', '')
        end
{% endmacro %}
