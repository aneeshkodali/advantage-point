{% macro remove_empty_string_from_source(column_name) %}
    case
        when {{ column_name }} = '''""''' then null
        when {{ column_name }} = '''''' then null
        else replace({{ column_name }}, '''', '')
    end
{% endmacro %}
