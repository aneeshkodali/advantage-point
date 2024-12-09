{% macro convert_rank_to_integer(column_name) %}
    case 
        -- check if the cleaned column contains only numeric characters
        when regexp_replace(trim({{ column_name }}), '["'']', '', 'g') ~ '^\d+$' 
        then cast(regexp_replace(trim({{ column_name }}), '["'']', '', 'g') as integer)
        else null
    end
{% endmacro %}
