{% macro get_latest_record(
    model_ref,
    partition_by_col,
    order_by_col
) %}

    select
        {{ dbt_utils.star(from=model_ref, except=["row_num"]) }}
    from (
        select
            *,
            row_number() over (
                partition by {{ partition_by_col }}
                order by {{ order_by_col }} desc
            ) as row_num
        from {{ model_ref }}
    ) as ranked
    where row_num = 1

{% end macro %}