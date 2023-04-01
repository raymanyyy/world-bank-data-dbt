{{
    config(
        materialized = 'incremental', 
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "loaded_at",
            "data_type": "timestamp",
            "granularity":"day",
        }
    )
}}

SELECT id
    ,   cast(loaded_at as timestamp) as loaded_at
FROM {{ ref('clicks') }}
WHERE True

{%if is_incremental()%}
    and date(loaded_at) >= date_sub(date(_dbt_max_partition), interval 1 day) --(select max(loaded_at) from {{this}} )
    
{%endif%}