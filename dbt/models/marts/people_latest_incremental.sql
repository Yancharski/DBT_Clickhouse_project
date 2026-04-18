{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(_inserted_at)',
    order_by=['craft', 'name']
) }}

select
    craft,
    name,
    _inserted_at
from {{ source('core', 'people') }}
{% if is_incremental() %}
where _inserted_at > (
    select coalesce(
        max(_inserted_at),
        toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')
    )
    from {{ this }}
)
{% endif %}
