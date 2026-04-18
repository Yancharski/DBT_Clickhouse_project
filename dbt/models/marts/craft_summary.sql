{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['craft_family', 'craft']
) }}

with latest_people as (
    select
        craft,
        name,
        _inserted_at
    from {{ ref('people_latest_incremental') }} final
),
classified as (
    select
        craft,
        name,
        _inserted_at,
        case
            when lowerUTF8(craft) = 'iss' then 'iss'
            when lowerUTF8(craft) = 'tiangong' then 'tiangong'
            else 'other'
        end as craft_family
    from latest_people
)
select
    craft_family,
    craft,
    count() as people_count,
    max(_inserted_at) as latest_loaded_at
from classified
group by
    craft_family,
    craft
order by
    craft_family,
    craft
