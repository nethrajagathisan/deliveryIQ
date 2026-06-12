-- Silver staging: ratings-over-time simulation, one row per restaurant_id.
-- Zomato gives us no rating history, so we simulate SCD Type 1: each restaurant's
-- current aggregate_rating is treated as effective from valid_from with an open
-- valid_to (NULL = still current). When real history arrives this becomes Type 2.

with source as (

    select * from {{ source('bronze', 'raw_restaurants') }}

),

deduplicated as (

    -- same dedup rule as stg_restaurants: highest votes wins (most authoritative)
    select
        *,
        row_number() over (
            partition by restaurant_id
            order by votes desc, _ingested_at desc
        ) as _row_num
    from source
    where restaurant_id is not null

),

renamed_and_cast as (

    select
        restaurant_id,

        -- guard the non-numeric Zomato sentinels ("NEW", "-") and out-of-range values
        case
            when aggregate_rating between 0 and 5 then cast(aggregate_rating as float64)
            else null
        end as aggregate_rating,

        rating_text,
        votes

    from deduplicated
    where _row_num = 1

),

final as (

    select
        restaurant_id,
        aggregate_rating,
        rating_text,
        votes,

        -- rating tier bands
        case
            when aggregate_rating >= 4.5 then 'EXCELLENT'
            when aggregate_rating >= 4.0 then 'GOOD'
            when aggregate_rating >= 3.5 then 'AVERAGE'
            when aggregate_rating >= 3.0 then 'BELOW_AVERAGE'
            when aggregate_rating is not null then 'POOR'
            else null
        end as rating_tier,

        -- SCD Type 1 validity window (single open version per restaurant)
        cast('{{ var("start_date") }}' as date) as valid_from,
        cast(null as date) as valid_to,
        true as is_current

    from renamed_and_cast

)

select * from final
