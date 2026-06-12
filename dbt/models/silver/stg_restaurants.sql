-- Silver staging: one row per restaurant_id.
-- Bronze raw_restaurants can contain duplicate restaurant_ids (re-ingested files);
-- we keep the most authoritative copy (highest votes = most reviewed = most current).

with source as (

    select * from {{ source('bronze', 'raw_restaurants') }}

),

deduplicated as (

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
        -- identifiers
        restaurant_id,
        trim(restaurant_name) as restaurant_name,

        -- standardise city: trim, fix common misspellings/variants, then Title-case the first letter
        case
            when upper(trim(city)) in ('BANGALORE', 'BENGALURU') then 'Bangalore'
            when upper(trim(city)) in ('NEW DELHI', 'DELHI') then 'Delhi'
            when upper(trim(city)) = 'BOMBAY' then 'Mumbai'
            else
                concat(
                    upper(substr(trim(city), 1, 1)),
                    lower(substr(trim(city), 2))
                )
        end as city,

        address,
        locality,
        longitude,
        latitude,

        -- cuisines: keep original list, derive the primary (first) cuisine
        cuisines as cuisine_list,
        trim(split(cuisines, ',')[safe_offset(0)]) as cuisine_primary,

        -- TRUE when the restaurant serves any Indian cuisine
        case
            when upper(coalesce(cuisines, '')) like '%NORTH INDIAN%'
              or upper(coalesce(cuisines, '')) like '%SOUTH INDIAN%'
              or upper(coalesce(cuisines, '')) like '%INDIAN%'
              or upper(coalesce(cuisines, '')) like '%BIRYANI%'
            then true
            else false
        end as cuisine_is_indian,

        average_cost_for_two,
        currency,

        -- cost per person (guarded against zero/null denominator)
        {{ safe_divide('cast(average_cost_for_two as float64)', '2.0') }} as cost_per_person,

        has_table_booking,
        has_online_delivery,
        cast(has_online_delivery as bool) as is_delivery_available,

        price_range,

        -- price tier from the Zomato price_range bucket
        case price_range
            when 1 then 'BUDGET'
            when 2 then 'AFFORDABLE'
            when 3 then 'PREMIUM'
            when 4 then 'LUXURY'
            else null
        end as price_tier,

        -- aggregate_rating: bronze stores it as FLOAT64 already, but guard the
        -- non-numeric Zomato sentinels ("NEW", "-") by nulling out-of-range values
        case
            when aggregate_rating between 0 and 5 then cast(aggregate_rating as float64)
            else null
        end as aggregate_rating,

        rating_text,
        votes,
        country_code,

        _ingested_at,
        _source_file

    from deduplicated
    where _row_num = 1

)

select * from renamed_and_cast
