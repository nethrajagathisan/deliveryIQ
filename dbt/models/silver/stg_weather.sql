-- Silver staging: one row per (city, weather_date).
-- Bronze raw_weather may re-fetch the same day; we keep the most recently fetched copy.

with source as (

    select * from {{ source('bronze', 'raw_weather') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by city, weather_date
            order by _fetched_at desc
        ) as _row_num
    from source
    where city is not null
      and weather_date is not null

),

renamed_and_cast as (

    select
        -- standardise city to match stg_restaurants convention
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

        cast(weather_date as date) as weather_date,

        -- explicit numeric casts
        cast(temperature_max as float64) as temperature_max,
        cast(temperature_min as float64) as temperature_min,
        cast(precipitation_sum as float64) as precipitation_sum,
        cast(windspeed_max as float64) as windspeed_max,
        cast(weather_code as int64) as weather_code,

        weather_description,
        cast(is_rainy as bool) as is_rainy,

        -- derived weather classification (heavy_rain / light_rain / extreme_heat / hot / normal)
        {{ classify_weather('precipitation_sum', 'temperature_max') }} as weather_category,

        -- temperature spread for the day
        cast(temperature_max as float64) - cast(temperature_min as float64) as temperature_range,

        -- Indian monsoon months: Jun–Oct
        case
            when extract(month from weather_date) in (6, 7, 8, 9, 10) then true
            else false
        end as monsoon_month,

        -- extreme weather = heavy rain OR extreme heat (uses the same classify_weather thresholds)
        case
            when {{ classify_weather('precipitation_sum', 'temperature_max') }} in ('heavy_rain', 'extreme_heat')
            then true
            else false
        end as is_extreme_weather,

        -- Indian meteorological seasons (not the Western 4)
        case
            when extract(month from weather_date) in (12, 1, 2) then 'WINTER'
            when extract(month from weather_date) in (3, 4, 5) then 'SPRING'
            when extract(month from weather_date) in (6, 7, 8, 9) then 'MONSOON'
            when extract(month from weather_date) in (10, 11) then 'POST_MONSOON'
        end as season,

        _fetched_at

    from deduplicated
    where _row_num = 1

)

select * from renamed_and_cast
