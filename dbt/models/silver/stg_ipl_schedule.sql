-- Silver staging: one row per (match_date, venue_city, team1, team2).
-- Re-ingested schedule files can duplicate fixtures; keep the most recently ingested copy.

with source as (

    select * from {{ source('bronze', 'raw_ipl_schedule') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_date, venue_city, team1, team2
            order by _ingested_at desc
        ) as _row_num
    from source
    where match_date is not null
      and venue_city is not null

),

renamed_and_cast as (

    select
        cast(match_date as date) as match_date,
        cast(season as int64) as season,
        cast(match_number as int64) as match_number,
        team1,
        team2,

        -- standardise venue_city to match stg_restaurants convention
        case
            when upper(trim(venue_city)) in ('BANGALORE', 'BENGALURU') then 'Bangalore'
            when upper(trim(venue_city)) in ('NEW DELHI', 'DELHI') then 'Delhi'
            when upper(trim(venue_city)) = 'BOMBAY' then 'Mumbai'
            else
                concat(
                    upper(substr(trim(venue_city), 1, 1)),
                    lower(substr(trim(venue_city), 2))
                )
        end as venue_city,

        lower(trim(match_type)) as match_type,
        winner,
        cast(is_deliveryiq_city as bool) as is_deliveryiq_city,

        -- human-readable fixture label
        concat(team1, ' vs ', team2) as match_label,

        -- knockout games drive bigger order spikes than league fixtures
        case
            when lower(trim(match_type)) in ('qualifier', 'eliminator', 'final') then true
            else false
        end as is_knockout,

        _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from renamed_and_cast
