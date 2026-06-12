-- Silver staging: one row per (festival_date, festival_name).
-- A single date can carry multiple festivals (regional + national overlap) — that is
-- expected and preserved; downstream EXISTS-based macros handle the many-per-date case.
-- We only dedup exact (date, name) repeats from re-ingested calendar files.

with source as (

    select * from {{ source('bronze', 'raw_festival_calendar') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by festival_date, festival_name
            order by _ingested_at desc
        ) as _row_num
    from source
    where festival_date is not null
      and festival_name is not null

),

renamed_and_cast as (

    select
        cast(festival_date as date) as festival_date,
        festival_name,

        upper(trim(festival_type)) as festival_type,
        upper(trim(region)) as region,
        cast(is_major as bool) as is_major,

        -- grouping helpers
        extract(year from festival_date) as festival_year,
        extract(month from festival_date) as festival_month,

        _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from renamed_and_cast
