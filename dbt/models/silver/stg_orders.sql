-- Silver staging: one row per order_id.
-- Outlier rows are rejected here: order_amount_inr <= 0 (invalid/refund artefacts)
-- and order_amount_inr > 10000 (implausible single-order value for food delivery).
-- These rejected rows are filtered out in the final WHERE and never reach Silver.

with source as (

    -- raw_orders is partitioned by order_date with require_partition_filter=true,
    -- so every read MUST constrain order_date. Lower-bound at the project start_date
    -- (open upper bound so recent orders are not excluded) to satisfy partition elimination.
    select * from {{ source('bronze', 'raw_orders') }}
    where order_date >= date('{{ var("start_date") }}')

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id
            order by _generated_at desc
        ) as _row_num
    from source
    where order_id is not null

),

renamed_and_cast as (

    select
        -- identifiers
        order_id,
        restaurant_id,
        customer_id,
        city,

        -- temporal casts
        cast(order_date as date) as order_date,
        cast(order_datetime as timestamp) as order_datetime,

        -- peak-hour / seasonality helpers
        extract(hour from order_datetime) as order_hour,

        -- BigQuery DAYOFWEEK: 1=Sunday..7=Saturday. Remap to 0=Monday..6=Sunday.
        mod(extract(dayofweek from order_datetime) + 5, 7) as order_day_of_week,
        case
            when mod(extract(dayofweek from order_datetime) + 5, 7) >= 5 then true
            else false
        end as is_weekend,

        order_amount_inr,
        payment_method,
        platform,

        -- normalise status, plus boolean flags
        upper(order_status) as order_status,
        case when upper(order_status) = 'CANCELLED' then true else false end as is_cancelled,
        case when upper(order_status) = 'DELIVERED' then true else false end as is_delivered,
        case when upper(order_status) = 'REFUND_REQUESTED' then true else false end as is_refund_requested,

        delivery_time_mins,
        customer_rating,
        cuisine_category,

        _generated_at

    from deduplicated
    where _row_num = 1

)

select * from renamed_and_cast
-- reject outliers: keep only plausible order amounts (0 < amount <= 10000 INR)
where order_amount_inr > 0
  and order_amount_inr <= 10000
