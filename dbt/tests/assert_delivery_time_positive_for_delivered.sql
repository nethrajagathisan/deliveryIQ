-- Singular test: delivered orders must have a positive delivery_time_mins.
-- Returns offending rows; the test passes when zero rows are returned.

select
    order_id,
    delivery_time_mins
from {{ ref('stg_orders') }}
where is_delivered
  and (delivery_time_mins is null or delivery_time_mins <= 0)
