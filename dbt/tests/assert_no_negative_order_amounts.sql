-- Singular test: every delivered order must have a positive order amount.
-- Returns offending rows; the test passes when zero rows are returned.

select
    order_id,
    order_amount_inr
from {{ ref('stg_orders') }}
where is_delivered
  and not (order_amount_inr > 0)
