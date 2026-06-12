-- Singular test: aggregate_rating must be between 0 and 5 (NULL allowed).
-- Returns offending rows; the test passes when zero rows are returned.

select
    restaurant_id,
    aggregate_rating
from {{ ref('stg_restaurants') }}
where aggregate_rating is not null
  and aggregate_rating not between 0 and 5
