with product_prices as (
    select * from {{ source('coffee_shop', 'product_prices') }}
)

select
    id as price_id
  , product_id
  , price
  , created_at
  , ended_at
from product_prices