with product_prices as (
    select * from {{ source('coffee_shop', 'product_prices') }}
)

select
    id as price_id
  , product_id
  , created_at
  , ended_at
  , price
from product_prices