with products as (
    select * from {{ source('coffee_shop', 'products') }}
)

select
    id as product_id
  , name
  , category
  , created_at
from products