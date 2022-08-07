with orders as (
    select * from {{ source('coffee_shop', 'orders') }}
)

select
    id as order_id
  , customer_id
  , address
  , state
  , zip
  , total
  , created_at
from orders