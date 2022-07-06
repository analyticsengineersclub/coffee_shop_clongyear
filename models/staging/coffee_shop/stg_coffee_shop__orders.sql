with orders as (
    select * from {{ source('coffee_shop', 'orders') }}
)

select
    id as order_id
  , created_at
  , customer_id
  , total
  , address
  , state
  , zip
from orders