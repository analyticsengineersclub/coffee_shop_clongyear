with order_items as (
    select * from {{ source('coffee_shop', 'order_items') }}
)

select
    id as orderitem_id
  , order_id
  , product_id
from order_items