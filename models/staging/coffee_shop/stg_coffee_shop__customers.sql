with customers as (
    select * from {{ source('coffee_shop', 'customers') }}
)

select 
    id as customer_id
  , name
  , email
from customers