with customers as (
    select * from {{ source('coffee_shop', 'customers') }}
)

select * from customers