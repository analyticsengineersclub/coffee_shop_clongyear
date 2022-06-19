with order_items as (
    select * from {{ source('coffee_shop', 'order_items') }}
)

select * from order_items