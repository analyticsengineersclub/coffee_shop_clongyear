with products as (
    select * from {{ source('coffee_shop', 'products') }}
)

select * from products