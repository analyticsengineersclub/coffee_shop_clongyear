with product_prices as (
    select * from {{ source('coffee_shop', 'product_prices') }}
)

select * from product_prices