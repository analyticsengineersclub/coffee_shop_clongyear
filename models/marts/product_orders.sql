{{ config(materialized='table') }}

with order_prices as (

    select * from {{ ref('int_order_prices') }}

)

, products as (

    select * from {{ ref('stg_coffee_shop__products') }}

)

, final as (
    select
        order_prices.*
      , products.name as product_name
      , products.category as product_category
    from order_prices
    left join products
      on order_prices.product_id = products.product_id
)

select * from final