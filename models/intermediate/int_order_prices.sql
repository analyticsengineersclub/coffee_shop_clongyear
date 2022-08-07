with order_items as (
    
    select * from {{ ref('stg_coffee_shop__order_items') }}

)

, orders as (

    select * from {{ ref('stg_coffee_shop__orders') }}

) 

, product_prices as (

    select * from {{ ref('stg_coffee_shop__product_prices') }}

)

, final as (
    select
        order_items.*
      , product_prices.price

    from order_items
    left join orders
      on order_items.order_id = orders.order_id
    left join product_prices
      on order_items.product_id = product_prices.product_id
      and orders.created_at >= product_prices.created_at
      and orders.created_at < product_prices.ended_at
)

select * from final