with orders as (

    select * from {{ ref('stg_coffee_shop__orders') }}

) 


, order_items as (
    
    select * from {{ ref('stg_coffee_shop__order_items') }}

)

, product_prices as (

    select * from {{ ref('stg_coffee_shop__product_prices') }}

)

, final as (
    select
        orders.*
      , order_items.product_id
      , product_prices.price
    from orders
    left join order_items
      on orders.order_id = order_items.order_id
    left join product_prices
      on order_items.product_id = product_prices.product_id
      and orders.created_at >= product_prices.created_at
      and orders.created_at < product_prices.ended_at
)

select * from final