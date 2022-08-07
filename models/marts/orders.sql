{{ config(materialized='table') }}

with order_prices as (

    select * from {{ ref('int_order_prices') }}

)

, orders as (

    select * from {{ ref('stg_coffee_shop__orders') }}

) 

, products as (

    select * from {{ ref('stg_coffee_shop__products') }}

)

, final as (
    select
        order_prices.orderitem_id
      , orders.*
      , products.product_id
      , products.name as product_name
      , products.category as product_category
      , order_prices.price
      , date_trunc(orders.created_at, day) as order_day
      , date_trunc(orders.created_at, week) as order_week
      , date_trunc(orders.created_at, month) as order_month
      , date_trunc(orders.created_at, quarter) as order_quarter
      , date_trunc(orders.created_at, year) as order_year
    
    from order_prices
    left join orders
      on order_prices.order_id = orders.order_id
    left join products
      on order_prices.product_id = products.product_id

    order by created_at, order_id, product_category, product_name
)

select * from final