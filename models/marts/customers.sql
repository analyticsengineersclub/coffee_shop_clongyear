{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_coffee_shop__customers') }}
)

, orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
)

, customer_order_history as (
    select
        customer_id
      , count(*) as n_orders
      , sum(total) as cumulative_order_total
      , min(created_at) as first_order_at
      
    from orders 
    group by 1
)

, final as (
    select 
        customers.customer_id
      , customers.name
      , customers.email
      , coalesce(customer_order_history.n_orders, 0) as n_orders
      , customer_order_history.cumulative_order_total
      , customer_order_history.first_order_at
      , date_trunc(customer_order_history.first_order_at, week) as cohort_week
      , date_trunc(customer_order_history.first_order_at, month) as cohort_month
      , date_trunc(customer_order_history.first_order_at, quarter) as cohort_quarter
      , date_trunc(customer_order_history.first_order_at, year) as cohort_year
      
    from customers
    left join  customer_order_history
      on  customers.customer_id = customer_order_history.customer_id
)

select * from final
order by first_order_at, customer_id