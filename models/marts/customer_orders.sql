{{ config(materialized='table') }}

with product_orders as (

    select * from {{ ref('product_orders') }}

)

, customers as (

    select * from {{ ref('customers') }}

)

, product_orders_join_customers as (
    select
        product_orders.*
      , customers.name
      , customers.email
      , customers.first_order_at
      , customers.cohort_week
      , customers.cohort_month
      , customers.cohort_quarter
      , customers.cohort_year
    from product_orders
    left join customers
      on product_orders.customer_id = customers.customer_id
)

, final as (
    select
        product_orders_join_customers.*
      , date_trunc(created_at, day) as order_day
      , date_trunc(created_at, week) as order_week
      , date_trunc(created_at, month) as order_month
      , date_trunc(created_at, quarter) as order_quarter
      , date_trunc(created_at, year) as order_year
      , date_diff(created_at, first_order_at, day) as days_since_first_order
    from product_orders_join_customers
)

select * from final