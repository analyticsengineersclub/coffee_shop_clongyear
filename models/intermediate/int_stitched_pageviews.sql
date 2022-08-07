with pageviews as (

    select * from {{ ref('stg_web_tracking__pageviews') }}

)

, known_customer_pageviews as (
    select
        customer_id
      , visitor_id
      , timestamp
      , first_value(visitor_id) over (partition by customer_id order by timestamp, visitor_id) as first_visitor_id
    from pageviews
    where customer_id is not null
)

, distinct_customer_visitors as (
    select distinct customer_id, visitor_id, first_visitor_id
    from known_customer_pageviews
)

, backfill as (
    select
        pageviews.*
      , coalesce(pageviews.customer_id, distinct_customer_visitors.customer_id) as cid_clean
      , coalesce(distinct_customer_visitors.first_visitor_id, pageviews.visitor_id) as vid_clean
    from pageviews
    left join distinct_customer_visitors
      on pageviews.visitor_id = distinct_customer_visitors.visitor_id
)

, final as (
    select
        pageview_id
      , vid_clean as visitor_id
      , cid_clean as customer_id
      , device_type
      , page
      , timestamp
    from backfill
)

select * from final