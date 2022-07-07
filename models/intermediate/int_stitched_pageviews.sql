with pageviews as (

    select * from {{ ref('stg_web_tracking__pageviews') }}

)

, known_customer_pageviews as (
    select
        customer_id
      , visitor_id
      , timestamp
      , row_number() over (partition by customer_id order by timestamp, visitor_id) as pageview_rank
    from pageviews
    where customer_id is not null
)

, first_customer_visitor_ids as (
    select
        customer_id
      , visitor_id
    from known_customer_pageviews
    where pageview_rank = 1
)

, distinct_customer_visitors as (
    select distinct customer_id, visitor_id
    from known_customer_pageviews
)

, customer_id_backfill as (
    select
        pageviews.*
      , coalesce(pageviews.customer_id, distinct_customer_visitors.customer_id) as cid_clean
    from pageviews
    left join distinct_customer_visitors
      on pageviews.visitor_id = distinct_customer_visitors.visitor_id
)

, visitor_id_backfill as (
    select
        customer_id_backfill.*
      , coalesce(first_customer_visitor_ids.visitor_id, customer_id_backfill.visitor_id) as vid_clean
    from customer_id_backfill
    left join first_customer_visitor_ids
      on customer_id_backfill.cid_clean = first_customer_visitor_ids.customer_id
)

, final as (
    select
        pageview_id
      , vid_clean as visitor_id
      , cid_clean as customer_id
      , device_type
      , page
      , timestamp
    from visitor_id_backfill
)

select * from final