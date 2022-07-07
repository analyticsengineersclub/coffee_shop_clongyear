{{ config(materialized='table') }}

with pageviews as (

    select * from {{ ref('int_stitched_pageviews') }}

)

, event_lags as (
  select
      pageviews.*
    , lag(timestamp, 1) over (partition by visitor_id order by timestamp) as last_event
  from pageviews
)

, time_deltas as (
  select
      event_lags.*
    , timestamp_diff(timestamp, last_event, second) as delta_seconds
  from event_lags
)

, session_flags as (
  select
      time_deltas.*
    , case
        when last_event is null then 1
        when delta_seconds <= 60*30 then 1
        else 0
        end as new_session_flag
  from time_deltas
)

, sum_session_flags as (
  select
      session_flags.*
    , sum(new_session_flag) over (partition by visitor_id order by timestamp) as visitor_session
  from session_flags
)

, assign_session_ids as (
  select
    sum_session_flags.*
  , {{ dbt_utils.surrogate_key(['visitor_id', 'visitor_session']) }} as session_id
  from sum_session_flags
)

, final as (
  select
      pageview_id
    , visitor_id
    , customer_id
    , session_id
    , device_type
    , page
    , timestamp
  from assign_session_ids
  order by visitor_id, timestamp
)

select * from final