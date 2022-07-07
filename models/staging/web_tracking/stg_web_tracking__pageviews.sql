with pageviews as (
    select * from {{ source('web_tracking', 'pageviews') }}
)

select 
    id as pageview_id
  , visitor_id
  , customer_id
  , device_type
  , page
  , timestamp
from pageviews