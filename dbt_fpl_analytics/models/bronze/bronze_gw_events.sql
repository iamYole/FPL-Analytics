select 
    *
from {{ source('fpl_analytics_raw', 'gw_events') }}