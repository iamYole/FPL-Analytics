select 
    *
from {{ source('fpl_analytics_raw', 'teams') }}