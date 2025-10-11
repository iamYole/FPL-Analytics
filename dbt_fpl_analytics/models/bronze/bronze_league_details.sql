select 
    *
from {{ source('fpl_analytics_raw', 'league_details') }}