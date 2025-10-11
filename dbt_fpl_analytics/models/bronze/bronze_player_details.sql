select 
    *
from {{ source('fpl_analytics_raw', 'player_details') }}