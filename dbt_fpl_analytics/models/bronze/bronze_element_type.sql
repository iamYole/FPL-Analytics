select 
    *
from {{ source('fpl_analytics_raw', 'elements_type') }}