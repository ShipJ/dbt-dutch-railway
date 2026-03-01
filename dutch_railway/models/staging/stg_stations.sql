select
    id as station_id,
    code as station_code,
    uic as uic_code,
    name_short,
    name_medium,
    name_long as station_name,
    slug,
    country,
    type as station_type,
    geo_lat as latitude,
    geo_lng as longitude
from {{ source('railway', 'stations') }}
