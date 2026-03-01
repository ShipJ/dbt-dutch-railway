select
    "Station" as origin_station_code,
    * EXCLUDE ("Station")
    
from {{ source('railway', 'tariff_distances') }}
