with source_data as (
    select * from {{ source('railway', 'services') }}
)

select
    -- Service-level identifiers
    "Service:RDT-ID"::bigint as service_rdt_id,
    "Service:Date"::date as service_date,
    
    -- Service details  
    "Service:Type"::varchar as service_type,
    "Service:Company"::varchar as service_company,
    "Service:Train number"::bigint as train_number,
    
    -- Service status flags
    "Service:Completely cancelled"::boolean as is_completely_cancelled,
    "Service:Partly cancelled"::boolean as is_partly_cancelled,
    "Service:Maximum delay"::bigint as maximum_delay_minutes,
    
    -- Stop-level identifiers
    "Stop:RDT-ID"::bigint as stop_rdt_id,
    "Stop:Station code"::varchar as station_code,
    "Stop:Station name"::varchar as station_name,
    
    -- Departure information
    "Stop:Departure time"::timestamp as departure_time,
    "Stop:Departure delay"::bigint as departure_delay_minutes,
    "Stop:Departure cancelled"::boolean as is_departure_cancelled,

    -- Arrival information
    "Stop:Arrival time"::timestamp as arrival_time,
    "Stop:Arrival delay"::bigint as arrival_delay_minutes,
    "Stop:Arrival cancelled"::boolean as is_arrival_cancelled,    
    
    -- Platform information
    "Stop:Platform change"::boolean as has_platform_change,
    "Stop:Planned platform"::varchar as planned_platform,
    "Stop:Actual platform"::varchar as actual_platform
    
from source_data
