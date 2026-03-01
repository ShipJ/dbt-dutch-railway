{{ 
  config(
    materialized='incremental',
    unique_key='service_stop_key',
    incremental_strategy='delete+insert'
  ) 
}}

with service_stop_deduped as (

  select 

    -- Generate unique service + stop key
    {{ dbt_utils.generate_surrogate_key(['service_rdt_id', 'stop_rdt_id']) }} as service_stop_key,

    service_rdt_id as service_id,
    service_date,
    service_type,
    station_code,
    stop_rdt_id as stop_id,
    
    -- service_company,
    -- train_number,

    -- case 
    --   when is_completely_cancelled then 1 
    --   else 0 
    -- end as is_completely_cancelled,
    
    -- case 
    --   when is_partly_cancelled then 1 
    --   else 0 
    -- end as is_partly_cancelled,

    -- Departures
    departure_time as scheduled_departure_time,
    case 
      when is_departure_cancelled = 0 then scheduled_departure_time + departure_delay_minutes * interval '1 minute'
    end as actual_departure_time,
    case 
      when departure_time is not null then 1 
      else 0 
    end as is_departure_scheduled,
    departure_delay_minutes,
    case 
      when is_departure_cancelled then 1 
      else 0 
    end as is_departure_cancelled,

    -- Arrivals
    arrival_time as scheduled_arrival_time,
    case 
      when is_arrival_cancelled = 0 then scheduled_arrival_time + arrival_delay_minutes * interval '1 minute'
    end as actual_arrival_time,
    case 
      when arrival_time is not null then 1
      else 0 
    end as is_arrival_scheduled,
    arrival_delay_minutes,
    case 
      when is_arrival_cancelled then 1 
      else 0 
    end as is_arrival_cancelled,

    -- Service stop order based on earliest departure time, then earliest arrival time if missing. If both missing, last.
    row_number() over (
      partition by service_id
      order by coalesce(scheduled_departure_time, scheduled_arrival_time) nulls last
    ) as stop_order,

    -- Reverse to above
    row_number() over (
      partition by service_id
      order by coalesce(scheduled_arrival_time, scheduled_departure_time) desc nulls last
    ) as stop_reverse_order,

    -- Revisit:
    -- maximum_delay_minutes,
    -- s.has_platform_change,
    -- s.planned_platform,
    -- s.actual_platform

  from {{ ref('stg_services') }} 

  where service_date <= current_date -- data quality, ignore future services

  -- delete + insert within last 3 days
  {% if is_incremental() %}
      and service_date >= (
        select coalesce(max(service_date), date '1900-01-01')
        from {{ this }}
      ) - interval 3 day
  {% endif %}

  -- Dedupe per service + stop by latest depart/arrive time
  qualify row_number() over (
    partition by service_stop_key
    order by coalesce(scheduled_departure_time, scheduled_arrival_time) desc nulls last
    ) = 1
    
),
  
service_stops_sequence as (

  select
    s.service_stop_key,
    s.service_id,
    s.service_date,
    s.service_type,
    -- s.service_company,
    -- s.train_number,
    -- s.is_completely_cancelled,
    -- s.is_partly_cancelled,
    s.stop_id,
    ds.station_id,
    s.station_code,
    s.scheduled_departure_time,
    s.actual_departure_time,
    s.is_departure_scheduled,
    s.departure_delay_minutes,
    s.is_departure_cancelled,
    s.scheduled_arrival_time,
    s.actual_arrival_time,
    s.is_arrival_scheduled,
    s.arrival_delay_minutes,
    s.is_arrival_cancelled,
    
    -- The station was skipped for some reason (neither was scheduled)
    case
      when scheduled_arrival_time is null and scheduled_departure_time is null then 1
      else 0 
    end as is_station_skipped,

    case 
      when actual_departure_time is not null then 1 
      else 0 
    end as is_departure,

    -- Based on arrival time being during 
    case 
      when is_departure then
        case
          when extract('dow' from actual_departure_time) between 1 and 5
          and (
            (extract('hour' from actual_departure_time) >= 7 and extract('hour' from actual_departure_time) < 9)
            or
            (extract('hour' from actual_departure_time) >= 16 and extract('hour' from actual_departure_time) < 18)
          )
          then 1
          else 0
        end 
      else null
    end as is_peak_departure,

    case 
      when actual_arrival_time is not null then 1
      else 0 
    end as is_arrival,

    case 
      when is_arrival then
        case
          when extract('dow' from actual_arrival_time) between 1 and 5
          and (
            (extract('hour' from actual_arrival_time) >= 7 and extract('hour' from actual_arrival_time) < 9)
            or
            (extract('hour' from actual_arrival_time) >= 16 and extract('hour' from actual_arrival_time) < 18)
          )
          then 1
          else 0
        end 
      else null
    end as is_peak_arrival,

    -- scheduled stops within a service run
    case 
      when scheduled_departure_time is not null or scheduled_arrival_time is not null then stop_order
      else null
    end as scheduled_stop_sequence,

    -- Flag 'first' stop as origin
    case
      when stop_order = 1 then 1 
      else 0
    end as is_origin,

    -- Flag 'last' stop as destination
    case
      when stop_reverse_order = 1 then 1 
      else 0
    end as is_destination

  from service_stop_deduped s
  left join {{ ref('dim_station') }} ds 
    on ds.station_code = s.station_code

)

select * from service_stops_sequence

-- 187778, 5988