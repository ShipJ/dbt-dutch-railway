{{ 
  config(
    materialized='incremental',
    unique_key='station_date_key',
    incremental_strategy='delete+insert'
    ) 
}}

with station_day as (

  -- Much faster than using fact_service_stop for arrivals / departures

  select 
    -- Generate unique service + date key
    {{ dbt_utils.generate_surrogate_key(['station_id', 'service_date']) }} as station_date_key,
    service_date,
    station_id,

    sum(is_departure_scheduled) as total_departures_scheduled,
    sum(is_departure) as total_departures,
    sum(is_departure_cancelled) as total_departures_cancelled,

    sum(is_arrival_scheduled) as total_arrivals_scheduled,
    sum(is_arrival) as total_arrivals,
    sum(is_arrival_cancelled) as total_arrivals_cancelled

  from {{ ref('fact_service_stop') }}

  {% if is_incremental() %}
    where service_date >= (current_date - interval '7 day')
  {% endif %}

  group by 1, 2, 3

)

select * from station_day


