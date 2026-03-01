with services as (

  select
    service_id,
    service_date,
    service_type,

    max(case when is_origin then station_id end) as origin_station_id,
    max(case when is_destination then station_id end) as destination_station_id,
    max(case when is_arrival = 1 or is_departure = 1 then 1 else 0 end) as service_ran,

    max(
      case
        when (is_peak_departure = 1 and is_departure_cancelled = 0) or (is_peak_arrival = 1 and is_arrival_cancelled = 0) then 1
      end
    ) as service_ran_during_peak_hours,

    max(
      case
        when (is_peak_departure = 0 and is_departure_cancelled = 0) or (is_peak_arrival = 0 and is_arrival_cancelled = 0) then 1
      end
    ) as service_ran_during_off_peak_hours,

    -- Not ansi-sql, may want to revise
    array_agg(station_id order by scheduled_stop_sequence) filter (where not is_arrival_cancelled and (is_origin or is_arrival_scheduled)) as stations_on_route,
    array_agg(station_id order by scheduled_stop_sequence) filter (where not is_arrival_cancelled and (is_origin or is_arrival)) as stations_visited_sequence,
    array_agg(station_id order by scheduled_stop_sequence) filter (where is_station_skipped) as stations_skipped,
    array_agg(station_id order by scheduled_stop_sequence) filter (where is_arrival_cancelled) as stations_cancelled
    from {{ ref('fact_service_stop') }}
    group by 1, 2, 3

),

service_routes_mapped as (
  select
    service_id,
    service_date,
    service_type,

    -- Generate unique route key
    md5(
      cast(service_type as varchar) || 
      '-' || 
      cast(array_to_string(stations_on_route, '-') as varchar)
    ) as route_key,

    -- Generate unique origin/destination key
    {{ dbt_utils.generate_surrogate_key(['origin_station_id', 'destination_station_id']) }} as origin_destination_key,
    

    origin_station_id,
    destination_station_id,

    service_ran,
    service_ran_during_peak_hours,
    service_ran_during_off_peak_hours,

    stations_on_route,
    stations_visited_sequence,
    stations_skipped,
    stations_cancelled,
    ifnull(array_length(stations_on_route), 0) as stations_on_route_count,
    ifnull(array_length(stations_visited_sequence), 0) as stations_visited_count,
    ifnull(array_length(stations_skipped), 0) as stations_skipped_count,
    ifnull(array_length(stations_cancelled), 0) as stations_cancelled_count
  from services

)

select * from service_routes_mapped
