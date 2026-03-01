with deduped_route_patterns as (

    select
        service_type,
        stations_on_route,
        stations_visited_sequence,
        stations_skipped,
        stations_cancelled,
        route_key,
        origin_destination_key,
    from {{ ref('fact_service') }}

    -- Deduplicate route patterns to get unique routes only
    qualify row_number() over (
        partition by route_key
        order by service_id
    ) = 1

)

, dim_route as (

    select
        route_key,
        origin_destination_key,
        service_type,
        stations_visited_sequence[1] as origin_station_id,
        src.station_code as origin_station_code,
        src.station_name_long as origin_station_name_long,
        stations_visited_sequence[array_length(stations_visited_sequence)] as destination_station_id,
        dst.station_code as destination_station_code,
        dst.station_name_long as destination_station_name_long,
        stations_on_route,
        ifnull(array_length(stations_on_route), 0) as stations_on_route_count,
        stations_visited_sequence,
        ifnull(array_length(stations_visited_sequence), 0) as stations_visited_count,
        stations_skipped,
        ifnull(array_length(stations_skipped), 0) as stations_skipped_count,
        stations_cancelled,
        ifnull(array_length(stations_cancelled), 0) as stations_cancelled_count,
    from deduped_route_patterns dr 
    left join {{ ref('dim_station') }} src   
        on src.station_id = dr.stations_visited_sequence[1]
    left join {{ ref ('dim_station') }} dst 
        on dst.station_id = stations_visited_sequence[array_length(stations_visited_sequence)]

    where stations_cancelled_count = 0 and stations_skipped_count = 0 -- only include complete routes

) 

select * from dim_route