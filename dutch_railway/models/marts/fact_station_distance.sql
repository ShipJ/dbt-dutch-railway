with station_distance_unpivotted as (

    /* fact_station_distance
    Description: Fact table containing distances between railway stations based on tariff distances.
    Notes:
    - No NULLs in source data -> possible to travel between any 2 stations
    - Distances always symmetric i.e A -> B == B -> A
    - Filter out self -> self distances ('XXX')
    */

    select
        origin_station_code,
        destination_station_code,
        distance
    from {{ ref('stg_tariff_distances') }}

    -- Unpivot wide table into a long table 
    unpivot (
        distance for destination_station_code in (
            * exclude (origin_station_code)
        )
    )

    where origin_station_code <> destination_station_code -- Filter out self -> self ('XXX')
)

, station_distance_mapped as (

    select
        src.station_id as origin_station_id,
        origin_station_code,
        dst.station_id as destination_station_id,
        destination_station_code,
        {{ dbt_utils.generate_surrogate_key(['origin_station_id', 'destination_station_id']) }} as origin_destination_key,
        try_cast(distance as float) as distance_km
    from station_distance_unpivotted su 
    left join {{ ref('dim_station') }} src -- map source station codes to ids
        on src.station_code = su.origin_station_code
    left join {{ ref ('dim_station') }} dst -- map destination station codes to ids
        on dst.station_code = su.destination_station_code

    -- Dedupe on unique key
    qualify row_number() over (
        partition by origin_destination_key
        order by origin_station_code
    ) = 1

) 

select * from station_distance_mapped