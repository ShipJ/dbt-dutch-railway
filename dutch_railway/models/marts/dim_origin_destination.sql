with origin_destination as (

    select 
        -- Generate unique origin/destination key
        origin_destination_key,
        origin_station_id,
        origin_station_code,
        origin_station_name_long,
        destination_station_id,
        destination_station_code,
        destination_station_name_long
    from {{ ref('dim_route') }} dr 
    group by 1, 2, 3, 4, 5, 6, 7

    -- Dedupe on unique key
    qualify row_number() over (
        partition by origin_destination_key
        order by origin_station_code
    ) = 1

)

select * from origin_destination