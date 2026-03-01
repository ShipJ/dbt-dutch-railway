with stations as (

    /* dim_station
    Description: Dimension table containing information about railway stations.
    Notes:
    - SCD type 2 snapshot used to get current records only
    */

    select
        station_id,
        station_code,
        uic_code,
        station_name_short,
        station_name_medium,
        station_name_long,
        slug,
        country,
        station_type,
        latitude,
        longitude,
    from {{ ref('snap_station') }} -- read from SCD type 2 snapshot
    where dbt_valid_to is null -- only current records

)

select * from stations
