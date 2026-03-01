with disruption_stations_unnested as (
    
    select 
        disruption_id
        , station_code
    -- Split out station_code array as separate rows
    from {{ ref('fact_disruption') }}, unnest(split(affected_station_codes, ', ' )) as t(station_code)

)

, disruption_stations_deduped as (

    select 
        md5(
            cast(dsu.disruption_id as varchar) || '-' || cast(ds.station_id as varchar)
        ) as disruption_station_key,
        dsu.disruption_id,
        ds.station_id
    from disruption_stations_unnested dsu
    inner join {{ ref('dim_station') }} ds -- Map codes to IDs
        on ds.station_code = dsu.station_code

    -- Dedupe on unique key
    qualify row_number() over (
        partition by disruption_station_key
        order by ds.station_id
    ) = 1

)

select * from disruption_stations_deduped
