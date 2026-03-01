{% snapshot snap_station %}

{{
  config(
    target_schema='snapshots',
    unique_key='station_id',
    strategy='check',
    check_cols=['station_type']
  )
}}

select
    station_id,
    station_code,
    uic_code,
    name_short as station_name_short,
    name_medium as station_name_medium,
    station_name as station_name_long,
    slug,
    country,
    station_type,
    latitude,
    longitude
from {{ ref('stg_stations') }}

{% endsnapshot %}
