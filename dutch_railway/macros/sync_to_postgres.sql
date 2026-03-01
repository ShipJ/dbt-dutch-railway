{% macro sync_to_postgres() %}

{% set user = env_var('POSTGRES_USER') %}
{% set password = env_var('POSTGRES_PASSWORD') %}
{% set port = env_var('POSTGRES_PORT') %}
{% set db = env_var('POSTGRES_DB') %}
{% set url = 'postgresql://' ~ user ~ ':' ~ password ~ '@localhost:' ~ port ~ '/' ~ db %}

INSTALL postgres;

LOAD postgres;

ATTACH '{{ url }}' AS pg (TYPE postgres);

CREATE SCHEMA IF NOT EXISTS pg.marts;

CREATE OR REPLACE TABLE pg.marts.dim_station AS SELECT * FROM {{ ref('dim_station') }};
CREATE OR REPLACE TABLE pg.marts.fact_service_stop AS SELECT * FROM {{ ref('fact_service_stop') }};
CREATE OR REPLACE TABLE pg.marts.fact_station_distance AS SELECT * FROM {{ ref('fact_station_distance') }};

{% endmacro %}