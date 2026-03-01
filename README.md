# Analytics Engineering Technical Test 

**Simple analytics engineering setup with dbt + DuckDB**

This repository contains a setup script for building a virtual environment with sample Dutch railway data, accompanied by a dbt project with staging layers. The data is based on train service performance from [rijdendetreinen.nl](https://opendata.rijdendetreinen.nl/).

---

# 📋 Table of Contents

- [Getting Started](#getting-started)
- [Repository Guide](#repository-guide)
- [Technical Test Requirements](#technical-test-requirements)

---

# Getting Started

## 📋 Prerequisites

**Works on Mac, Windows, and Linux!** The setup script will check for these (and help install missing ones):
- **Python 3.10+** (supports 3.10, 3.11, 3.12) - [Download here](https://www.python.org/)
## ⚡ Quick Setup

```bash
python setup.py
```

## 🗄️ Database Access

Your data will be stored in a local DuckDB file inside the dbt project:
- **Database file**: `dutch_railway/dutch_railway.duckdb`
- **Query with**: Duckdb CLI: `duckdb dutch_railway.duckdb`
- **Sample query**: `SELECT * FROM main.stg_stations LIMIT 10;`

---

# Repository Guide

### **Dutch Railway Data (January 2024)**
- **Service records** (1.8M records): Real train service performance data from January 2024
- **Station data** (578 stations): All railway stations across Netherlands with geographic coordinates
- **Disruption incidents** (519 records): Service disruptions with cause analysis for January 2024
- **Distance matrix** (399 pairs): Tariff distance calculations between station pairs

### **dbt Project Structure**

```
dutch_railway/
├── models/
│   ├── staging/
│   │   ├── stg_services.sql
│   │   ├── stg_stations.sql
│   │   ├── stg_disruptions.sql
│   │   └── stg_tariff_distances.sql
├── profiles.yml
├── dbt_project.yml
└── dutch_railway.duckdb
```

### 📈 Business Questions
1. **"Which station is stopped at the most?"**
2. **"Which station experiences the most disruptions longer than 30 minutes?"**
3. **"What is the average distance between connected stations?"**
4. **"How many services run in peak vs off-peak hours?"**
    - Peak hours are defined as 7-9 AM and 4-6 PM on weekdays.
5. **"What are the top 5 most common causes of service disruptions?"**
6. **"Which train routes have the most services per day split by service type?"**

### 📝 Additional Considerations

- Only a month's worth of services are provided so consider how to model for scalability. 
    - What if we were modeling a full year of data? 
    - What if we were looking at data from the whole of Europe?
- Businesses use a Semantic Layer for serving metrics to business users. Consider how you would model for this. Some examples of Semantic Layer tools:
    - [Lightdash](https://docs.lightdash.com/guides/lightdash-semantic-layer)
    - [dbt Cloud](https://docs.getdbt.com/guides/sl-partner-integration-guide?step=1)

## 🧬 Modelling

I modeled the Dutch Railway data to reflect real-world railway concepts as much as possible. I considered *entities* (things that exist, are relatively stable, and descriptive), *events* (things that happen, are time-bound and/or or measurable), and the *relationships* between them. I created an example ERD (`images > Example ERD.png`) but would want to flesh this out.

##### Naming conventions
I preferred lower-case sql, snake_case column names, and standard prefixes for views/tables:
- `dim_*`: slowly changing dimension or descriptive entity
- `fact_*`: time-variant events or measurements between entities
- `bridge_*`: many-to-many relationships for ease of joining
- `agg_*`: aggregated queries that summarise events for optimising BI performance

### Entities
- **Station** (`marts.dim_station`): a real train station in the network (e.g Utrecht Centraal) that has a name, code, and location, and uniquely identified using `station_id`. 
- **Service** (`marts.fact_service`): one record represents one service. A service runs on a particular date, follows a particular route, and can be partially or wholly cancelled. It is an anomaly in that we model it as a fact rather than a dimension, because it is time-dependent and meaningful context comes from the journey itself. It can be uniquely identified using `service_id` and is derived from `fact_service_stop` by aggregating at the service level.
- **Route** (`marts.dim_route`): one record represents one route, defined as a unique sequence of stations that a service could run through. The first station in the sequence is known as the 'origin', while the last is known as the 'destination'. Different services can run through the same route, and different routes can exist between stations.
- **Origin -> Destination Pair** (`marts.dim_origin_destination`): a pair of stations - one being the 'origin', the other the 'destination' of a route. The route itself does not matter. There can be many routes between origin -> destination pairs. More typically how passengers think of 'from A -> Z'.

### Events
- **Stop** (`marts.fact_service_stop`): one record represents one service stopping at one station, capturing arrival and departure information (times, delays, cancellations). 
- **Distance** (`marts.fact_station_distance`): one record captures the distance between a unique pair of stations for fare estimation. Treated as a fact rather than a dimension as it may evolve over time, and is a measurement that captures a relationship between stations.
- **Disruption** (`marts.fact_disruption`): one record represents one incident. An incident can affect one or more stations for a period of time. Used to derive `bridge_disruption_station` by exploding out the list of affected stations.

### Notes / Tradeoffs
- Despite an 'arrival' being a distinct operational event to a 'departure', I chose to model them together as one *stop* to reflect source data and the fact that they are often analysed jointly. This results in columns (e.g `departure_time` and `arrival_time`, that could be combined as a single `event_time` with an `event_type`), but the extra complexity outweighs the negligible impact on cognitive load.
- `marts.dim_station` is modelled as an SCD Type 2 dimension (dbt snapshot), to capture changes over time, filtered on 'current' records only. This could be run on a less frequent schedule depending on real-world expected change.
- `marts.dim_route` is based on 'scheduled' routes rather than skipped or cancelled stops to avoid *slight variant* routes
- `marts.agg_station_day` aggregates `fact_service` at the station + day level, to massively reduce dataset size for common queries about arrivals and departures
- `marts.fact_service_stop` and `marts.agg_station_day` are implemented as incremental models that use the delete+insert strategy (merge not supported in duckdb), improving efficiency when loading new data by avoiding re-processing historical unchanged records. I would partition these tables on respective dates if supported (e.g BigQuery). I would also want to review this in light of late-arriving data considerations, and potentially use an `ingested_at` timestamp instead.
- A `skipped` station is where neither an arrival nor departure was scheduled, cancelled, or actually occured, but an event is recorded nonetheless (route planning purposes?)
- I generated surrogate keys using dbtutils where necessary for consistent reporting and reliable downstream joins
- I chose to ignore `line` and `platform_change` information which I'm sure is useful but needs more thought
- I preferred lower-level `arrival_cancelled` and `departure_cancelled` over `completely_cancelled` and `partly_cancelled`, assuming they can be reconciled (i.e if every arrival and departure is cancelled for a service, it is completely cancelled)
- I avoided editing staging models directly, but would want to push some basic cast/transform/clean logic there
- I did edit `sources.yml` to demonstrate a `source freshness` test
- I would materialise mart models as **tables** rather than views to boost BI performance
- I did not consider **lines** or **services** and how they might be impacted by disruptions; only **stations**. With more time I could look into this based on occurence time.


## 🧪 Tests

Tests are split into the following 3 sections: **source freshness**, **data quality**, and **unit tests**. I did not consider *data contracts* at this stage as the schemas are still rapidly evolving, but I would as soon as assets are in production.

#### 1. Source freshness checks

`dbt source freshness` will `warn` and `error` against certain datasets, based on latest records being n days out of date. In practice, I would want to align timings with business operations and any SLAs.

#### 2. Data Quality tests

I assigned dummy tags based on a test's importance to operation. By default, `tier2` is applied to all tests, while `tier1` or `tier2` tags can be applied on a case by case basis. This would require collaboration with operational and analytics teams.
- **tier0** - critical correctness guarantees for operation, failures are blocking
- **tier1** - important quality signals, failures initiate investigation
- **tier2** - Informational or exploratory checks, failures can be reviewed later.

Tags allow for the generation of simple commands to run subsets of tests e.g `dbt test -select tag:tier0` runs all `tier0` tests, while `dbt test -select tag:tier2 -exclude tag:tier0 tag:tier1` runs only `tier2` tests (by excluding anything also tagged as `tier0` or `tier1`). I didn't implement this fully but there are some examples of each.

In practice, we could run these commands *alongside*, or *independently* of model runs depending on severity and time to complete. E.g we might require `tier0` tests, while `tier2` tests may only occur in dev, or on an infrequent basis. With dbt_artifacts, we can also analyse how long tests take to run to identify bottlenecks.

#### 3. Unit tests
I did not have time to implement [unit tests](https://docs.getdbt.com/docs/build/unit-tests) but could imagine doing something as follows to validate logic.

```
unit_tests:
  - name: test_station_distance_origin_destination
    description: "Check that the distance between origin station X and destination station Y is correctly calculated."
    model: fact_station_distance
    given:
      - input: ref('fact_station_distance')
        rows:
          - {origin_id: X, destination_id: Y}
    expect:
      rows:
        - {distance_km: 92}
```

## 📊 Visualisation

### Lightdash + Postgres + Docker

I have never used lightdash but managed to set up a docker service that runs a local lightdash stack pointing to data in postgres (Lightdash doesn't support duckdb right now). I assume Snowflake/BigQuery would be used in practice.

The `docker-compose.yml` installs postgres and points lightdash to it. It also requires object storage hence the need for minIO.

Data is synced on every `dbt run|build`, provided the post-hook command in `dbt_project.yml` is uncommented. I created a macro that creates a schema and each table. Please see `images > *.png` for some examples!

### Prerequisites

- Docker Desktop/Engine
- Docker Compose v2

### Quickstart

1. Create an env file in root: `cp .env.example .env`. 
2. You may need to run `set -a source .env set +a` to load into session
3. Start stack: `docker compose up -d`
4. Open Lightdash: http://localhost:8080
- Use anything for first/last name, email, password
- Select warehouse: `PostgresDB`, upload: `Manual`
5. Postgres
- DB details: as in .env
- Advanced > disable SSL
6. dbt connection
- Type: `dbt local`
- Version: `latest`
- Target: `dev`
- Schema: `marts`
1. Sync dbt -> postgres
- Uncomment the post-hook in dbt_project.yml
- Run `dbt build` to sync

### Lightdash Dimensions and Metrics

I noticed that dimensions defined in `.yml` files are created by default in lightdash, while measures and joins require additional `meta` and `config` tags. I added an example of both in `fact_station_distance.yml`. I found this part very intuitive coming from a Looker/LookML background! I did not spend much time cleaning/standardising fields for BI.

```
# Example measure
config:
    meta:
        metrics:
            avg_distance_km:
                type: average
            total_distance_km:
                type: sum
```

```
# Example join
meta:
    primary_key: origin_destination_key
    joins:
        - join: dim_station
            alias: origin
            label: "Origin"
            type: inner
            sql_on: ${origin.station_id} = ${fact_station_distance.origin_station_id}
            relationship: many-to-one
```

## ❓ Questions 1-6

Please see `models>analytics>*.sql` files for queries, and sample metrics in `.yml` files that work with lightdash - I'd be happy to demo this. `images > Lightdash Q3 Match.png` shows me getting the same result when writing SQL, as we do when exposing the fact/dim tables in an explore. I did not spend much more time here but personally love this part of the process!

### What if we were modelling a full year of data? 

Of the source data sets, by considering their 'grain', I would prioritise those that would grow most substantially in volume or complexity:
- Stations (*grain = station*): volume change negligible, but capturing changes (e.g code, name, fixes etc) by modelling as an SCD Type 2 dimension still useful (implemented)
- Station Distance (*grain = station x station*): volume change larger but still negligible. Would also be useful to capture as an SCD Type 2 to run point-in-time analyses of distances
- Disruptions (*grain = disruption*): volume grows linearly but likely still negligible. Would *benefit* from an incremental model, partition on `disruption_date` but not critical
- Routes (*grain = one ordered sequence of stations*): potential to increase a lot, especially with (temporary stops, construction), but still dwarfed by service stops. 
- Service Stops (*grain = service x stop*): <u>main volume driver</u> and would have to be modelled as an incremental model for efficiency, consider partition on `service_date` and clustering where supported. This is the main bottleneck, so I would direct my efforts here.

#### Operational considerations
- Enforce sensible defaults in BI-tools (e.g date filters initially select last 7 days). This would be covered in some sort of educational piece for analysts (use the right tables for the right jobs)
- Analyse query logs for common queries and identify bottlenecks or more efficient methods
- dbt runtime (models, tests, snapshots etc). Can install `dbt_artifacts` package to monitor runtimes and bottlenecks (both in terms of GB processed and time taken)
- Backfill strategy - consider late-arriving data, how important is correctness over speed and define an explicit approach to balance correctness against pipeline execution time.
- If needed, run tests and full refreshes separately and on lower cadences

### What if we were looking at data for the whole of Europe?

Assuming scale is not the *only* concern, several modelling and semantic considerations appear. I thought about this in terms of what would immediately break or stop being true.

#### Unique Station Codes
- Initial observation is that `station_code` will no longer be unique. Concat with `country` to be sure, or use uic_code if found to be reliable.

#### Timezones
- Convert / store all timestamps in UTC. Time-based analytics (e.g. peak hours) should be derived using local time per country or region, rather than assumed globally. We would have to extend the 'peak hour' logic, perhaps as a date/time dimension that could be joined in where needed.

#### Distance Tariffs
- Will likely no longer be symmetric, and potentially different tariffs per country. Potentially also a scale concern, given there will likely be a *lot* more station pairs.

#### Language
- Ensure disruption causes are translated to a common language, and grouped if/where language differs. Service types, station types, route operators, and other text-based fields may also need to be considered.

#### Data structure
- It is unlikely that other railway operators have the same data structure and completeness. Before anything, we'd have to establish similarities and differences, track assumptions and gaps, particularly around semantics and logic.

