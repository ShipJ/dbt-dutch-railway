/* Q1 which station is stopped at the most
>> Utrecht Centraal

Assume we mean actual arrivals, not skipped or cancelled. 
This does not include the origin as it didn't 'stop' there, it 'started', but it would be trivial to add this. The answer is the same regardless, but some other stations are ordered differently.
*/

-- Option 1: Naive method - uses biggest fact table
select
  fss.station_id,
  ds.station_code,
  ds.station_name_long,
  sum(is_arrival) as arrivals, -- business friendly summation rather than complex filtering
  sum(is_arrival) + sum(is_origin) as arrivals_including_origin,
  sum(is_arrival_scheduled) as arrivals_scheduled,
  sum(is_arrival_cancelled) as arrivals_cancelled
from {{ ref('fact_service_stop') }} fss
inner join {{ ref('dim_station') }} ds
  on fss.station_id = ds.station_id
group by 1, 2, 3
order by arrivals desc

-- Option 2: Faster / more efficient - uses aggregate table
-- select 
--   asd.station_id,
--   ds.station_code,
--   ds.station_name_long,
--   sum(asd.total_arrivals) as arrivals,
--   sum(asd.total_arrivals_scheduled) as arrivals_scheduled,
--   sum(asd.total_arrivals_cancelled) as arrival_cancellations
-- from {{ ref('agg_station_day') }} asd 
-- inner join {{ ref('dim_station') }} ds
--   on asd.station_id = ds.station_id
-- group by 1, 2, 3
-- order by arrivals desc
