/* Q2 - Which station experiences the most disruptions longer than 30 minutes?
>> Schipol Airport with 43

*/

select
  bds.station_id,
  ds.station_code,
  ds.station_name_long,
  sum(case when fd.duration_minutes > 30 then 1 else 0 end) as disruptions_over_30m -- BI-friendly sum (exchange 30 for any number)
from {{ ref('fact_disruption') }} fd 
inner join {{ ref('bridge_disruption_station') }} bds
  on fd.disruption_id = bds.disruption_id
inner join {{ ref('dim_station') }} ds
  on ds.station_id = bds.station_id
group by 1, 2, 3
order by disruptions_over_30m desc

