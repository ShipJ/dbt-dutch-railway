/* Q4 - How many services run in peak vs off-peak hours?
>> Peak: 49,321, Off-Peak: 165,396

Assume we mean across services that run, not cancelled ones, regardless of stops, and across all dates. The question could be interpreted as 'typically' how many services run in peak hours. I assumed if a service departs OR arrives somewhere during peak hours it 

I noticed that some can overlap, which makes sense, as some may depart in peak hours, but continue to travel in off-peak hours, and vice versa, hence why total_services < peak + off_peak. I would want to analyse this by day, to see whether proportions change, and relative to cancellation rates.
*/

select
  count(*) as services_including_cancellations,
  sum(service_ran) as services_excluding_cancellations,
  sum(service_ran_during_peak_hours) as peak_services,
  sum(service_ran_during_off_peak_hours) as off_peak_services
from {{ ref('fact_service') }} fss

