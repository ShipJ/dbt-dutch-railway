/* Q6 - Which train routes have the most services per day split by service type?
>> Amsterdam Sloterdijk -> Amsterdam Zuid has ~162 Metro ipv services per day

Assume by the question it means origin -> destination pairs, not each possible route. I have modeled for this, but answered the more obvious question. It would be simple to add 'route' to the group by below.

Also assume it means highest daily average number of services over the month of data we have.
*/

with daily_services as (
    
    select
        b.service_date,
        b.service_type,
        -- create user-friendly name
        dod.origin_station_name_long || ' -> ' || dod.destination_station_name_long as origin_destination_name,
        sum(service_ran) as services_per_day 
    from {{ ref('fact_service') }} b
    inner join {{ ref('dim_origin_destination') }} dod 
        on dod.origin_destination_key = b.origin_destination_key
    group by 1, 2, 3
)

select
    origin_destination_name,
    service_type,
    round(avg(services_per_day), 0) as avg_daily_services
from daily_services 
group by 1, 2
order by 3 desc