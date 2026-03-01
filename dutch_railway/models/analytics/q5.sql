/* Q5 - What are the top 5 most common causes of service disruptions?
1. rolling stock > broken down train
2. infrastructure > points failure
3. accidents > collision
4. infrastructure > defective railway track
5. unknown > technical investigation (I noticed these can get updated at a later date)

I noticed when looking at the cause group, 'external' was higher on the list, with fewer but more specific causes

*/

select
  disruption_cause_group,
  disruption_cause,
  count(*) as disruptions
from {{ ref('fact_disruption') }}
group by 1, 2
order by 3 desc

