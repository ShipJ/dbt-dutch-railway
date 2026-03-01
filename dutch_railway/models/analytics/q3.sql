/* Q3 - What is the average distance between connected stations?
>> 138.17km

Initially I thought this was wrong because intuitively I thought the distance between ADJACENT stations is small, especially in cities. But that's not what is being averaged, which is the thousands of pairs of any two stations, that could be anywhere in the country and so the larger number starts to make more sense.

Assume by 'connected' we mean there is a service between the two stations, not that they are adjacent. I noticed that all pairs of stations do exist, and the distance between them is symmetric (i.e A -> B = B -> A).
*/

select
  round(avg(fsd.distance_km), 2) as avg_distance_km
from {{ ref('fact_station_distance') }} fsd