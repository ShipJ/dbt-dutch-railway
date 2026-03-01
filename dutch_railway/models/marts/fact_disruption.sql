with fact_disruption as (

  /* fact_disruption
  Description: Fact table containing information about railway disruptions.
  Notes:
  - Assume we must have a start time and non-negative duration
  */

  select
    disruption_id,
    -- ns_lines,
    -- rdt_lines,
    -- rdt_lines_id,
    -- affected_station_names,
    affected_station_codes,
    coalesce(statistical_cause_en, cause_en) as disruption_cause, -- Prefer statistical cause if available (latest)
    cause_group as disruption_cause_group,
    disruption_start_time,
    disruption_end_time,
    duration_minutes
  from {{ ref('stg_disruptions') }}
  where 1=1
    and disruption_start_time <= current_date() -- Disruption must have started before today
    and disruption_start_time is not null -- Disruption must have a start time
    -- and disruption_end_time is not null -- Might want to allow for ongoing disruptions?
    and duration_minutes >= 0 -- Should not be negative (potentially seconds round to 0)
    and duration_minutes is not null -- Should not be null

)

select * from fact_disruption
