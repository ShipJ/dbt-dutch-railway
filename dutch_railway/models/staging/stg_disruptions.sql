select
    rdt_id as disruption_id,
    ns_lines,
    rdt_lines,
    rdt_lines_id,
    rdt_station_names as affected_station_names,
    rdt_station_codes as affected_station_codes,
    
    -- Cause information
    cause_nl,
    cause_en,
    statistical_cause_nl,
    statistical_cause_en,
    cause_group,
    
    -- Timing information
    start_time as disruption_start_time,
    end_time as disruption_end_time,
    duration_minutes

from {{ source('railway', 'disruptions') }}
