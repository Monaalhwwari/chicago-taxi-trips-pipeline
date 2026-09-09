
    
    

select
    trip_id as unique_field,
    count(*) as n_records

from "chicago_taxi"."gold"."fact_trips"
where trip_id is not null
group by trip_id
having count(*) > 1


