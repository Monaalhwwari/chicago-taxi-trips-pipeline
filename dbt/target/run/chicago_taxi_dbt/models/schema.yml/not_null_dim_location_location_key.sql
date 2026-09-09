
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_key
from "chicago_taxi"."gold"."dim_location"
where location_key is null



  
  
      
    ) dbt_internal_test