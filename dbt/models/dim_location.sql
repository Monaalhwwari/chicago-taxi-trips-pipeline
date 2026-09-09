with source_data as (
    select *
    from read_parquet('/home/mona/PycharmProjects/PythonProject/taxi_data_silver/**/*.parquet')
)

select distinct
    {{ dbt_utils.generate_surrogate_key([
        'pickup_community_area',
        'pickup_census_tract',
        'dropoff_community_area',
        'dropoff_census_tract'
    ]) }} as location_key,
    pickup_community_area,
    pickup_census_tract,
    dropoff_community_area,
    dropoff_census_tract
from source_data