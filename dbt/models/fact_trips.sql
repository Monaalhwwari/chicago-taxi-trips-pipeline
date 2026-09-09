with source_data as (
    select *
    from read_parquet('/home/mona/PycharmProjects/PythonProject/taxi_data_silver/**/*.parquet')
)

select
    trip_id,
    {{ dbt_utils.generate_surrogate_key([
        'pickup_community_area',
        'pickup_census_tract',
        'dropoff_community_area',
        'dropoff_census_tract'
    ]) }} as location_key,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_seconds,
    trip_miles,
    fare,
    tips,
    tolls,
    extras,
    trip_total
from source_data