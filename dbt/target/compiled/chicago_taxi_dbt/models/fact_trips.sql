with source_data as (
    select *
    from read_parquet('/home/mona/PycharmProjects/PythonProject/taxi_data_silver/**/*.parquet')
)

select
    trip_id,
    md5(cast(coalesce(cast(pickup_community_area as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_census_tract as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_community_area as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_census_tract as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_key,
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