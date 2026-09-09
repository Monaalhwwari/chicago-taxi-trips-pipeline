with source_data as (
    select *
    from read_parquet('/home/mona/PycharmProjects/PythonProject/taxi_data_silver/**/*.parquet')
)

select distinct
    md5(cast(coalesce(cast(pickup_community_area as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_census_tract as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_community_area as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_census_tract as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_key,
    pickup_community_area,
    pickup_census_tract,
    dropoff_community_area,
    dropoff_census_tract
from source_data