with source_data as (
    select *
    from read_parquet('/home/mona/PycharmProjects/PythonProject/taxi_data_silver/**/*.parquet')
)

select distinct
    trip_id,
    taxi_id,
    payment_type,
    company
from source_data