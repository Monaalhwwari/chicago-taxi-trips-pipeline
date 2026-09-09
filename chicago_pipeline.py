import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, coalesce, col, concat_ws, lit, md5
from pyspark.sql.types import DoubleType, IntegerType, StringType
import sys
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dbt import *
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_spark_session():
    return SparkSession.builder \
        .appName("ChicagoTaxiDataPipeline") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def ingestion():
    spark = get_spark_session()
    file_path = os.path.join(BASE_DIR, "Taxi_Trips_-_2024_20240408.csv")

    raw_df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    renamed_cols = [col(c).alias(c.strip().lower().replace(" ", "_")) for c in raw_df.columns]
    df = raw_df.select(renamed_cols)

    wanted_cols = [
        "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
        "trip_seconds", "trip_miles", "pickup_census_tract", "dropoff_census_tract",
        "pickup_community_area", "dropoff_community_area", "fare", "tips",
        "tolls", "extras", "trip_total", "payment_type", "company"
    ]
    existing_cols = [c for c in wanted_cols if c in df.columns]
    bronze_df = df.select(*existing_cols)

    bronze_df.write.mode("overwrite").parquet(os.path.join(BASE_DIR, "taxi_data_bronze"))

    spark.stop()


def transformation():
    spark = get_spark_session()
    # Fixed to Absolute Path
    input_df = spark.read.parquet(os.path.join(BASE_DIR, "taxi_data_bronze"))
    Df_Cleaned = input_df.drop_duplicates().dropna(subset=['trip_id'])

    str_cols = [
        "trip_id", "taxi_id", "pickup_census_tract", "dropoff_census_tract",
        "payment_type", "company", "pickup_centroid_location", "dropoff_centroid_location"
    ]
    int_cols = ["trip_seconds", "pickup_community_area", "dropoff_community_area"]
    double_cols = [
        "trip_miles", "fare", "tips", "tolls", "extras", "trip_total"
    ]

    for col_name in str_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(StringType()))

    for col_name in int_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(IntegerType()))

    for col_name in double_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(DoubleType()))

    Df_Cleaned = (Df_Cleaned
                  .withColumn("trip_start_timestamp", F.to_timestamp(F.col("trip_start_timestamp"), "MM/dd/yyyy hh:mm:ss a")) \
        .withColumn("trip_end_timestamp", F.to_timestamp(F.col("trip_end_timestamp"), "MM/dd/yyyy hh:mm:ss a")))

    means = Df_Cleaned.select([avg(c).alias(c) for c in int_cols]).first().asDict()

    fill_dict = {
        "payment_type": "UNKNOWN",
        "company": "UNKNOWN",
        "pickup_census_tract": "UNKNOWN",
        "dropoff_census_tract": "UNKNOWN",
    }
    fill_dict.update(means)
    Df_Cleaned = Df_Cleaned.fillna(fill_dict)

    Df_Cleaned.write.mode("overwrite").parquet(os.path.join(BASE_DIR, "taxi_data_silver"))
    spark.stop()


def load():
    spark = get_spark_session()
    silver_df = spark.read.parquet(os.path.join(BASE_DIR, "taxi_data_silver"))

    dim_location = silver_df.select(
        "pickup_community_area",
        "pickup_census_tract",
        "dropoff_community_area",
        "dropoff_census_tract"
    ).dropDuplicates() \
        .withColumn(
        "location_key",
        md5(concat_ws("||",
                      coalesce(col("pickup_community_area"), lit(-1)),
                      coalesce(col("pickup_census_tract"), lit("UNKNOWN")),
                      coalesce(col("dropoff_community_area"), lit(-1)),
                      coalesce(col("dropoff_census_tract"), lit("UNKNOWN"))
                      ))
    )

    dim_trips = silver_df.select(
        "trip_id", "taxi_id", "trip_seconds", "trip_miles", "fare", "tips", "trip_total", "trip_start_timestamp",
        "trip_end_timestamp"
    )

    fact_trips = silver_df \
        .withColumn(
        "location",
        md5(concat_ws("||",
                      coalesce(col("pickup_community_area"), lit(-1)),
                      coalesce(col("pickup_census_tract"), lit("UNKNOWN")),
                      coalesce(col("dropoff_community_area"), lit(-1)),
                      coalesce(col("dropoff_census_tract"), lit("UNKNOWN"))
                      ))).select(
        "trip_id", "taxi_id",
        "location",
        "trip_start_timestamp", "trip_end_timestamp",
        "trip_seconds", "trip_miles", "fare", "tips",
        "tolls", "extras", "trip_total", "payment_type"
    ).dropDuplicates(["trip_id"])

    dim_trips.write.mode("overwrite").parquet(os.path.join(BASE_DIR, "data", "gold", "dim_trips.sql"))
    dim_location.write.mode("overwrite").parquet(os.path.join(BASE_DIR, "data", "gold", "dim_location"))
    fact_trips.write.mode("overwrite").parquet(os.path.join(BASE_DIR, "data", "gold", "fact_trips"))
    spark.stop()


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Chicago_Taxi_Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 7),
    schedule="@once",
    catchup=False,
) as Chicago_Taxi_Pipeline:

    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=ingestion,
    )

    transformation_task = PythonOperator(
        task_id="transformation",
        python_callable=transformation,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    ingestion_task >> transformation_task >> load_task
