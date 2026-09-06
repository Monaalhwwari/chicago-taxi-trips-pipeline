from os import truncate

from pyspark.sql.functions import col, concat_ws, md5, coalesce, lit
from pyspark.sql.types import IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ChicagoTaxiDataPipeline") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()


def ingestion():
    file_path = "Taxi_Trips_-_2024_20240408.csv"

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
        "tolls", "extras", "trip_total", "payment_type", "company",
        "pickup_centroid_latitude", "pickup_centroid_longitude",
        "dropoff_centroid_latitude", "dropoff_centroid_longitude"
    ]
    existing_cols = [c for c in wanted_cols if c in df.columns]
    bronze_df = df.select(*existing_cols)
    bronze_df.write.mode("overwrite").parquet("./taxi_data_bronze")

    return bronze_df

df_bronze = ingestion()


def transformation(input_df):
    Df_Cleaned = input_df.drop_duplicates().dropna(subset=['trip_id'])

    str_cols = [
        "trip_id", "taxi_id", "pickup_census_tract", "dropoff_census_tract",
        "payment_type", "company", "pickup_centroid_location", "dropoff_centroid_location"
    ]
    int_cols = ["trip_seconds", "pickup_community_area", "dropoff_community_area"]
    double_cols = [
        "trip_miles", "fare", "tips", "tolls", "extras", "trip_total",
        "pickup_centroid_latitude", "pickup_centroid_longitude",
        "dropoff_centroid_latitude", "dropoff_centroid_longitude"
    ]
    timestamp_cols = ["trip_start_timestamp", "trip_end_timestamp"]

    for col_name in str_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(StringType()))

    for col_name in int_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(IntegerType()))

    for col_name in double_cols:
        if col_name in Df_Cleaned.columns:
            Df_Cleaned = Df_Cleaned.withColumn(col_name, col(col_name).cast(DoubleType()))


    Df_Cleaned = Df_Cleaned \
        .withColumn("trip_start_timestamp", F.to_timestamp(F.col("trip_start_timestamp"), "MM/dd/yyyy hh:mm:ss a")) \
        .withColumn("trip_end_timestamp", F.to_timestamp(F.col("trip_end_timestamp"), "MM/dd/yyyy hh:mm:ss a"))

    means = Df_Cleaned.select([avg(c).alias(c)for c in int_cols]).first().asDict()

    fill_dict = {
        "payment_type": "UNKNOWN",
        "company": "UNKNOWN",
        "pickup_census_tract": "UNKNOWN",
        "dropoff_census_tract": "UNKNOWN",
    }
    fill_dict.update(means)
    Df_Cleaned = Df_Cleaned.fillna(fill_dict)
    Df_Cleaned.write.mode("overwrite").parquet("./taxi_data_silver")
    cols_unique = (Df_Cleaned.count()==Df_Cleaned.select("taxi_id").distinct().count())
    print(cols_unique)
    return Df_Cleaned

df_Silver = transformation(df_bronze)

print(df_Silver.show(5,truncate=False))

def load(silver_df):
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
        "trip_id", "taxi_id","trip_seconds", "trip_miles", "fare", "tips","trip_total","trip_start_timestamp",
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



    dim_trips.write.mode("overwrite").parquet("./data/gold/dim_trips")
    dim_location.write.mode("overwrite").parquet("./data/gold/dim_location")
    fact_trips.write.mode("overwrite").parquet("./data/gold/fact_trips")

    return silver_df


load(df_Silver)

spark.read.parquet("./data/gold/fact_trips").createOrReplaceTempView("fact_trips")
spark.read.parquet("./data/gold/dim_trips").createOrReplaceTempView("dim_trips")
spark.read.parquet("./data/gold/dim_location").createOrReplaceTempView("dim_location")