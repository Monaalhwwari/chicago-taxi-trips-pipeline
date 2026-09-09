# Chicago Taxi ELT Pipeline

An ELT data pipeline for Chicago Taxi Trips data using **PySpark**, **dbt**, and **DuckDB**. 

The project processes raw trip data through Medallion Architecture layers (Bronze -> Silver -> Gold) to prepare clean dimensional models for analytics.

---

##  Architecture & Data Flow

1. **Ingestion & Processing (PySpark)**
   - Extract raw CSV data.
   - Clean, format timestamps, and partition data into Parquet files.
   - Store processed data in Silver layer (`taxi_data_silver/`).

2. **Data Transformation & Modeling (dbt + DuckDB)**
   - Load Silver Parquet files directly using DuckDB's `read_parquet`.
   - Build Gold layer dimensional models in DuckDB:
     - `dim_location`: Locations surrogate keys & census boundaries.
     - `dim_trips`: Trip details, payment types, and companies.
     - `fact_trips`: Trip metrics (fare, duration, distance, tips).

---

##  Project Structure

```
.
├── chicago_pipeline.py     # PySpark processing script (Bronze -> Silver)
├── dbt/                    # dbt project directory
│   ├── models/             # Gold layer models & SQL transformations
│   │   ├── dim_location.sql
│   │   ├── dim_trips.sql
│   │   ├── fact_trips.sql
│   │   └── schema.yml      # Data tests & documentation
│   ├── dbt_project.yml     # dbt project configuration
│   └── profiles.yml        # DuckDB connection details
└── taxi_data_silver/       # Cleaned Parquet files (Git ignored)

How to Run
Prerequisites

    Python 3.10+

    PySpark

    dbt-duckdb

Step 1: Run Ingestion Pipeline

Execute the PySpark script to generate Silver Parquet data:
Bash

python chicago_pipeline.py

Step 2: Run Transformations & Tests

Navigate to the dbt directory, build the tables, and run testing constraints:
Bash

cd dbt
dbt run
dbt test

 Data Quality

Quality checks are managed via dbt/models/schema.yml including:

    unique & not_null constraints on primary keys (trip_id, location_key).

    relationships tests to maintain referential integrity between Fact and Dimension tables.
