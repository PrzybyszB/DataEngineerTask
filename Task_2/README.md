# NYC Taxi Data Aggregation Pipeline

This project implements a simple ETL (Extract, Transform, Load) pipeline for NYC Taxi data. The goal is to aggregate ride data by day and passenger count, save the result as a Parquet file, and upload it to S3 or a local MinIO instance.

## 📌 Processing Steps

### 1. Data Aggregation (using Pandas)

The source data is processed using **Pandas**. During aggregation:

- The data is grouped by:
  - `pickup_date` (the date of the ride),
  - `passenger_count`.

- For each group, the following metrics are calculated:
  - `trip_count`: number of rides (`count`),
  - `avg_distance`: average trip distance (`mean(trip_distance)`),
  - `avg_duration_minutes`: average trip duration in minutes (`mean(dropoff - pickup)`).

#### Sample output:
| pickup_date | passenger_count | trip_count | avg_distance | avg_duration_minutes |
|-------------|------------------|------------|---------------|------------------------|


---

### 2. Save as Parquet

The aggregated result is saved as a local Parquet file:

### 3. Upload to S3

## ⚙️ Airflow DAG

To automate the pipeline, an Airflow DAG is created that:

- Runs **daily**
- Performs the full pipeline:
  1. Read input data,
  2. Clean the data (e.g., convert datetime, filter outliers),
  3. Aggregate using Pandas,
  4. Save to `.parquet` file,
  5. Upload to S3 or MinIO.