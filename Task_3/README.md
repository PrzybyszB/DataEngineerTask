# 🔄 Pipeline #3 – Spark + EMR Serverless + Athena

## 🎯 Objective  
Build a data pipeline using the following AWS components:  

- **EMR Serverless (Spark)**: Process large NYC Taxi datasets (yellow and green taxis).  
- **Glue Data Catalog**: Catalog the processed data.  
- **Athena**: Query and validate results; perform aggregations and enrich data with geographic information.  

This setup simulates a real-world cloud architecture used in production.

---

## 🗄 Input Data  
- Raw Parquet files with NYC Yellow and Green Taxi data (e.g., January 2023).  
- Additional lookup data: `taxi_zone_lookup.csv` (mapping PULocationID and DOLocationID to zone names).

---

## 📌 Pipeline Steps

### 1. Upload to S3  
- Upload raw Parquet files and the `taxi_zone_lookup.csv` file to S3

### 2. Spark Job (EMR Serverless)  
- Read data from S3.  
- Enrich the data by joining pickup and dropoff location IDs with the lookup zones.  
- Calculate daily aggregations:  
  - Number of trips and average distance/duration per `(pickup_zone, dropoff_zone)` combination.  
  - Taxi type (yellow or green).  
- Write the output as Parquet files to `s3://<your-bucket>/processed/`.

### 3. Glue Crawler / Catalog  
- Create a Glue crawler to catalog the processed data, partitioned (e.g., by date).

### 4. Athena Validation  
- Create Athena tables from the Glue catalog.  
- Run queries to validate the data, such as:  
  - Confirm that the number of trips is greater than zero.  
  - Get statistics like the top 10 zone pairs with the highest trip counts.