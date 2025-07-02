# NYC Taxi Data ETL Pipeline

## Technology Stack

- **Data source:** NYC Taxi data CSV file (yellow taxis), e.g. `yellow_tripdata_2023-01.csv`
- **Tools:**
  - Python
  - Pandas
  - boto3 (for downloading the file from S3)
  - PostgreSQL (or local database via Docker)
  - SQLAlchemy
  - AWS S3 (or MinIO as a local alternative)

## Data

The dataset can be downloaded from the official [NYC OpenData](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) or sources such as Kaggle: NYC Taxi Trip Data.

If you don’t have the file, let me know — I can generate a sample dataset for you.

## Task

1. Download the data
2. Load the data into Pandas and perform basic cleaning:
   - Remove duplicates.
   - Remove rows where `passenger_count = 0` or `trip_distance = 0`.
   - Convert `tpep_pickup_datetime` and `tpep_dropoff_datetime` columns to datetime type.
3. Save the cleaned data into a PostgreSQL database as a table named `trips_cleaned`.

---
