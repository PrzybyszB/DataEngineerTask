# NYC Taxi Data Processing (PySpark)

## Extract
- Load data into PySpark:
  - CSV or Parquet format — whichever is more convenient.
  - Ensure the dataset contains columns like `pickup_datetime`, `dropoff_datetime`, `trip_distance`, `fare_amount`, `passenger_count`.

---

## Transform
1. **Parse datetime columns**  
   - Parse `pickup_datetime` and `dropoff_datetime` into `timestamp` type.

2. **Add derived columns**  
   - `trip_duration_minutes` = difference between `dropoff_datetime` and `pickup_datetime` in minutes  
   - `pickup_hour` = hour when the trip started  
   - `day_of_week` = day of the week as a string (e.g. `"Monday"`)

3. **Filter trips**  
   Remove rows where:  
   - trip duration is less than 1 minute  
   - trip distance ≤ 0.5 miles  
   - fare amount (`fare_amount`) is negative

4. **Calculate aggregations**  
   - Average trip duration and average distance per day of the week  
   - Top 5 hours of the day with the highest average number of passengers

---

## Load
- Save the transformed results in **Parquet** format:  
  - Partition data by `day_of_week`  
  - Output directory: `output/nyc_taxi/`

- (Optional) Upload to S3 bucket:  