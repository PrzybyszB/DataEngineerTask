import pandas as pd
import boto3

# Create dataframe
def extract_data(parquet_path: str,output_path: str,):
    df = pd.read_parquet(parquet_path, engine='pyarrow')
    df.to_parquet(output_path)
    print(f"Extracted data saved to {output_path}")
    return df


# Cleaning section
def transform_data(input_path: str, output_path: str):
    df = pd.read_parquet(input_path, engine='pyarrow')
    before = len(df)
    df = df.drop_duplicates()
    after  = len(df)

    removed = before - after
    print(f"Duplicated file with number of {removed} was deleted")


    df = df[df['passenger_count'] != 0]
    df = df[df['trip_distance'] != 0]
    print(f"Data was clenead from rows where passenger_count = 0 and trip_distance = 0")

    # Change datatype
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    print(f"Tpep_pickup_datetime was successfully change to datetime")

    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(f"Tpep_dropoff_datetime was successfully change to datetime")

    # Create duration column to future avg distance
    df['duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    print(f"Creating durations times in taxi")

    # Some aggregate functions
    df_agg = df.groupby(['tpep_pickup_datetime', 'passenger_count']).agg(
        trip_count = ('tpep_pickup_datetime','count'),
        avg_distance = ('trip_distance', 'mean'),
        avg_duration_minutes = ('duration', 'mean' )  
    ).reset_index()
    print(f"Trip_count, avg distance and avg duration minutes success")

    df_agg.to_parquet(output_path)
    print(f"Transformed data saved to {output_path}")
    return df_agg

# Save to parquet

# Load into AWS s3

def load_to_s3(file_path: str, bucket_name: str, s3_key: str):

    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)
    print("Upload completed.")
