from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator 
from Task_2.dag import extract_data, transform_data, load_to_s3
import os
default_args = {
    'owner':'airflow',
    'retries':5,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30)
}

with DAG(
    default_args=default_args,
    dag_id = 'task_2_dagv10',
    schedule_interval = '@daily',
     start_date= datetime.today()
) as dag:

    extract_task= PythonOperator (
        task_id = 'extract',
        python_callable = extract_data,
        op_kwargs = {
            'parquet_path': 'Task_2/Data/yellow_tripdata_2023-01.parquet',
            'output_path': 'Task_2/Data/extracted.parquet'
    }
    )

    transform_task= PythonOperator (
            task_id = 'transform',
            python_callable = transform_data,
            op_kwargs={
                'input_path': 'Task_2/Data/extracted.parquet',
                'output_path': 'Task_2/Data/transformed.parquet'
    }
        )

    load_task= PythonOperator (
            task_id = 'load',
            python_callable = load_to_s3,
            op_kwargs={
                'file_path': 'Task_2/Data/transformed.parquet',
                'bucket_name': os.getenv("BUCKET_NAME"),
                's3_key': os.path.join(os.getenv("S3_RAW_PREFIX"), "yellow_tripdata_2023-01.parquet")
            }
    )
    extract_task >> transform_task >> load_task