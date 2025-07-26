import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")

def extract(data):
    ''' Extract data '''
    df = pd.read_csv(data)
    logging.info('File was successfully extract')
    
    return df


def transform(df):
    ''' Transform data '''
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    logging.info('Timestamp was successfully change to datetime')

    df['day_of_week'] = df['timestamp'].dt.day_name()
    logging.info('Column with day was successfullyt created')

    df['hours_of_day'] = df['timestamp'].dt.hour
    logging.info('Column with time was successfully created')

    new_df = df.groupby('user_id')['amount'].agg(
        total_spent= 'sum',
        avg_transaction_value = 'mean',
        transaction_count = 'count'
    ).reset_index().sort_values('total_spent',ascending= False)
    
    return new_df


def load(new_df):
    '''Load data to db'''
    new_df.to_sql('user_stats', engine, if_exists='replace', index=False)
    logging.info(f"Loaded {len(new_df)} records into 'user_stats' table.")
    
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    df = extract("Data/data.csv")
    transformed_df = transform(df)
    load(transformed_df)