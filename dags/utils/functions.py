import pandas as pd
import csv
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(CUR_DIR)),"dags/data/temp")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(CUR_DIR)),"dags/data/csv")
PARQUET_DIR = os.path.join(os.path.dirname(os.path.dirname(CUR_DIR)),"dags/data/parquet")

def getInputs(**context):
    params = context['params']
    
    if 'search' not in params:
        raise ValueError('Required parameters [search] not found in DAG conf. Optional parameters [sort_by, quantity]')

    search = params['search']
    sort_by = params.get('sort_by', 'hottest')
    quantity = params.get('quantity', 1)

    if not isinstance(quantity, int) or quantity < 0:
        raise ValueError('Invalid value for quantity. Expected a non-negative integer.')
    
    if sort_by not in {'hottest', 'votes', 'updated', 'active'}:
        raise ValueError('Invalid value for sort_by. Expected valid options: ''hottest'', ''votes'', ''updated'', and ''active''')
    
    context['ti'].xcom_push(key='search', value=search)
    context['ti'].xcom_push(key='sort_by', value=sort_by)
    context['ti'].xcom_push(key='quantity', value=quantity)

def print_params(**context):

    search = context['ti'].xcom_pull(key='search')
    sort_by = context['ti'].xcom_pull(key='sort_by')
    quantity = context['ti'].xcom_pull(key='quantity')
    
    print(f'search: {search}')
    print(f'sort_by: {sort_by}')
    print(f'quantity: {quantity}')

def getFiles(**context):

    kaggle_conn = BaseHook.get_connection('kaggle_conn')

    os.environ['KAGGLE_USERNAME'] = kaggle_conn.login
    os.environ['KAGGLE_KEY'] = kaggle_conn.password

    try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
            api.authenticate()

    except:
            raise AirflowException("Invalid Kaggle Credentials.")

    search = context['ti'].xcom_pull(key='search')
    sort_by = context['ti'].xcom_pull(key='sort_by')
    quantity = context['ti'].xcom_pull(key='quantity')

    datasets = api.dataset_list(search=search, sort_by=sort_by)
    count = 0

    for dataset in datasets:

        if count >= quantity:
            break

        api.dataset_download_files(dataset.ref, path= DATA_DIR, unzip = True, force = True)

        count += 1

def partionateFiles(ds):

    df = pd.read_csv(DATA_DIR + '/movies.csv')

    filtered_df = df[df['release_date'] == ds]
    filtered_df['inserted_at'] = datetime.now()

    output_directory = DATA_DIR
    output_file = os.path.join(output_directory, f"movies_{ds}.csv")

    if os.path.exists(output_file):
        os.remove(output_file)

    filtered_df.to_csv(output_file, index=False)

def pushToDatabase(ds):
    postgres_conn_id = "postgres_localhost"

    output_directory = DATA_DIR
    csv_file = os.path.join(output_directory, f"movies_{ds}.csv")

    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        for row in csv_data:
           
            pg_hook.insert_rows(
                table="movies",
                rows=[row],  # Inserting one row at a time
                replace_index="id",
                replace = True,
                target_fields=[
                    "id", "title", "genres", "original_language", "overview", 
                    "popularity", "production_companies", "release_date", "budget", 
                    "revenue", "runtime", "status", "tagline", "vote_average", 
                    "vote_count", "credits", "keywords", "poster_path", 
                    "backdrop_path", "recommendations", "inserted_at"
                ]
            )
        

def pushToRDS(ds):
    postgres_conn_id = "rds_conn"

    output_directory = DATA_DIR
    csv_file = os.path.join(output_directory, f"movies_{ds}.csv")

    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        for row in csv_data:
     
            pg_hook.insert_rows(
                table="movies",
                replace_index="id",
                replace = True,
                rows=[row],
                target_fields=[
                    "id", "title", "genres", "original_language", "overview", 
                    "popularity", "production_companies", "release_date", "budget", 
                    "revenue", "runtime", "status", "tagline", "vote_average", 
                    "vote_count", "credits", "keywords", "poster_path", 
                    "backdrop_path", "recommendations", "inserted_at"
                ]
            )
            
def processData(ds):
    df = pd.read_csv(DATA_DIR + f"/movies_{ds}.csv")
    
    df['genres'] = df['genres'].astype(str)
    
    genre_df = df['genres'].str.split("-", expand=True).stack().dropna().reset_index(level=1, drop=True).rename('genre')
    merged_df = df.drop('genres', axis=1).merge(genre_df, how='outer', left_index=True, right_index=True)
    
    output_directory = PARQUET_DIR
    output_file = os.path.join(output_directory, f'processed_movies_{ds}.parquet')

    merged_df.to_parquet(output_file, index=False)

def rawDataToS3(ds, year, month, day):
    output_directory = DATA_DIR

    prefix = f"raw/{year}/{month}/{day}/"

    s3_key = prefix + f"movies_{ds}.csv"

    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=os.path.join(output_directory, f"movies_{ds}.csv"),
        key=s3_key,
        bucket_name="kaggle-movies",
        replace=True
    )

def processedDataToS3(ds, year, month, day):
    output_directory = PARQUET_DIR

    prefix = f"processed/{year}/{month}/{day}/"

    s3_key = prefix + f'processed_movies_{ds}.parquet'

    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=os.path.join(output_directory, f'processed_movies_{ds}.parquet'),
        key=s3_key,
        bucket_name="kaggle-movies",
        replace=True
    )

def removeFiles(file_path):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension in ['.csv', '.parquet']:
        os.remove(file_path)

def removeCsvFiles():
    csv_dir = DATA_DIR
    file_list = os.listdir(csv_dir)
    for file_name in file_list:
        file_path = os.path.join(csv_dir, file_name)
        removeFiles(file_path)

def removeParquetFiles():
    parquet_dir = PARQUET_DIR
    file_list = os.listdir(parquet_dir)
    for file_name in file_list:
        file_path = os.path.join(parquet_dir, file_name)
        removeFiles(file_path)