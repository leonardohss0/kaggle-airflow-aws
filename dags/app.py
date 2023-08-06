from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import os

from utils.functions import partionateFiles, getInputs, getFiles, pushToDatabase, rawDataToS3, pushToRDS, processData, processedDataToS3, removeCsvFiles, removeParquetFiles
from resources.config import QUERY_CREATE_TABLE

START_DATE = datetime.now() - timedelta(days=2)

default_args = {
    'owner': 'leonardo.sanches',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    
    dag_id='kaggle_data_pipeline',
    description='Orchestrating a data pipeline to store Kaggle data as a .csv file',
    start_date= START_DATE,
    schedule_interval= "@daily",
    catchup= True,
    default_args=default_args

) as dag:
    
    year = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}"
    month = "{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}"
    day = "{{ macros.ds_format(ds, '%Y-%m-%d', '%d') }}"
    
    getInputs = PythonOperator(
        task_id= 'getInputs',
        python_callable= getInputs,
        provide_context=True,
        params = {
            'search': 'akshaypawar7/millions-of-movies'
        }
    )

    getFiles = PythonOperator(
        task_id= 'getFiles',
        python_callable= getFiles,
        provide_context=True
    )

    partionateFiles = PythonOperator(
        task_id = 'partionateFiles',
        python_callable= partionateFiles
    )

    rawDataToS3 = PythonOperator(
        task_id="pushToLake",
        python_callable=rawDataToS3,
        op_kwargs={'year': year, 'month': month, 'day': day}
    )

    createTablePostgres = PostgresOperator(
        task_id ="createTablePostgres",
        postgres_conn_id= "postgres_localhost",
        sql = QUERY_CREATE_TABLE
    )

    pushToDatabase = PythonOperator(
        task_id="pushToDatabase",
        python_callable= pushToDatabase
    )

    createTableRDS = PostgresOperator(
        task_id ="createTableRDS",
        postgres_conn_id= "rds_conn",
        sql = QUERY_CREATE_TABLE
    )

    pushToRDS = PythonOperator(
        task_id="pushToRDS",
        python_callable= pushToRDS
    )

    processData = PythonOperator(
        task_id="processData",
        python_callable=processData
    )

    processedDataToS3 = PythonOperator(
        task_id="processedDataToS3",
        python_callable=processedDataToS3,
        op_kwargs={'year': year, 'month': month, 'day': day}

    )

    removeCsvFiles = PythonOperator(
        task_id='removeCsvFiles',
        python_callable=removeCsvFiles,
    )

    removeParquetFiles = PythonOperator(
        task_id='removeParquetFiles',
        python_callable=removeParquetFiles,
    )

    getInputs >> getFiles >> partionateFiles >> [ createTablePostgres, createTableRDS ]

    partionateFiles >> rawDataToS3 >> processData >> processedDataToS3 

    createTablePostgres >> pushToDatabase 
    createTableRDS >> pushToRDS 

    [processedDataToS3, pushToDatabase, pushToRDS] >> removeCsvFiles >> removeParquetFiles