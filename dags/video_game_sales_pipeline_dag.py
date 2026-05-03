import sys
import os
import logging
sys.path.append('/opt/airflow/dags/sales_data_pipeline')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract.load_data import load_data
from src.transform.transform_data import aggregate_data, clean_data
from src.load.save_data import save_to_db
from src.load.read_from_db import read_from_db
from src.validate.data_quality import validate_clean_data, validate_aggregated_data

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
    )
logger = logging.getLogger(__name__)



# def run_pipeline():
#     logging.basicConfig(
#     level=logging.INFO,
#     format='%(levelname)s: %(message)s'
#     )
#     logger = logging.getLogger(__name__)

#     logger.info("Starting pipeline")

#     logger.info("Loading raw data")
#     df = load_data()

#     logger.info("Cleaning data")
#     df_clean = clean_data(df)

#     logger.info("Validating cleaned data")
#     validate_clean_data(df_clean)

#     logger.info("Aggregating data")
#     df_agg = aggregate_data(df_clean)

#     logger.info("Validating aggregated data")
#     validate_aggregated_data(df_agg)

#     logger.info("Saving CSV files")
#     save_data(df_clean,'data/processed/clean_sales.csv')
#     save_data(df_agg, 'data/processed/aggregate_sales.csv')

#     logger.info("Saving data to PostgreSQL")
#     save_to_db(df=df_clean, table='games_clean')
#     save_to_db(df=df_agg, table='sales_by_platform_year')

#     logger.info("Pipeline completed successfully")

default_args = {
    "retries" : 1,
    "retry_delay": timedelta(minutes=2),
}

def load_clean_data():
    logger.info("Loading and cleaning data")
    df = load_data()
    df_clean = clean_data(df)
    save_to_db(df=df_clean, table='games_clean')

def validate_clean_table():
    logger.info("Reading and validating data")
    df_clean = read_from_db("games_clean")
    validate_clean_data(df_clean)

def aggregate_sales():
    logger.info("Reading and aggregating data")
    df_clean = read_from_db("games_clean")
    df_agg = aggregate_data(df_clean)
    save_to_db(df=df_agg, table="sales_by_platform_year")

def validate_aggregated_table():
    logger.info("Reading and validating aggregated data")
    df_agg = read_from_db("sales_by_platform_year")
    validate_aggregated_data(df_agg)




with DAG(
    dag_id='video_game_sales_pipeline',
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args= default_args
)as dag:
    
    task_1 = PythonOperator(
        task_id="load_clean_data",
        python_callable=load_clean_data
    )

    task_2 = PythonOperator(
        task_id="validate_clean_table",
        python_callable=validate_clean_table
    )

    task_3 = PythonOperator(
        task_id="aggregate_sales",
        python_callable=aggregate_sales
    )

    task_4 = PythonOperator(
        task_id="validate_aggregated_table",
        python_callable=validate_aggregated_table
    )

    task_1>>task_2>>task_3>>task_4