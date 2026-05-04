from datetime import datetime, timedelta
import logging
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags/sales_data_pipeline")

from src.extract.load_data import load_data
from src.transform.transform_data import clean_data, aggregate_data
from src.load.save_data import save_to_db
from src.load.read_from_db import read_from_db
from src.validate.data_quality import validate_clean_data, validate_aggregated_data


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

logger = logging.getLogger(__name__)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def load_clean_data():
    logger.info("Starting data ingestion from CSV")
    df = load_data()

    logger.info("Cleaning raw dataset")
    df_clean = clean_data(df)

    logger.info("Saving cleaned data to table 'games_clean'")
    save_to_db(df=df_clean, table='games_clean')

    logger.info("Load and clean step completed successfully")


def validate_clean_table():
    logger.info("Reading cleaned data from table 'games_clean'")
    df_clean = read_from_db("games_clean")

    logger.info("Running data quality checks on cleaned data")
    validate_clean_data(df_clean)

    logger.info("Clean data validation completed successfully")


def aggregate_sales():
    logger.info("Reading cleaned data from table 'games_clean'")
    df_clean = read_from_db("games_clean")

    logger.info("Aggregating sales by platform and year")
    df_agg = aggregate_data(df_clean)

    logger.info("Saving aggregated data to table 'sales_by_platform_year'")
    save_to_db(df=df_agg, table="sales_by_platform_year")

    logger.info("Aggregation step completed successfully")


def validate_aggregated_table():
    logger.info("Reading aggregated data from table 'sales_by_platform_year'")
    df_agg = read_from_db("sales_by_platform_year")

    logger.info("Running data quality checks on aggregated data")
    validate_aggregated_data(df_agg)

    logger.info("Aggregated data validation completed successfully")



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