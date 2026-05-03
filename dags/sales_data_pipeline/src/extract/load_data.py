import pandas as pd

def load_data():
    CSV_PATH = "/opt/airflow/dags/sales_data_pipeline/data/raw/vgsales.csv"
    df = pd.read_csv(CSV_PATH)
    return df