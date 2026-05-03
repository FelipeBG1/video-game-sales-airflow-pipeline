from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def show_message1():
    print("Mi primer mensaje y mi primer DAG")

def show_message2():
    print("Mi segundo mensaje y mi primer DAG")

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False
)as dag:
    
    task_1 = PythonOperator(
        task_id="show_message_1",
        python_callable=show_message1
    )

    task_2 = PythonOperator(
        task_id="show_message_2",
        python_callable=show_message2
    )

    task_1 >> task_2