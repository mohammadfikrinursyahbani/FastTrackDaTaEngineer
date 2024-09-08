from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def fun_total_film_get_data(**kwargs):
    pass

def fun_total_film_load_data(**kwargs):
    pass

project_folder = '/home/de-fikri/de-series/mini_project_3/scripts'
python_path = '/home/de-fikri/de-series/mini_project_3/scripts/venv/bin/python'

with DAG(
    dag_id='d_1_batch_processing_polars',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_top_countries_get_data = BashOperator(
        task_id='top_countries_get_data',
        bash_command=f'{python_path} {project_folder}/get_data.py'
    )

    op_top_countries_load_data = BashOperator(
        task_id='top_countries_load_data',
        bash_command=f'{python_path} {project_folder}/load_data.py'
    )

    op_total_film_get_data = PythonOperator(
        task_id='total_film_get_data',
        python_callable=fun_total_film_get_data
    )

    op_total_film_load_data = PythonOperator(
        task_id='total_film_load_data',
        python_callable=fun_total_film_load_data
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> op_top_countries_get_data >> op_top_countries_load_data >> end_task
    start_task >> op_total_film_get_data >> op_total_film_load_data >> end_task