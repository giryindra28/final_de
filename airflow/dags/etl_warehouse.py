from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pandas as pd

from datetime import datetime
from datetime import timedelta

from airflow.utils.dates import days_ago

from script import main_dw
from script import main

with DAG(dag_id="load_to_DWlayer",
    # start_date=datetime.datetime(2022, 8, 14),
    start_date = days_ago(1),
    schedule_interval=None,
    catchup=False,) as dag:

    task_mulai_etl = PythonOperator(
        task_id='Start_Proses_Load_to_datawarehouse',
        python_callable=main.start
    )

    task_load_csv = PythonOperator(
        task_id='load_to_dw', 
        python_callable=main_dw.load_dw)

    task_sukses_etl = PythonOperator(
        task_id='sukses_Proses_Load_ToDW',
        python_callable=main.success
    )
    
task_mulai_etl >> task_load_csv >> task_sukses_etl