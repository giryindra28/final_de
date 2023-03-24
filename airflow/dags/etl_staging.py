from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pandas as pd

from datetime import datetime
from datetime import timedelta

from airflow.utils.dates import days_ago

from script import main
from script import load_stagging

with DAG(dag_id="etl_to_staging",
    # start_date=datetime.datetime(2022, 8, 14),
    start_date = days_ago(1),
    schedule_interval=None,
    catchup=False,) as dag:

    task_mulai_etl = PythonOperator(
        task_id='Start_Proses_ETL_Staging',
        python_callable=main.start
    )

    task_transform_csv = PythonOperator(
        task_id='trans_csv', 
        python_callable=main.transform_csv)

    task_transform_business_json = PythonOperator(
        task_id='trans_business_json', 
        python_callable=main.transform_business_json)
    
    task_transform_checkin_json = PythonOperator(
        task_id='trans_checkin_json', 
        python_callable=main.transform_checkin_json)
    
    task_load_postgres = PythonOperator(
        task_id='load_postgres', 
        python_callable=load_stagging.load_postgres)

    task_sukses_etl = PythonOperator(
        task_id='sukses_Proses_ETL',
        python_callable=main.success
    )
    
task_mulai_etl >> task_transform_csv >> task_transform_business_json >> task_transform_checkin_json >>  task_load_postgres >> task_sukses_etl