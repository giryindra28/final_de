from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pandas as pd
import json
from datetime import datetime
from datetime import timedelta

from airflow.utils.dates import days_ago


def transform_csv():
    # get data csv file
    data_las_vegas = pd.read_csv('/opt/airflow/dataset/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv')
    data_las_vegas.dropna(inplace=True)
    data_las_vegas['date'] = data_las_vegas['date'].astype('datetime64[ns]')

    for col in ['date','precipitation','precipitation_normal']:
        data_las_vegas[col] = data_las_vegas[col].astype(str)

    data_las_vegas.to_csv('/opt/airflow/data_output/las_vegas.csv')

    data_degreef = pd.read_csv('/opt/airflow/dataset/USW00023169-temperature-degreeF.csv')
    data_degreef.dropna(inplace=True)
    for column in ['date','min','max','normal_min','normal_max']:
        data_degreef[column] = data_degreef[column].astype(str)

    data_degreef.to_csv('/opt/airflow/data_output/degreef.csv')

  

    # save to csv file
    
    print('Data berhasil disimpan ke csv file')


def transform_business_json():
    # json 1
    with open("/opt/airflow/dataset/yelp_academic_dataset_business.json", "r",encoding='utf-8') as f:
        json_data = f.read()

    json_list = json_data.strip().split("\n")
    data = []

    for json_str in json_list:
        try:
            data_dict = json.loads(json_str)

            data.append(data_dict)

        except json.JSONDecodeError:
            continue

    trailing_data = json_data.split("\n")[-1].strip()
    if trailing_data:
        print("ea")
    df = pd.DataFrame(data)

    df.to_csv('/opt/airflow/data_output/business.csv',index=False)
    print('Data berhasil disimpan ke json file')

    #json 2
    

def transform_checkin_json():
    with open("/opt/airflow/dataset/yelp_academic_dataset_checkin.json", "r",encoding='utf-8') as f:
        json_data = f.read()

    json_list = json_data.strip().split("\n")
    data = []

    for json_str in json_list:
        try:
            data_dict = json.loads(json_str)

            data.append(data_dict)

        except json.JSONDecodeError:
            continue

    trailing_data = json_data.split("\n")[-1].strip()
    if trailing_data:
        print("ea")
    df = pd.DataFrame(data)

    df.to_csv('/opt/airflow/data_output/checkin.csv',index=False)
    print('Data berhasil disimpan ke json file')

def success():
    print("Etl Sukses Dilakukan")

def start():
    print("ETL Dimulai")
