
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_dw():
    #1
    engine_1 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_suhu = pd.read_csv("/opt/airflow/data_dim_fact/dim_degreef_suhu.csv", sep=',')
    df_suhu.to_sql("dim_suhu",engine_1)

    #2
    engine_2 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_precipation = pd.read_csv("/opt/airflow/data_dim_fact/dim_precipation.csv", sep=',')
    df_precipation.to_sql("dim_precipation",engine_2)

    #3
    engine_3 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_store = pd.read_csv("/opt/airflow/data_dim_fact/dim_business_store.csv", sep=',')
    df_store.to_sql("dim_store",engine_3)

    #4
    engine_4 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_review = pd.read_csv("/opt/airflow/data_dim_fact/dim_business_review.csv", sep=',')
    df_review.to_sql("dim_review",engine_4)

    #5
    engine_5 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_location = pd.read_csv("/opt/airflow/data_dim_fact/dim_business_location.csv", sep=',')
    df_location.to_sql("dim_location",engine_5)

    #6
    engine_6 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datawarehouse'})

    df_date = pd.read_csv("/opt/airflow/data_dim_fact/dim_business_date.csv", sep=',')
    df_date.to_sql("dim_date",engine_6)