
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_datamart():
    #1
    engine_1 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datamart'})

    df_store = pd.read_csv("/opt/airflow/data_fact/fact_store.csv", sep=',')
    df_store.to_sql("fact_store",engine_1)

    #2
    engine_2 = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz', 
                        connect_args={'options': '-csearch_path=datamart'})

    df_suhu = pd.read_csv("/opt/airflow/data_fact/fact_suhu.csv", sep=',')
    df_suhu.to_sql("fact_suhu",engine_2)

    