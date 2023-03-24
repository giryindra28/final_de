import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_postgres():
    
    # load data csv
    df_vegas = pd.read_csv("/opt/airflow/data_output/las_vegas.csv", sep=',')
    engine_vegas = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz',
                                 connect_args={'options': '-csearch_path=stagging'})
    df_degref = pd.read_csv("/opt/airflow/data_output/degreef.csv", sep=',')
    engine_degref = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz',
                                  connect_args={'options': '-csearch_path=stagging'})
        
    df_vegas.to_sql("vegas",engine_vegas)
    df_degref.to_sql("degreef",engine_degref)

    #load data json
    df_business = pd.read_csv("/opt/airflow/data_output/business.csv", sep=',')
    engine_business = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz',
                                    connect_args={'options': '-csearch_path=stagging'})

    df_checkin = pd.read_csv("/opt/airflow/data_output/checkin.csv", sep=',')
    engine_checkin = create_engine('postgresql://user_dev:finalde@172.17.0.1:5437/divisi_xyz',
                                   connect_args={'options': '-csearch_path=stagging'})
        
    df_business.to_sql("business",engine_business)
    df_checkin.to_sql("checkin",engine_checkin)
