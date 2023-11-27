import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import sqlalchemy
import pandas as pd
import datetime as dt
import os
from dotenv import load_dotenv
import logging

load_dotenv()

DATABASE = {
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'pass'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'connect_timeout': 2
}

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'depends_on_past': False,
    'email': ['alexvicbor@gmail.com'],
    'email_on_failure': True,
    'email_on_success': False,
}

def main_proc():
    engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{DATABASE['user']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['database']}")
    get_report_response = requests.get("https://random-data-api.com/api/cannabis/random_cannabis?size=10").json() # take data from api to json
    df = pd.DataFrame(data=get_report_response) # pandas df - easy method for structuring data
    
    try:
        df.to_sql('your_table', con=engine, schema = 'public', if_exists = 'append') # write data to postgres table (but I didn't test it)
    except:
        engine2 = PostgresHook(postgres_conn_id='your_hook_id').get_sqlalchemy_engine()
        df.to_sql('your_table', con=engine2, schema = 'public', if_exists = 'append') # second method if you have hook

    logging.info('completed')
    
with DAG(dag_id = "connector_etl", default_args = args, schedule_interval = '0 12,0 * * *', catchup = False) as dag:
    main_task = PythonOperator(
        task_id='first_task',
        python_callable=main_proc,
        dag=dag
    )

    main_task