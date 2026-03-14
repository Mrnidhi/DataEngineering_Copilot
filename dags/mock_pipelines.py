import time
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# 1. A SUCCESSFUL DAG (simulate ELT)
def extract_data():
    print("Extracting data from API...")
    time.sleep(2)
    print("Extraction successful. 15,000 rows extracted.")

def load_data():
    print("Loading data into Snowflake staging area...")
    time.sleep(1)
    print("Load successful.")

def transform_data():
    print("Running dbt model: my_first_dbt_model")
    time.sleep(3)
    print("dbt run completed successfully.")

with DAG(
    'successful_elt_pipeline',
    default_args=default_args,
    description='A mock pipeline that always succeeds',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mock', 'elt'],
) as dag_success:

    t1_extract = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_data,
    )

    t2_load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_data,
    )

    t3_transform = PythonOperator(
        task_id='run_dbt_transform',
        python_callable=transform_data,
    )

    t1_extract >> t2_load >> t3_transform


# 2. A FAILING DAG (simulate a dbt error for the agent to catch)
def broken_extract():
    print("Connecting to API...")
    time.sleep(1)
    print("Connected. Fetching records...")

def broken_transform():
    print("Starting dbt run...")
    print("Running dbt model: stg_users")
    time.sleep(2)
    print("""
Runtime Error in model stg_users (models/staging/stg_users.sql)
  Database Error
  column "user_email" does not exist
  LINE 4:     user_email as email,
              ^
  HINT:  Perhaps you meant to reference the column "raw_users.email_address".
    """)
    raise Exception("dbt run failed due to missing column.")


with DAG(
    'failing_dbt_pipeline',
    default_args=default_args,
    description='A mock pipeline that simulates a dbt compilation error',
    schedule_interval=timedelta(minutes=5), # runs frequently for testing
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mock', 'dbt_failure'],
) as dag_fail:

    fail_t1 = PythonOperator(
        task_id='extract_users',
        python_callable=broken_extract,
    )

    fail_t2 = PythonOperator(
        task_id='run_dbt_models',
        python_callable=broken_transform,
    )

    fail_t1 >> fail_t2
