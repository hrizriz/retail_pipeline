from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd
import os

local_tz = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

DWH_PG_CONN_ID = "dwh_postgres"
DATA_DIR =  "/opt/airflow/data/"
FILE_PATH = os.path.join(DATA_DIR, "customers_raw.tsv")

def create_customers_raw_table():
    hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.customers_raw (
        customer_id   TEXT,
        customer_name TEXT,
        gender        TEXT,
        city          TEXT,
        signup_date   DATE
    );
    """
    hook.run(sql)

def load_customers_raw():
    # baca TSV
    df = pd.read_csv(FILE_PATH, sep="\t")

    # standardisasi tanggal
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date

    dwh_hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)

    # truncate + insert
    dwh_hook.run("TRUNCATE TABLE raw.customers_raw;")

    engine = dwh_hook.get_sqlalchemy_engine()
    df.to_sql("customers_raw", engine, schema="raw", if_exists="append", index=False)

with DAG(
    dag_id="ingest_customers_raw",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 19 * * *",
    catchup=False,
    tags=["ingestion", "raw", "customers"],
) as dag:

    create_table = PythonOperator(
        task_id="create_customers_raw_table",
        python_callable=create_customers_raw_table,
    )

    load_data = PythonOperator(
        task_id="load_customers_raw",
        python_callable=load_customers_raw,
    )

    create_table >> load_data
