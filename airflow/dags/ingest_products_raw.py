from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd
import os

# --- Konfigurasi Umum ---
local_tz = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

DWH_PG_CONN_ID = "dwh_postgres"
DATA_DIR = "/opt/airflow/data/"
FILE_PATH = os.path.join(DATA_DIR, "products_raw.tsv")

# --- Fungsi untuk Task Airflow ---

def create_products_raw_table():
    """Membuat tabel raw.products_raw jika belum ada."""
    hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.products_raw (
        product_id      TEXT,
        product_name    TEXT,
        category        TEXT,
        sub_category    TEXT,
        price           NUMERIC
    );
    """
    hook.run(sql)

def load_products_raw():
    """Membaca data dari TSV, membersihkan (jika perlu), dan memuat ke DWH."""
    # Baca TSV
    df = pd.read_csv(FILE_PATH, sep="\t")
    
    # Konversi kolom 'price' ke tipe numerik
    df["price"] = pd.to_numeric(df["price"], errors='coerce') 

    dwh_hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)

    # Truncate + Insert
    dwh_hook.run("TRUNCATE TABLE raw.products_raw;")

    engine = dwh_hook.get_sqlalchemy_engine()
    df.to_sql("products_raw", engine, schema="raw", if_exists="append", index=False)

# --- Definisi DAG Airflow ---

with DAG(
    dag_id="ingest_products_raw",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 19 * * *", 
    catchup=False,
    tags=["ingestion", "raw", "products"],
) as dag:

    create_table = PythonOperator(
        task_id="create_products_raw_table",
        python_callable=create_products_raw_table,
    )

    load_data = PythonOperator(
        task_id="load_products_raw",
        python_callable=load_products_raw,
    )

    create_table >> load_data
