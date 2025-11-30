from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd
import os
import logging

# Ambil logger Airflow
logger = logging.getLogger("airflow.task")

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
FILE_PATH = os.path.join(DATA_DIR, "stores_raw.tsv")

# --- Fungsi untuk Task Airflow ---


def create_stores_raw_table():
    """
    Membuat tabel raw.stores_raw jika belum ada.

    DISAMAKAN dengan struktur file:
    ['store_id', 'store_name', 'city', 'region']
    """
    hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.stores_raw (
        store_id   TEXT,
        store_name TEXT,
        city       TEXT,
        region     TEXT
    );
    """
    try:
        hook.run(sql)
        logger.info("Tabel raw.stores_raw berhasil dibuat atau sudah ada.")
    except Exception as e:
        logger.exception(f"Gagal membuat tabel stores_raw: {e}")
        raise e


def load_stores_raw():
    """Membaca data dari file TSV, membersihkan, dan memuat ke DWH."""

    logger.info(f"Mencoba membaca file dari path: {FILE_PATH}")

    try:
        # File berekstensi .tsv -> asumsikan delimiter TAB '\t'
        df = pd.read_csv(FILE_PATH, sep="\t")

        # Normalisasi header
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.lower()

        logger.info(f"Kolom yang terbaca: {list(df.columns)}")
        logger.info(f"Berhasil membaca {len(df)} baris data.")

        # Kolom wajib sesuai file + tabel
        required_cols = ["store_id", "store_name", "city", "region"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise KeyError(
                f"Kolom wajib tidak ditemukan: {missing}. "
                f"Header di file saat ini: {list(df.columns)}"
            )

        # Hanya pilih kolom yang memang ada di tabel
        df_to_load = df[required_cols]

        dwh_hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)

        # Truncate + Insert
        logger.warning("Melakukan TRUNCATE pada tabel raw.stores_raw.")
        dwh_hook.run("TRUNCATE TABLE raw.stores_raw;")

        engine = dwh_hook.get_sqlalchemy_engine()
        df_to_load.to_sql(
            "stores_raw",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        logger.info("Data berhasil dimuat ke PostgreSQL (raw.stores_raw).")

    except FileNotFoundError as e:
        logger.error(
            f"FATAL ERROR: File tidak ditemukan di {FILE_PATH}. "
            f"Cek mounting volume Docker/Podman Anda. Error: {e}"
        )
        raise e
    except KeyError as e:
        logger.error(
            f"FATAL ERROR: {e}. "
            f"Cek kembali header file dan delimiter (saat ini set ke TAB '\\t')."
        )
        raise e
    except Exception as e:
        logger.exception(f"Terjadi kesalahan umum saat memuat data stores: {e}")
        raise e


# --- Definisi DAG Airflow ---

with DAG(
    dag_id="ingest_stores_raw",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 19 * * *",
    catchup=False,
    tags=["ingestion", "raw", "stores"],
) as dag:

    create_table = PythonOperator(
        task_id="create_stores_raw_table",
        python_callable=create_stores_raw_table,
    )

    load_data = PythonOperator(
        task_id="load_stores_raw",
        python_callable=load_stores_raw,
    )

    # Atur dependency: Buat tabel dulu baru load data
    create_table >> load_data

