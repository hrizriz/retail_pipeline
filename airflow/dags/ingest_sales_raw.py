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
FILE_PATH = os.path.join(DATA_DIR, "sales_transaction_raw.tsv")


# --- Fungsi untuk Task Airflow ---


def create_sales_raw_table():
    """Membuat tabel raw.sales_transaction_raw jika belum ada."""
    hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.sales_transaction_raw (
        transaction_id      TEXT,
        transaction_date    DATE,
        customer_id         TEXT,
        product_id          TEXT,
        store_id            TEXT,
        quantity            INTEGER,
        total_price         NUMERIC
    );
    """
    try:
        hook.run(sql)
        logger.info("Tabel raw.sales_transaction_raw berhasil dibuat atau sudah ada.")
    except Exception as e:
        logger.exception(f"Gagal membuat tabel sales_transaction_raw: {e}")
        raise e


def load_sales_raw():
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

        # Kolom minimal yang wajib ada dari file
        required_from_file = [
            "transaction_id",
            "transaction_date",
            "customer_id",
            "product_id",
            "store_id",
            "quantity",
        ]

        missing_basic = [c for c in required_from_file if c not in df.columns]
        if missing_basic:
            raise KeyError(
                f"Kolom wajib dari file tidak ditemukan: {missing_basic}. "
                f"Header di file saat ini: {list(df.columns)}"
            )

        # Kolom harga: boleh salah satu dari:
        # - total_price langsung dari file, atau
        # - unit_price (nanti kita hitung total_price = quantity * unit_price)
        has_total_price = "total_price" in df.columns
        has_unit_price = "unit_price" in df.columns

        if not has_total_price and not has_unit_price:
            raise KeyError(
                "Tidak ditemukan kolom 'total_price' maupun 'unit_price' di file. "
                f"Header di file saat ini: {list(df.columns)}"
            )

        # Standardisasi tanggal
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date

        # Konversi kolom numerik
        df["quantity"] = (
            pd.to_numeric(df["quantity"], errors="coerce")
            .astype(pd.Int64Dtype())
        )

        if has_total_price:
            logger.info("Menggunakan kolom 'total_price' langsung dari file.")
            df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
        else:
            logger.info(
                "Kolom 'total_price' tidak ada, menghitung dari quantity * unit_price."
            )
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
            df["total_price"] = df["quantity"].astype("float64") * df[
                "unit_price"
            ].astype("float64")

        # Hanya pilih kolom yang memang ada di tabel Postgres
        df_to_load = df[
            [
                "transaction_id",
                "transaction_date",
                "customer_id",
                "product_id",
                "store_id",
                "quantity",
                "total_price",
            ]
        ]

        dwh_hook = PostgresHook(postgres_conn_id=DWH_PG_CONN_ID)

        # Truncate + Insert
        logger.warning("Melakukan TRUNCATE pada tabel raw.sales_transaction_raw.")
        dwh_hook.run("TRUNCATE TABLE raw.sales_transaction_raw;")

        engine = dwh_hook.get_sqlalchemy_engine()
        df_to_load.to_sql(
            "sales_transaction_raw",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        logger.info("Data berhasil dimuat ke PostgreSQL (raw.sales_transaction_raw).")

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
        logger.exception(f"Terjadi kesalahan umum saat memuat data sales: {e}")
        raise e


# --- Definisi DAG Airflow ---

with DAG(
    dag_id="ingest_sales_raw",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 19 * * *",
    catchup=False,
    tags=["ingestion", "raw", "sales"],
) as dag:

    create_table = PythonOperator(
        task_id="create_sales_raw_table",
        python_callable=create_sales_raw_table,
    )

    load_data = PythonOperator(
        task_id="load_sales_raw",
        python_callable=load_sales_raw,
    )

    create_table >> load_data

