from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum
import datetime

local_tz = pendulum.timezone("Asia/Jakarta")

DBT_PROJECT_PATH = "/opt/dbt/retail_project/retail_project"
DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROFILES_DIR = "/root/.dbt"
DWH_PG_CONN_ID = "dwh_postgres"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="dbt_full_pipeline",
    default_args=DEFAULT_ARGS,
    # Jadwal diatur setelah semua ingest selesai (ingest jam 19:00, dbt jam 20:00)
    schedule_interval="0 20 * * *", 
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    tags=["dbt", "analytics"],
):

    # --- Sensors: Pastikan semua data RAW sudah tersedia ---
    
    # 1. Wait Customers Raw
    wait_customers_raw = SqlSensor(
        task_id="wait_customers_raw",
        conn_id=DWH_PG_CONN_ID,
        sql="SELECT 1 FROM raw.customers_raw LIMIT 1;",
        poke_interval=60, # Cek setiap 60 detik
        timeout=60 * 30, # Timeout setelah 30 menit
        mode="poke",
    )

    # 2. Wait Products Raw
    wait_products_raw = SqlSensor(
        task_id="wait_products_raw",
        conn_id=DWH_PG_CONN_ID,
        sql="SELECT 1 FROM raw.products_raw LIMIT 1;",
        poke_interval=60,
        timeout=60 * 30,
        mode="poke",
    )

    # 3. Wait Stores Raw
    wait_stores_raw = SqlSensor(
        task_id="wait_stores_raw",
        conn_id=DWH_PG_CONN_ID,
        sql="SELECT 1 FROM raw.stores_raw LIMIT 1;",
        poke_interval=60,
        timeout=60 * 30,
        mode="poke",
    )

    # 4. Wait Sales Raw
    wait_sales_raw = SqlSensor(
        task_id="wait_sales_raw",
        conn_id=DWH_PG_CONN_ID,
        sql="SELECT 1 FROM raw.sales_transaction_raw LIMIT 1;",
        poke_interval=60,
        timeout=60 * 30,
        mode="poke",
    )

    
    # --- Transformation Stages (DBT) ---
    
    # 1. dbt deps: Mengunduh dependencies (packages) yang dibutuhkan
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && {DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}, # Set env variable untuk dbt
    )
    
    # 2. dbt run: Menjalankan semua model SQL
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_PATH} && {DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )
    
    # 3. dbt test: Menguji kualitas data pada model yang telah dibuat
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_PATH} && {DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # --- Dependency (Urutan Eksekusi) ---
    
    # Semua sensor harus sukses sebelum menjalankan dbt deps
    all_sensors = [
        wait_customers_raw, 
        wait_products_raw, 
        wait_stores_raw, 
        wait_sales_raw
    ]
    
    # Alur: Sensor -> Deps -> Run -> Test
    all_sensors >> dbt_deps >> dbt_run >> dbt_test
