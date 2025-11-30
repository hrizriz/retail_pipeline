FROM apache/airflow:2.9.3

# Install dependencies sistem yang dibutuhkan dbt-postgres (optional, tapi oke)
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Kembali ke user Airflow dan install Python deps
USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN pip install --no-cache-dir \
    "dbt-core==1.7.0" \
    "dbt-postgres==1.7.0" \
    "pandas"

