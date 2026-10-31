FROM apache/airflow:2.10.5
# Pakai constraints supaya pyarrow tidak menggeser dependency Airflow.
# Sesuaikan 3.12 kalau versi Python image berbeda.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
