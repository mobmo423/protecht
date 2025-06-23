FROM apache/airflow:2.4.0

# Use Airflow user (this is REQUIRED by Airflow images)
USER airflow

# Upgrade pip and install required packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres \
    kaggle \
    kagglehub[pandas-datasets] \
    pandas \
    sqlalchemy \
    python-dotenv \
    psycopg2 \
    psycopg2-binary \
    pg8000 \
    boto3
