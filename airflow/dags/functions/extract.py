
import os
import io
import zipfile
import logging
import hashlib
import pandas as pd
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, DateTime
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generate_hash(row):
    key_str = f"{row['currency']}_{row['base_currency']}_{row['currency_name']}_{row['exchange_rate']}_{row['date']}"
    return hashlib.sha256(key_str.encode()).hexdigest()

def main():
    load_dotenv()

    logging.info("Authenticating with Kaggle API...")
    api = KaggleApi()
    api.authenticate()

    dataset = 'asaniczka/forex-exchange-rate-since-2004-updated-daily'
    zip_filename = 'forex-exchange-rate-since-2004-updated-daily.zip'
    logging.info(f"Downloading dataset: {dataset}")
    api.dataset_download_files(dataset, path='.', force=True)
    logging.info("Download complete.")

    logging.info("Extracting CSV file from ZIP...")
    with zipfile.ZipFile(zip_filename, 'r') as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith('.csv'))
        with archive.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file)

    logging.info(f"CSV file '{csv_name}' loaded into memory.")
    logging.info(f"Preview of data:\n{df.head()}")

    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        logging.info("Converting 'date' column to datetime...")
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    logging.info("Generating unique record_id hash...")
    df["record_id"] = df.apply(generate_hash, axis=1)

    logging.info("Connecting to PostgreSQL database...")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=5432,
        database=db_name
    )

    engine = create_engine(connection_url)

    logging.info("Creating table schema with unique key (record_id)...")
    meta = MetaData()
    raw_forex_table = Table(
        "raw_forex", meta,
        Column("record_id", String, primary_key=True),
        Column("currency", String),
        Column("base_currency", String),
        Column("currency_name", String),
        Column("exchange_rate", Float),
        Column("date", DateTime)
    )
    meta.create_all(engine)

    logging.info("Performing UPSERT into PostgreSQL using record_id...")
    insert_stmt = pg_insert(raw_forex_table).values(df.to_dict(orient="records"))
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["record_id"],
        set_={c.key: c for c in insert_stmt.excluded if c.key != "record_id"}
    )

    with engine.begin() as conn:
        conn.execute(update_stmt)

    logging.info("✅ UPSERT complete. Data synchronized.")

    os.remove(zip_filename)
    logging.info("Removed ZIP file and finished execution.")

if __name__ == "__main__":
    main()
