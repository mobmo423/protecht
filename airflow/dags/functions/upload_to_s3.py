import os
import boto3
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    load_dotenv()

    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_PREFIX", "currency-reports/")
    report_name = f"currency_report_{datetime.today().strftime('%Y-%m-%d')}.csv"

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

    query = """
        SELECT DISTINCT
            currency_name,
            avg_cons_perc_change,
            RANK() OVER (ORDER BY avg_cons_perc_change DESC) AS today_rank
        FROM currency_momentum_metrics
        ORDER BY avg_cons_perc_change DESC
        LIMIT 10
    """
    logging.info("Querying top 10 currencies by avg_cons_perc_change...")
    df_today = pd.read_sql(query, engine)

    s3 = boto3.client("s3")
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    previous_key = f"{s3_prefix}currency_report_{yesterday}.csv"

    try:
        logging.info(f"Attempting to fetch yesterday's report: {previous_key}")
        obj = s3.get_object(Bucket=s3_bucket, Key=previous_key)
        df_prev = pd.read_csv(obj['Body'])

        df_today = df_today.merge(
            df_prev[['currency_name', 'today_rank']],
            on='currency_name',
            how='left',
            suffixes=('', '_prev')
        ).rename(columns={'today_rank_prev': 'yesterday_rank'})

        logging.info("Merged with previous day’s ranks.")
    except s3.exceptions.NoSuchKey:
        logging.warning("No previous report found on S3. Adding empty yesterday_rank column.")
        df_today['yesterday_rank'] = None

    df_today.to_csv(report_name, index=False)
    logging.info(f"Local CSV saved: {report_name}")

    s3.upload_file(
        Filename=report_name,
        Bucket=s3_bucket,
        Key=f"{s3_prefix}{report_name}"
    )

    logging.info(f"✅ Report uploaded to: s3://{s3_bucket}/{s3_prefix}{report_name}")

if __name__ == "__main__":
    main()
