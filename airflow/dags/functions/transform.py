import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import jinja2 as j2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    load_dotenv()

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

    logging.info("Fetching schema for raw_forex...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'raw_forex'
            LIMIT 1;
        """))
        schema_row = result.fetchone()

    if not schema_row:
        raise Exception("Table 'raw_forex' not found in any schema.")

    schema = schema_row[0]
    logging.info(f"Found 'raw_forex' in schema: {schema}")

    model_dir = "models"
    sql_files = [f for f in os.listdir(model_dir) if f.endswith(".sql")]

    for sql_file in sql_files:
        target_table = os.path.splitext(sql_file)[0]
        path = os.path.join(model_dir, sql_file)

        logging.info(f"Processing model: {target_table}")

        with open(path, "r") as f:
            raw_sql = f.read()

        template = j2.Template(raw_sql)
        rendered_sql = template.render(schema=schema, target_table=target_table)

        logging.info(f"Executing SQL for {target_table}...\n{rendered_sql}")

        with engine.begin() as conn:
            conn.execute(text(rendered_sql))

        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{target_table}"))
            row_count = result.scalar()
            logging.info(f"✅ Table '{target_table}' created with {row_count} rows.")

if __name__ == "__main__":
    main()
