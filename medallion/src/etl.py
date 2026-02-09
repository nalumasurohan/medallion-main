import pandas as pd
from sqlalchemy import create_engine, text
import hashlib
import logging
from datetime import datetime
import os
import sys
from db_config import DB_URI



CSV_FOLDER = "bronze_inputs/"
LOG_FILE = "logs/etl.log"

BRONZE_TABLES = {
    "riders": "raw_riders.csv",
    "drivers": "raw_drivers.csv",
    "rides": "raw_rides.csv",
    "payments": "raw_payments.csv",
    "driver_shifts": "raw_driver_shifts.csv"
}

SILVER_SQL_PATH = "sql/silver"
GOLD_SQL_PATH = "sql/gold"



os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)



engine = create_engine(DB_URI)



def run_sql_file(file_path):
    with open(file_path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))



def load_bronze():
    logging.info("BRONZE LOAD STARTED")

    for table, csv_file in BRONZE_TABLES.items():
        start_time = datetime.now()

        try:
            df = pd.read_csv(os.path.join(CSV_FOLDER, csv_file))
            rows = len(df)

            checksum = hashlib.md5(
                pd.util.hash_pandas_object(df, index=True).values
            ).hexdigest()

            df.to_sql(
                table,
                engine,
                schema="bronze",
                if_exists="replace",
                index=False,
                method="multi"
            )

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO audit.bronze_load_log
                        (table_name, file_name, row_count, checksum)
                        VALUES (:table, :file, :rows, :checksum)
                    """),
                    {
                        "table": table,
                        "file": csv_file,
                        "rows": rows,
                        "checksum": checksum
                    }
                )

            elapsed = datetime.now() - start_time

            logging.info(
                f"BRONZE | TABLE={table} | ROWS={rows} | CHECKSUM={checksum} | TIME={elapsed}"
            )
            print(f"Loaded bronze.{table}: {rows} rows")

        except Exception as e:
            logging.error(f"BRONZE | TABLE={table} | ERROR={str(e)}")
            print(f"Failed loading bronze.{table}: {e}")

    logging.info("BRONZE LOAD COMPLETED")



def build_silver():
    logging.info("SILVER BUILD STARTED")
    start_time = datetime.now()

    silver_files = [
        "riders.sql",
        "drivers.sql",
        "rides.sql",
        "payments.sql",
        "driver_shifts.sql",
        "audit.sql"
    ]

    try:
        for file in silver_files:
            run_sql_file(os.path.join(SILVER_SQL_PATH, file))
            logging.info(f"SILVER | Executed {file}")

        elapsed = datetime.now() - start_time
        logging.info(f"SILVER BUILD COMPLETED | TIME={elapsed}")
        print("Silver layer built successfully")

    except Exception as e:
        logging.error(f"SILVER BUILD FAILED | ERROR={str(e)}")
        print(f"Silver build failed: {e}")



def build_gold():
    logging.info("GOLD BUILD STARTED")
    start_time = datetime.now()

    gold_files = [
        "daily_ride_metrics.sql",
        "driver_performance.sql",
        "dashboard_rides.sql"
    ]

    try:
        for file in gold_files:
            run_sql_file(os.path.join(GOLD_SQL_PATH, file))
            logging.info(f"GOLD | Executed {file}")

        elapsed = datetime.now() - start_time
        logging.info(f"GOLD BUILD COMPLETED | TIME={elapsed}")
        print("Gold layer built successfully")

    except Exception as e:
        logging.error(f"GOLD BUILD FAILED | ERROR={str(e)}")
        print(f"Gold build failed: {e}")



def run_all():
    load_bronze()
    build_silver()
    build_gold()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py [bronze|silver|gold|all]")
        sys.exit(1)

    step = sys.argv[1].lower()

    if step == "bronze":
        load_bronze()
    elif step == "silver":
        build_silver()
    elif step == "gold":
        build_gold()
    elif step == "all":
        run_all()
    else:
        print("Invalid argument. Use: bronze | silver | gold | all")
