import pandas as pd
from sqlalchemy import create_engine
import os
from db_config import DB_URI

OUTPUT_DIR = "dashboard_exports"

os.makedirs(OUTPUT_DIR, exist_ok=True)

engine = create_engine(DB_URI)

tables = [
    "gold.dashboard_rides",
    "gold.daily_ride_metrics",
    "gold.driver_performance"
]

for table in tables:
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    file_name = table.replace(".", "_") + ".csv"
    df.to_csv(f"{OUTPUT_DIR}/{file_name}", index=False)
    print(f"Exported {table} → {file_name}")
