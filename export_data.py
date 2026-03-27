import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database="clickstream_db",
    user="pgadmin",
    password=os.getenv("POSTGRES_PASSWORD"),
    sslmode="require"
)

tables = [
    "analytics.mart_page_performance",
    "analytics.mart_conversion_funnel",
    "analytics.mart_geo_performance",
    "analytics.mart_device_performance"
]

os.makedirs("power_bi/data_exports", exist_ok=True)

for table in tables:
    table_name = table.split(".")[1]
    filepath = f"power_bi/data_exports/{table_name}.csv"
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Exported {len(rows)} rows from {table} → {filepath}")
    cur.close()

conn.close()
print("All tables exported successfully!")