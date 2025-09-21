# test_etl.py
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from etl_pipeline import etl  # Import your ETL function
import yaml

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------
# Load DB config
# -----------------------------
with open("config.yaml") as f:
    config = yaml.safe_load(f)

DB_HOST = config['db']['host']
DB_PORT = config['db']['port']
DB_USER = config['db']['user']
DB_PASS = config['db']['password']
DB_NAME = config['db']['name']

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------
# Test ETL
# -----------------------------
def test_etl(data_dir='./data'):
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    if not csv_files:
        logging.error("No CSV files found in the data directory!")
        return

    # Take the first CSV for testing
    test_file = csv_files[0]
    region_code = test_file.split('videos')[0].upper()
    csv_path = os.path.join(data_dir, test_file)
    logging.info(f"Running ETL for test file: {csv_path}")

    # Run ETL
    etl(csv_path, region_code)

    # -----------------------------
    # Check DB tables
    # -----------------------------
    try:
        # Check dim_region
        regions = pd.read_sql("SELECT * FROM dim_region WHERE region_code=%s", engine, params=(region_code,))
        logging.info(f"dim_region entries for {region_code}: {len(regions)}")

        # Check dim_category
        categories = pd.read_sql("SELECT * FROM dim_category", engine)
        logging.info(f"Total categories in dim_category: {len(categories)}")

        # Check fact_videos
        facts = pd.read_sql("SELECT * FROM fact_videos WHERE region_code=%s", engine, params=(region_code,))
        logging.info(f"fact_videos entries for {region_code}: {len(facts)}")

        logging.info("ETL test completed successfully!")

    except Exception as e:
        logging.error(f"Database check failed: {e}")


if __name__ == "__main__":
    test_etl()
