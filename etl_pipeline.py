"""import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

# -----------------------------
# Database connection
# -----------------------------
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "root"
DB_NAME = "youtube_etl"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


# -----------------------------
# ETL function
# -----------------------------
def etl(csv_file, region_code):
    print(f"Reading {csv_file} ...")
    df = pd.read_csv(csv_file, encoding='ISO-8859-1')

    print("Transforming data ...")
    df['publish_time'] = pd.to_datetime(df['publish_time'])
    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m').dt.date
    df['like_ratio'] = df['likes'] / df['views']

    # -----------------------------
    # Load dim_region
    # -----------------------------
    df_region = pd.DataFrame({'region_code': [region_code]})
    existing_regions = pd.read_sql("SELECT region_code FROM dim_region", engine)
    df_region = df_region[~df_region['region_code'].isin(existing_regions['region_code'])]
    if not df_region.empty:
        df_region.to_sql('dim_region', engine, if_exists='append', index=False)

    # -----------------------------
    # Load dim_category
    # -----------------------------
    df_category = df[['category_id']].drop_duplicates()
    df_category['category_name'] = df_category['category_id'].apply(lambda x: f"Category_{x}")
    existing_categories = pd.read_sql("SELECT category_id FROM dim_category", engine)
    df_category = df_category[~df_category['category_id'].isin(existing_categories['category_id'])]
    if not df_category.empty:
        df_category.to_sql('dim_category', engine, if_exists='append', index=False)

    # -----------------------------
    # Load fact_videos
    # -----------------------------
    df_fact = df[['video_id', 'title', 'channel_title', 'publish_time', 'trending_date',
                  'category_id', 'views', 'likes', 'dislikes', 'comment_count', 'like_ratio', 'tags']].copy()
    df_fact['region_code'] = region_code

    # Remove duplicates in CSV itself
    df_fact.drop_duplicates(subset=['video_id', 'trending_date', 'region_code'], inplace=True)

    # Remove rows already in DB
    existing_videos = pd.read_sql(
        f"SELECT video_id, trending_date, region_code FROM fact_videos WHERE region_code = '{region_code}'", engine)
    existing_videos['trending_date'] = pd.to_datetime(existing_videos['trending_date']).dt.date
    df_fact = df_fact.merge(existing_videos, on=['video_id', 'trending_date', 'region_code'], how='left',
                            indicator=True)
    df_fact = df_fact[df_fact['_merge'] == 'left_only']
    df_fact.drop(columns=['_merge'], inplace=True)

    if not df_fact.empty:
        df_fact.to_sql('fact_videos', engine, if_exists='append', index=False)

    print(f"ETL completed successfully for region: {region_code}")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True, help="Folder containing CSV files")
    args = parser.parse_args()

    for file in os.listdir(args.data_dir):
        if file.endswith(".csv"):
            region_code = file.split('videos')[0].upper()  # USvideos.csv -> US
            csv_path = os.path.join(args.data_dir, file)
            etl(csv_path, region_code)

"""

"""import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
import logging
from psycopg2.extras import execute_values
import psycopg2

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -----------------------------
# Database connection
# -----------------------------
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "root"
DB_NAME = "youtube_etl"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


# -----------------------------
# Helper function for bulk insert
# -----------------------------
def bulk_insert(df, table_name):
    if df.empty:
        return
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        logging.info(f"{len(df)} records inserted into {table_name}")
    except Exception as e:
        logging.error(f"Failed to insert into {table_name}: {e}")


# -----------------------------
# ETL function
# -----------------------------
def etl(csv_file, region_code):
    logging.info(f"Processing file: {csv_file} for region: {region_code}")

    try:
        # -----------------------------
        # Extract
        # -----------------------------
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')

        # -----------------------------
        # Transform
        # -----------------------------
        df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
        df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m', errors='coerce').dt.date
        df['like_ratio'] = df['likes'] / df['views']

        # -----------------------------
        # Load dim_region
        # -----------------------------
        df_region = pd.DataFrame({'region_code': [region_code]})
        existing_regions = pd.read_sql("SELECT region_code FROM dim_region", engine)
        df_region = df_region[~df_region['region_code'].isin(existing_regions['region_code'])]
        bulk_insert(df_region, 'dim_region')

        # -----------------------------
        # Load dim_category
        # -----------------------------
        df_category = df[['category_id']].drop_duplicates()
        df_category['category_name'] = df_category['category_id'].apply(lambda x: f"Category_{x}")
        existing_categories = pd.read_sql("SELECT category_id FROM dim_category", engine)
        df_category = df_category[~df_category['category_id'].isin(existing_categories['category_id'])]
        bulk_insert(df_category, 'dim_category')

        # -----------------------------
        # Load fact_videos
        # -----------------------------
        df_fact = df[['video_id', 'title', 'channel_title', 'publish_time', 'trending_date',
                      'category_id', 'views', 'likes', 'dislikes', 'comment_count', 'like_ratio', 'tags']].copy()
        df_fact['region_code'] = region_code

        # Remove duplicates in CSV itself
        df_fact.drop_duplicates(subset=['video_id', 'trending_date', 'region_code'], inplace=True)

        # Remove rows already in DB
        existing_videos = pd.read_sql(
            f"SELECT video_id, trending_date, region_code FROM fact_videos WHERE region_code = '{region_code}'", engine
        )
        existing_videos['trending_date'] = pd.to_datetime(existing_videos['trending_date']).dt.date
        df_fact = df_fact.merge(existing_videos, on=['video_id', 'trending_date', 'region_code'], how='left',
                                indicator=True)
        df_fact = df_fact[df_fact['_merge'] == 'left_only'].drop(columns=['_merge'])

        bulk_insert(df_fact, 'fact_videos')

        logging.info(f"ETL completed successfully for region: {region_code}")

    except Exception as e:
        logging.error(f"Error processing file {csv_file}: {e}")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL for YouTube trending videos")
    parser.add_argument("--data_dir", required=True, help="Folder containing CSV files")
    args = parser.parse_args()

    for file in os.listdir(args.data_dir):
        if file.endswith(".csv"):
            region_code = file.split('videos')[0].upper()  # USvideos.csv -> US
            csv_path = os.path.join(args.data_dir, file)
            etl(csv_path, region_code)

"""

import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import yaml

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -----------------------------
# Load configuration
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
# Helper function for bulk insert
# -----------------------------
def bulk_insert(df, table_name):
    if df.empty:
        logging.info(f"No new records to insert into {table_name}")
        return
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        logging.info(f"{len(df)} records inserted into {table_name}")
    except Exception as e:
        logging.error(f"Failed to insert into {table_name}: {e}")

# -----------------------------
# Data Validation
# -----------------------------
def validate_data(df):
    initial_count = len(df)
    df = df[df['video_id'].notnull()]
    df = df[df['views'] >= 0]
    df = df[df['likes'] >= 0]
    df = df[df['dislikes'] >= 0]
    logging.info(f"Validated data: {initial_count - len(df)} invalid rows removed")
    return df

# -----------------------------
# ETL Function
# -----------------------------
def etl(csv_file, region_code):
    logging.info(f"Processing file: {csv_file} for region: {region_code}")
    try:
        # Extract
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')

        # Transform
        df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
        df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m', errors='coerce').dt.date
        df['like_ratio'] = df['likes'] / df['views']

        # Data validation
        df = validate_data(df)

        # Load dim_region
        df_region = pd.DataFrame({'region_code': [region_code]})
        existing_regions = pd.read_sql("SELECT region_code FROM dim_region", engine)
        df_region = df_region[~df_region['region_code'].isin(existing_regions['region_code'])]
        bulk_insert(df_region, 'dim_region')

        # Load dim_category
        df_category = df[['category_id']].drop_duplicates()
        df_category['category_name'] = df_category['category_id'].apply(lambda x: f"Category_{x}")
        existing_categories = pd.read_sql("SELECT category_id FROM dim_category", engine)
        df_category = df_category[~df_category['category_id'].isin(existing_categories['category_id'])]
        bulk_insert(df_category, 'dim_category')

        # Load fact_videos
        df_fact = df[['video_id', 'title', 'channel_title', 'publish_time', 'trending_date',
                      'category_id', 'views', 'likes', 'dislikes', 'comment_count', 'like_ratio', 'tags']].copy()
        df_fact['region_code'] = region_code

        # Remove duplicates in CSV
        df_fact.drop_duplicates(subset=['video_id', 'trending_date', 'region_code'], inplace=True)

        # Remove rows already in DB
        existing_videos = pd.read_sql(
            f"SELECT video_id, trending_date, region_code FROM fact_videos WHERE region_code = '{region_code}'", engine
        )
        existing_videos['trending_date'] = pd.to_datetime(existing_videos['trending_date']).dt.date
        df_fact = df_fact.merge(existing_videos, on=['video_id', 'trending_date', 'region_code'], how='left', indicator=True)
        df_fact = df_fact[df_fact['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Insert into fact_videos
        bulk_insert(df_fact, 'fact_videos')

        logging.info(f"ETL completed successfully for region: {region_code}")

    except Exception as e:
        logging.error(f"Error processing file {csv_file}: {e}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL for YouTube trending videos")
    parser.add_argument("--data_dir", required=True, help="Folder containing CSV files")
    args = parser.parse_args()

    csv_files = [f for f in os.listdir(args.data_dir) if f.endswith('.csv')]

    # Parallel processing for multiple CSVs
    with ThreadPoolExecutor(max_workers=4) as executor:
        for file in csv_files:
            region_code = file.split('videos')[0].upper()  # USvideos.csv -> US
            csv_path = os.path.join(args.data_dir, file)
            executor.submit(etl, csv_path, region_code)
