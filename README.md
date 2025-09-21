# YouTube Trending Videos ETL Pipeline

This project is a Python-based ETL (Extract, Transform, Load) pipeline that processes YouTube trending videos data across multiple regions and stores it in a PostgreSQL database. The pipeline is designed for data engineering tasks, including data validation, transformation, and bulk insertion.

---

## Features

- **Multi-region support**: Processes CSV files from different regions (US, IN, GB, etc.).
- **ETL Pipeline**:
  - **Extract**: Reads YouTube CSV files.
  - **Transform**: Cleans and transforms data, calculates `like_ratio`, converts dates.
  - **Load**: Inserts into `dim_region`, `dim_category`, and `fact_videos` tables in PostgreSQL.
- **Data Validation**: Filters invalid rows (e.g., negative views/likes/dislikes or missing video IDs).
- **Parallel Processing**: Supports concurrent processing of multiple CSV files.
- **Configurable**: Uses a `config.yaml` file for database credentials and settings.
- **Logging**: Tracks ETL progress and errors.

---


---

## Prerequisites

- Python 3.12+
- PostgreSQL database
- Required Python packages:

```bash
pip install -r requirements.txt


