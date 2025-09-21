# YouTube Trending Videos ETL Pipeline

## Overview
This project is a Python-based ETL (Extract, Transform, Load) pipeline designed to process YouTube trending videos data for multiple regions. It reads CSV files containing video data, transforms and validates the data, and loads it into a PostgreSQL database.  

The project uses **pandas**, **SQLAlchemy**, and **PostgreSQL** for database operations, and supports parallel processing for multiple CSV files using **ThreadPoolExecutor**.  

This pipeline is modular, configurable via a `config.yaml` file, and suitable for building data engineering experience.

---

## Features

- **Extract:** Reads CSV files for different regions.
- **Transform:**  
  - Converts `publish_time` and `trending_date` to proper datetime formats.  
  - Calculates `like_ratio` (likes/views).  
  - Performs data validation to remove invalid or missing records.
- **Load:**  
  - Loads dimension tables: `dim_region`, `dim_category`.  
  - Loads fact table: `fact_videos` while avoiding duplicates.  
- **Configuration:** Database connection parameters are stored in `config.yaml`.
- **Logging:** ETL process logs activities and errors to console.
- **Parallel Processing:** Processes multiple CSV files concurrently for efficiency.

---



## Prerequisites

- Python 3.10+
- PostgreSQL
- Git

---

## Setup

1. **Clone the repository**
```bash
git clone https://github.com/umadeshp/youtube-etl.git
cd youtube-etl
```
2. **Create and activate a virtual environment**

```bash
Copy code
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate   # Linux/Mac
```
3. **Install dependencies**
```
bash
Copy code
pip install -r requirements.txt
Configure database
```
4. **Update config.yaml with your PostgreSQL credentials:**
```
yaml
Copy code
db:
  host: localhost
  port: 5432
  user: postgres
  password: root
  name: youtube_etl
```
## Running the ETL Pipeline
```bash
Copy code
python etl_pipeline.py --data_dir data
--data_dir: Path to folder containing regional CSV files (e.g., USvideos.csv, INvideos.csv).
```
##Example Data
The data/ folder contains sample CSV files per region, e.g.:

- USvideos.csv

- INvideos.csv

- GBvideos.csv

Each file should have columns like video_id, title, channel_title, publish_time, trending_date, category_id, views, likes, dislikes, comment_count, tags.

## Logging
The ETL script logs:

- Number of records inserted per table

- Invalid rows removed during validation

- Errors during processing

Example:
```
yaml
Copy code

2025-09-21 12:00:00 [INFO] Processing file: data/USvideos.csv for region: US

2025-09-21 12:00:01 [INFO] 2 invalid rows removed

2025-09-21 12:00:02 [INFO] 500 records inserted into dim_region

2025-09-21 12:00:03 [INFO] ETL completed successfully for region: US
```

## Contributing
Contributions are welcome! Please follow these steps:

1. Fork the repository

2. Create a new branch (git checkout -b feature/your-feature)

3. Commit your changes (git commit -m "Add feature")

4. Push to the branch (git push origin feature/your-feature)

5. Open a Pull Request

