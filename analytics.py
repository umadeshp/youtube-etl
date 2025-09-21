import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# Database connection
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "root"
DB_NAME = "youtube_etl"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------
# Load fact and dimension tables
# -----------------------------
fact_videos = pd.read_sql("SELECT * FROM fact_videos", engine)
dim_category = pd.read_sql("SELECT * FROM dim_category", engine)
dim_region = pd.read_sql("SELECT * FROM dim_region", engine)

# Merge category names
fact_videos = fact_videos.merge(dim_category, on='category_id', how='left')

# -----------------------------
# Analytics 1: Top 5 trending videos per region
# -----------------------------
top_videos = fact_videos.groupby(['region_code','title']).agg({'views':'max'}).reset_index()
top_videos = top_videos.sort_values(['region_code','views'], ascending=[True, False])
print("Top 5 videos per region:")
for region in dim_region['region_code']:
    print(f"\nRegion: {region}")
    print(top_videos[top_videos['region_code']==region].head(5)[['title','views']])

# -----------------------------
# Analytics 2: Most popular categories
# -----------------------------
category_views = fact_videos.groupby('category_name')['views'].sum().sort_values(ascending=False)
print("\nTop 5 categories by total views:")
print(category_views.head(5))

# -----------------------------
# Analytics 3: Average likes/views per category
# -----------------------------
fact_videos['like_ratio'] = fact_videos['likes'] / fact_videos['views']
avg_like_ratio = fact_videos.groupby('category_name')['like_ratio'].mean().sort_values(ascending=False)
print("\nAverage like ratio per category:")
print(avg_like_ratio.head(5))

# -----------------------------
# Analytics 4: Visualize top categories
# -----------------------------
plt.figure(figsize=(8,6))
sns.barplot(x=category_views.head(10).values, y=category_views.head(10).index, palette='viridis')
plt.title("Top 10 Categories by Total Views")
plt.xlabel("Total Views")
plt.ylabel("Category")
plt.tight_layout()
plt.show()
