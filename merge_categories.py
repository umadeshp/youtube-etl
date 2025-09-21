import json
import glob

# Path to your data folder
data_folder = "data"

# Find all category JSONs
files = glob.glob(f"{data_folder}/*_category_id.json")

merged = {}

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        for k, v in data.items():
            # Avoid duplicate keys
            merged[k] = v

# Save merged JSON
with open(f"{data_folder}/category_id.json", "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=4, ensure_ascii=False)

print("Merged category_id.json created successfully!")
