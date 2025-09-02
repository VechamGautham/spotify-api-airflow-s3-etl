import os
import pandas as pd
from great_expectations.dataset import PandasDataset


BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # go up from src/
csv_path = os.path.join(BASE_DIR, "silver", "tracks", "tracks_silver.csv")

print("Using:", csv_path)

df = pd.read_csv(csv_path)

# Wrap the DataFrame
ds = PandasDataset(df)

# --- Expectations ---
ds.expect_column_values_to_not_be_null("track_id")
ds.expect_column_values_to_be_unique("track_id")

ds.expect_column_values_to_not_be_null("track_name")
ds.expect_column_values_to_not_be_null("album_id")
ds.expect_column_values_to_not_be_null("album_name")

ds.expect_column_values_to_be_between(
    "track_popularity", min_value=0, max_value=100, allow_cross_type_comparisons=True
)

ds.expect_column_values_to_be_in_set(
    "explicit", [True, False, 0, 1, "true", "false", "True", "False"]
)

ds.expect_column_values_to_match_regex("ingestion_date", r"^\d{4}-\d{2}-\d{2}$")

# Run validations
results = ds.validate()
print(results["statistics"])
print("Passed" if results["success"] else "Failed")