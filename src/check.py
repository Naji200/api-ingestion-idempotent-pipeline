import pandas as pd
import glob

files = glob.glob("data/silver/events/date=*/part-*.parquet")
print("Found:", len(files), "parquet files")

df = pd.read_parquet(files[0])
print(df.head())
print(df.columns)