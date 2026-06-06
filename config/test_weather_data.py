# config/test_weather_data.py
# Reads back one weather file from ADLS and prints it
# Run with: python config/test_weather_data.py

import os
import io
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container    = os.getenv("AZURE_BRONZE_CONTAINER", "bronze")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={account_name};"
    f"AccountKey={account_key};"
    f"EndpointSuffix=core.windows.net"
)

client = BlobServiceClient.from_connection_string(connection_string)

# Read the ERCO weather file for current hour
now        = datetime.now(timezone.utc)
hour_str   = now.strftime("%H")
month_str  = now.strftime("%m")
day_str    = now.strftime("%d")
year_str   = now.strftime("%Y")

blob_path = (
    f"weather/year={year_str}/month={month_str}/"
    f"day={day_str}/region=ERCO/hour={hour_str}/data.parquet"
)

print(f"Reading: bronze/{blob_path}\n")

blob_client = client.get_blob_client(container=container, blob=blob_path)
data        = blob_client.download_blob().readall()
df          = pd.read_parquet(io.BytesIO(data))

print("Columns:", list(df.columns))
print(f"\nShape: {df.shape[0]} rows x {df.shape[1]} columns")
print("\nData:")
print(df.to_string(index=False))