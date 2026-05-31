# config/test_eia_api.py
# Purpose: Verify EIA API is working and understand the data structure
# Run this with: python config/test_eia_api.py

import requests
import json
from dotenv import load_dotenv
import os

# Load the API key from .env file
# This is why we created .env — so the key never appears in code
load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")

def test_eia_connection():
    """
    Makes a single API call to EIA to fetch the last 5 hours
    of electricity demand data for Texas (ERCO region).
    
    We use Texas first because it is the most dramatic grid
    in the US — highest renewable growth, most volatility.
    """
    
    print("=" * 50)
    print("Testing EIA API Connection")
    print("=" * 50)
    
    # The EIA API endpoint for hourly electricity data
    # v2 is the current API version as of 2024
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    
    # Parameters tell the API exactly what data we want
    params = {
        "api_key": API_KEY,
        "frequency": "hourly",           # We want hourly data
        "data[0]": "value",              # Return the MWh value
        "facets[respondent][]": "ERCO",  # Texas grid region
        "facets[type][]": "D",           # D = Demand
        "sort[0][column]": "period",     # Sort by time
        "sort[0][direction]": "desc",    # Latest first
        "length": 5                      # Just 5 rows to test
    }
    
    print(f"\nCalling: {url}")
    print(f"Region: Texas (ERCO)")
    print(f"Data type: Demand (D)")
    print(f"Rows requested: 5\n")
    
    # Make the API call
    response = requests.get(url, params=params)
    
    # Check if the call succeeded
    # Status 200 means success, anything else is an error
    if response.status_code != 200:
        print(f"ERROR: API returned status {response.status_code}")
        print(response.text)
        return
    
    # Parse the JSON response into a Python dictionary
    data = response.json()
    
    # The actual records are nested inside response > data
    records = data.get("response", {}).get("data", [])
    
    print(f"SUCCESS — API returned {len(records)} records\n")
    print("-" * 50)
    print(f"{'Period':<20} {'Region':<10} {'Type':<8} {'Value (MWh)':<15}")
    print("-" * 50)
    
    for record in records:
        print(
            f"{record['period']:<20} "
            f"{record['respondent']:<10} "
            f"{record['type']:<8} "
            f"{record['value']:<15}"
        )
    
    print("-" * 50)
    print("\nFull raw response structure (first record):")
    print(json.dumps(records[0], indent=2))
    print("\n✓ EIA API is working correctly")
    print("✓ You are seeing LIVE electricity demand data from Texas")
    print("✓ Value is in Megawatt-hours (MWh)")

if __name__ == "__main__":
    test_eia_connection()