# config/test_eia_regions.py
# Purpose: Find the correct EIA respondent codes for all regions
# Run with: python config/test_eia_regions.py

import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")

# Test each region code against the fuel-type endpoint
# This is where CALI, MISO, ISNE, SOCO are failing
# Replace TEST_REGIONS with the confirmed correct codes only
TEST_REGIONS = {
    "ERCO": "Texas",
    "CAL":  "California",
    "PJM":  "PJM Interconnection",
    "MISO": "Midcontinent ISO",
    "NYIS": "New York",
    "ISNE": "New England",
    "SWPP": "Southwest Power Pool",
    "BPAT": "Pacific Northwest",
    "SOCO": "Southern Company",
    "TVA":  "Tennessee Valley Authority",
    "DUK":  "Duke Energy",
    "FPL":  "Florida Power and Light",
    "SC":   "South Carolina"
}

URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

print("Testing EIA fuel-type endpoint for each region code")
print("=" * 60)

for code, name in TEST_REGIONS.items():
    params = {
        "api_key":              API_KEY,
        "frequency":            "hourly",
        "data[0]":              "value",
        "facets[respondent][]": code,
        "sort[0][column]":      "period",
        "sort[0][direction]":   "desc",
        "length":               1
    }

    response = requests.get(URL, params=params)
    if response.status_code == 200:
        records = response.json().get("response", {}).get("data", [])
        if records:
            print(f"  ✓ {code:<6} ({name}) — returned {len(records)} record — period: {records[0]['period']}")
        else:
            print(f"  ✗ {code:<6} ({name}) — 0 records returned")
    else:
        print(f"  ! {code:<6} ({name}) — HTTP {response.status_code}")

print("=" * 60)
print("Use the codes marked with ✓ in GRID_REGIONS")