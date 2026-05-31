# ingestion/functions/eia_ingestor.py
#
# Purpose: Fetches hourly electricity generation and demand data
#          from the EIA API for all 13 US grid regions and all
#          fuel types, then writes to ADLS Bronze zone.
#
# Called by: Azure Function (hourly trigger)
#            historical_backfill.py (one-time history load)
#
# Output:
#   bronze/eia/year=YYYY/month=MM/day=DD/hour=HH/region=XX/data.parquet

import os
import sys
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add project root to path so we can import adls_writer
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from ingestion.functions.adls_writer import ADLSWriter

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# All magic values in one place at the top.
# If anything changes you update it here only.
# ─────────────────────────────────────────────

EIA_API_KEY  = os.getenv("EIA_API_KEY")
EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
EIA_FUEL_URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

# All 13 US grid regions we track
# Code : Human-readable name
GRID_REGIONS = {
    "ERCO": "Electric Reliability Council of Texas",
    "CAL": "California ISO",
    "PJM":  "PJM Interconnection",
    "MISO": "Midcontinent ISO",
    "NYIS": "New York ISO",
    "ISNE": "ISO New England",
    "SWPP": "Southwest Power Pool",
    "BPAT": "Bonneville Power Administration",
    "SOCO": "Southern Company",
    "TVA":  "Tennessee Valley Authority",
    "DUK":  "Duke Energy Carolinas",
    "FPL":  "Florida Power and Light",
    "SC":   "South Carolina"
}

# Fuel types we track for generation data
FUEL_TYPES = {
    "COL": "Coal",
    "NG":  "Natural Gas",
    "NUC": "Nuclear",
    "OIL": "Oil",
    "SUN": "Solar",
    "WAT": "Hydro",
    "WND": "Wind",
    "OTH": "Other"
}

# How many records to request per API call
# 100 covers all fuel types for one region in one hour comfortably
RECORDS_PER_CALL = 100

# Seconds to wait between API calls
# EIA allows 5000 requests/hour so 0.2s gap is safe and polite
API_CALL_DELAY = 0.2

# How many times to retry a failed API call before giving up
MAX_RETRIES = 3

# Seconds to wait before retrying after a failure
RETRY_DELAY = 5


class EIAIngestor:
    """
    Fetches electricity grid data from the EIA API and writes
    it to ADLS Gen2 Bronze zone as partitioned Parquet files.
    """

    def __init__(self):
        """
        Initialise the ingestor — validate API key and
        set up the ADLS writer.
        """
        if not EIA_API_KEY:
            raise ValueError(
                "EIA_API_KEY not found in .env file. "
                "Register at eia.gov/opendata"
            )

        self.writer    = ADLSWriter()
        self.container = os.getenv("AZURE_BRONZE_CONTAINER", "bronze")

        logger.info("EIAIngestor initialised")
        logger.info(f"Tracking {len(GRID_REGIONS)} grid regions")
        logger.info(f"Tracking {len(FUEL_TYPES)} fuel types")

    def _get_latest_available_hour(self, region: str) -> datetime:
        """
        Finds the most recent hour that EIA has published
        data for a specific region.

        Some regions like MISO and ISNE have data lags of
        several days. Rather than requesting the current hour
        and getting empty data, we ask EIA what the latest
        available period is and use that instead.

        Args:
            region: Grid region code e.g. 'MISO'

        Returns:
            datetime of the latest available hour for that region
        """
        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": region,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",  # latest first
            "length":               1        # just need the most recent one
        }

        records = self._call_eia_api(EIA_FUEL_URL, params)

        if records:
            # Parse the period string back to datetime
            # EIA format is: 2026-05-28T05
            period_str   = records[0]["period"]
            latest_hour  = datetime.strptime(
                period_str, "%Y-%m-%dT%H"
            ).replace(tzinfo=timezone.utc)

            logger.info(
                f"Latest available hour for {region}: "
                f"{period_str}"
            )
            return latest_hour

        # Fallback — if we cannot determine latest hour
        # use 24 hours ago as a safe default
        logger.warning(
            f"Could not determine latest hour for {region} — "
            f"defaulting to 24 hours ago"
        )
        return datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=24)    


    def _call_eia_api(self, url: str, params: dict) -> list:
        """
        Makes a single EIA API call with retry logic.

        Why retry logic?
        APIs fail sometimes — network blip, server busy,
        temporary outage. Without retries a single failed
        call causes the entire hourly ingestion to miss data.
        With retries we try up to 3 times before giving up.

        Args:
            url:    The EIA API endpoint URL
            params: Query parameters dictionary

        Returns:
            List of records from the API response
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=30  # fail after 30 seconds, do not hang forever
                )

                # 200 = success
                # anything else = something went wrong
                if response.status_code == 200:
                    data    = response.json()
                    records = data.get("response", {}).get("data", [])
                    return records

                # 429 = rate limited — we are calling too fast
                elif response.status_code == 429:
                    logger.warning(
                        f"Rate limited by EIA API — "
                        f"waiting {RETRY_DELAY * attempt}s"
                    )
                    time.sleep(RETRY_DELAY * attempt)

                # Any other error
                else:
                    logger.warning(
                        f"EIA API returned {response.status_code} "
                        f"on attempt {attempt}/{MAX_RETRIES}"
                    )
                    time.sleep(RETRY_DELAY)

            except requests.exceptions.Timeout:
                logger.warning(
                    f"EIA API timed out on attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY)

            except requests.exceptions.ConnectionError:
                logger.warning(
                    f"Connection error on attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY)

        # All retries exhausted
        logger.error(
            f"EIA API failed after {MAX_RETRIES} attempts — "
            f"returning empty list"
        )
        return []


    def _fetch_demand(self, region: str, target_hour: datetime) -> list:
        """
        Fetches total electricity demand for one region
        for a specific hour.

        Demand = how much electricity consumers are using
        in that region at that hour. Measured in MWh.

        Args:
            region:      Grid region code e.g. 'ERCO'
            target_hour: The hour we want data for

        Returns:
            List of demand records from EIA
        """
        # Format the hour as EIA expects it
        # EIA uses format: 2026-05-31T14 (no minutes or seconds)
        period_str = target_hour.strftime("%Y-%m-%dT%H")

        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": region,
            "facets[type][]":       "D",        # D = Demand
            "start":                period_str,
            "end":                  period_str,
            "length":               RECORDS_PER_CALL
        }

        records = self._call_eia_api(EIA_BASE_URL, params)
        logger.info(
            f"Demand — {region} {period_str}: {len(records)} records"
        )
        return records


    def _fetch_generation_by_fuel(
        self,
        region: str,
        target_hour: datetime
    ) -> list:
        """
        Fetches electricity generation broken down by fuel type
        for one region for a specific hour.

        Generation by fuel = how much electricity was produced
        from coal, gas, solar, wind, nuclear etc.
        This is what powers our renewable vs fossil analysis.

        Args:
            region:      Grid region code
            target_hour: The hour we want data for

        Returns:
            List of generation records, one per fuel type
        """
        period_str = target_hour.strftime("%Y-%m-%dT%H")

        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": region,
            "start":                period_str,
            "end":                  period_str,
            "length":               RECORDS_PER_CALL
        }

        records = self._call_eia_api(EIA_FUEL_URL, params)
        logger.info(
            f"Generation — {region} {period_str}: "
            f"{len(records)} fuel type records"
        )
        return records


    def _records_to_dataframe(
        self,
        demand_records: list,
        generation_records: list,
        region: str,
        target_hour: datetime
    ) -> pd.DataFrame:
        """
        Combines demand and generation records into one
        clean DataFrame ready for Parquet.

        Why combine them?
        Having demand and generation in the same file means
        Spark only needs to read one file per region per hour
        instead of two. Fewer files = faster queries.

        Args:
            demand_records:     Raw demand records from EIA
            generation_records: Raw generation records from EIA
            region:             Grid region code
            target_hour:        The hour this data belongs to

        Returns:
            Combined DataFrame with consistent schema
        """
        all_records = demand_records + generation_records

        # If we got nothing back return empty DataFrame
        # adls_writer will skip writing empty DataFrames
        if not all_records:
            logger.warning(
                f"No records returned for {region} "
                f"at {target_hour.strftime('%Y-%m-%dT%H')}"
            )
            return pd.DataFrame()

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(all_records)

        # Rename columns to use underscores instead of hyphens
        # Hyphens in column names cause problems in SQL and Spark
        # "respondent-name" becomes "respondent_name"
        df.columns = [col.replace("-", "_") for col in df.columns]

        # Add metadata columns that are not in the raw API response
        # These help with debugging and auditing later
        df["ingested_at"]    = datetime.now(timezone.utc).isoformat()
        df["ingestion_hour"] = target_hour.strftime("%Y-%m-%dT%H")
        df["pipeline_run"]   = "hourly_eia_ingestor"

        # Convert value column to numeric
        # EIA returns value as a string — we want it as a number
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Basic validation — log any rows with null values
        null_values = df["value"].isna().sum()
        if null_values > 0:
            logger.warning(
                f"{null_values} null values in {region} data — "
                f"these will be handled in Silver cleaning"
            )

        return df


    def ingest_single_region(
    self,
    region: str,
    target_hour: datetime
) -> dict:
        """
        Runs the full ingestion for one region for one hour.
        Automatically handles regions with data publishing lags.
        """
        logger.info(
            f"Starting ingestion — {region} "
            f"({GRID_REGIONS.get(region, 'Unknown')}) "
            f"for {target_hour.strftime('%Y-%m-%dT%H')}"
        )

        # For each region, verify data actually exists
        # at the requested hour. If not, find the latest
        # available hour for that region instead.
        latest_hour = self._get_latest_available_hour(region)

        # If the latest available hour is more than 2 hours
        # behind what we requested, use the latest available
        # instead. This handles MISO, ISNE, SOCO lag.
        hours_behind = int(
            (target_hour - latest_hour).total_seconds() / 3600
        )

        if hours_behind > 2:
            logger.info(
                f"{region} data is {hours_behind}h behind — "
                f"using latest available: "
                f"{latest_hour.strftime('%Y-%m-%dT%H')}"
            )
            effective_hour = latest_hour
        else:
            effective_hour = target_hour

        # Check if we already have this data
        already_exists = self.writer.check_file_exists(
            source="eia",
            region=region,
            container=self.container,
            timestamp=effective_hour
        )

        if already_exists:
            logger.info(
                f"File already exists for {region} "
                f"{effective_hour.strftime('%Y-%m-%dT%H')} — skipping"
            )
            return {"status": "skipped", "reason": "already exists"}

        # Fetch demand data
        demand_records = self._fetch_demand(region, effective_hour)
        time.sleep(API_CALL_DELAY)

        # Fetch generation by fuel type
        generation_records = self._fetch_generation_by_fuel(
            region, effective_hour
        )
        time.sleep(API_CALL_DELAY)

        # Combine into DataFrame
        df = self._records_to_dataframe(
            demand_records,
            generation_records,
            region,
            effective_hour
        )

        # Write to ADLS Bronze
        result = self.writer.write_dataframe(
            df=df,
            source="eia",
            region=region,
            container=self.container,
            timestamp=effective_hour
        )

        return result

    def ingest_all_regions(self, target_hour: datetime = None) -> dict:
        """
        Runs ingestion for ALL 13 grid regions for one hour.
        This is the main entry point called by the Azure Function.

        Args:
            target_hour: Hour to ingest. Defaults to previous
                         complete hour (current hour - 1).
                         We use previous hour because EIA data
                         has a ~1 hour publishing lag.

        Returns:
            Summary dictionary with results per region
        """
        # Default to previous complete hour
        # EIA publishes data with roughly 1 hour delay
        # So at 3pm we fetch 2pm data — it is fully published
        if target_hour is None:
            now         = datetime.now(timezone.utc)
            target_hour = now.replace(
                minute=0, second=0, microsecond=0
            ) - timedelta(hours=1)

        logger.info("=" * 60)
        logger.info(
            f"Starting full ingestion run for "
            f"{target_hour.strftime('%Y-%m-%dT%H')} UTC"
        )
        logger.info(f"Regions: {list(GRID_REGIONS.keys())}")
        logger.info("=" * 60)

        results      = {}
        success_count = 0
        skip_count    = 0
        error_count   = 0

        for region in GRID_REGIONS:
            try:
                result = self.ingest_single_region(region, target_hour)
                results[region] = result

                if result.get("status") == "success":
                    success_count += 1
                elif result.get("status") == "skipped":
                    skip_count += 1

            except Exception as e:
                logger.error(
                    f"Failed to ingest {region}: {str(e)}"
                )
                results[region] = {
                    "status": "error",
                    "error":  str(e)
                }
                error_count += 1

        # Final summary
        summary = {
            "target_hour":   target_hour.isoformat(),
            "total_regions": len(GRID_REGIONS),
            "success":       success_count,
            "skipped":       skip_count,
            "errors":        error_count,
            "results":       results
        }

        logger.info("=" * 60)
        logger.info(
            f"Ingestion complete — "
            f"{success_count} success, "
            f"{skip_count} skipped, "
            f"{error_count} errors"
        )
        logger.info("=" * 60)

        return summary


# ─────────────────────────────────────────────
# Entry point when run directly
# python ingestion/functions/eia_ingestor.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    ingestor = EIAIngestor()
    summary  = ingestor.ingest_all_regions()

    print("\nFinal Summary:")
    print(f"  Target hour : {summary['target_hour']}")
    print(f"  Success     : {summary['success']}/{summary['total_regions']}")
    print(f"  Skipped     : {summary['skipped']}")
    print(f"  Errors      : {summary['errors']}")