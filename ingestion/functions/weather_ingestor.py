# ingestion/functions/weather_ingestor.py
#
# Purpose: Fetches hourly weather data from Open-Meteo API
#          for all 13 US grid regions and writes to ADLS
#          Bronze zone as partitioned Parquet files.
#
# Why weather data?
#   Temperature is the #1 driver of electricity demand.
#   Cold snaps → heating spikes. Heatwaves → AC spikes.
#   Wind speed affects wind generation output.
#   Cloud cover affects solar generation output.
#   Without weather our demand forecast model is blind.
#
# API: Open-Meteo (open-meteo.com)
#   - No API key needed
#   - No rate limits for reasonable usage
#   - Free forever
#
# Output:
#   bronze/weather/year=YYYY/month=MM/day=DD/
#          region=XX/hour=HH/data.parquet

import os
import sys
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)
    )))
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
# ─────────────────────────────────────────────

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Representative city coordinates for each grid region
# Format: region_code: (name, latitude, longitude)
#
# Why one city per region?
# Each grid region covers a large geographic area.
# We use the largest population centre as the
# representative point — this approximates the
# demand-weighted average conditions for the region.
#
REGION_COORDINATES = {
    "ERCO": ("Dallas TX",         32.7767,  -96.7970),
    "CAL":  ("Los Angeles CA",    34.0522, -118.2437),
    "PJM":  ("Philadelphia PA",   39.9526,  -75.1652),
    "MISO": ("Chicago IL",        41.8781,  -87.6298),
    "NYIS": ("New York NY",       40.7128,  -74.0060),
    "ISNE": ("Boston MA",         42.3601,  -71.0589),
    "SWPP": ("Oklahoma City OK",  35.4676,  -97.5164),
    "BPAT": ("Portland OR",       45.5051, -122.6750),
    "SOCO": ("Atlanta GA",        33.7490,  -84.3880),
    "TVA":  ("Nashville TN",      36.1627,  -86.7816),
    "DUK":  ("Charlotte NC",      35.2271,  -80.8431),
    "FPL":  ("Miami FL",          25.7617,  -80.1918),
    "SC":   ("Columbia SC",       34.0007,  -81.0348),
}

# Weather variables we request from Open-Meteo
# Each becomes a column in our DataFrame
WEATHER_VARIABLES = [
    "temperature_2m",        # Air temp at 2 metres height (°C)
    "relativehumidity_2m",   # Humidity % — affects perceived temp
    "windspeed_10m",         # Wind speed at 10m height (km/h)
    "winddirection_10m",     # Wind direction (degrees)
    "cloudcover",            # Total cloud cover (%)
    "precipitation",         # Rainfall (mm)
    "weathercode",           # WMO weather code (0=clear, 95=storm)
    "apparent_temperature",  # Feels-like temperature (°C)
    "surface_pressure",      # Atmospheric pressure (hPa)
]

# Seconds between API calls — be polite to free API
API_CALL_DELAY = 0.5

# Max retries on failure
MAX_RETRIES = 3
RETRY_DELAY = 5


class WeatherIngestor:
    """
    Fetches hourly weather data from Open-Meteo for all
    13 US grid regions and writes to ADLS Bronze zone.

    Weather data enriches EIA energy data downstream:
    - Temperature → demand forecasting feature
    - Wind speed  → wind generation forecast feature
    - Cloud cover → solar generation forecast feature
    """

    def __init__(self):
        self.writer    = ADLSWriter()
        self.container = os.getenv("AZURE_BRONZE_CONTAINER", "bronze")
        logger.info("WeatherIngestor initialised")
        logger.info(f"Tracking {len(REGION_COORDINATES)} grid regions")


    def _call_open_meteo(
        self,
        latitude: float,
        longitude: float,
        target_hour: datetime
    ) -> dict:
        """
        Calls Open-Meteo API for one location for one hour.

        Open-Meteo returns hourly data for a date range.
        We request just the target date to minimise
        response size, then filter to the exact hour.

        Args:
            latitude:    Location latitude
            longitude:   Location longitude
            target_hour: The hour we want weather for

        Returns:
            Dictionary of weather values for that hour
            or empty dict if call fails
        """
        # Format date for Open-Meteo
        # It expects: YYYY-MM-DD
        date_str = target_hour.strftime("%Y-%m-%d")

        params = {
            "latitude":       latitude,
            "longitude":      longitude,
            "hourly":         ",".join(WEATHER_VARIABLES),
            "start_date":     date_str,
            "end_date":       date_str,
            "timezone":       "UTC",
            "timeformat":     "unixtime",  # returns timestamps as integers
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    OPEN_METEO_URL,
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    return response.json()

                else:
                    logger.warning(
                        f"Open-Meteo returned {response.status_code} "
                        f"on attempt {attempt}/{MAX_RETRIES}"
                    )
                    time.sleep(RETRY_DELAY)

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Open-Meteo timed out — attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY)

            except requests.exceptions.ConnectionError:
                logger.warning(
                    f"Connection error — attempt {attempt}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY)

        logger.error(
            f"Open-Meteo failed after {MAX_RETRIES} attempts "
            f"for ({latitude}, {longitude})"
        )
        return {}


    def _extract_hour_from_response(
        self,
        api_response: dict,
        target_hour: datetime,
        region: str,
        city_name: str
    ) -> pd.DataFrame:
        """
        Extracts data for one specific hour from the
        Open-Meteo response.

        Why extract one hour?
        Open-Meteo returns all 24 hours of the day even
        when we only want one. We find the index of our
        target hour in the timestamps array and extract
        just that row.

        Args:
            api_response: Full response from Open-Meteo
            target_hour:  The specific hour we want
            region:       Grid region code
            city_name:    Representative city name

        Returns:
            Single-row DataFrame for that hour
        """
        if not api_response:
            logger.warning(f"Empty API response for {region}")
            return pd.DataFrame()

        hourly = api_response.get("hourly", {})
        if not hourly:
            logger.warning(f"No hourly data in response for {region}")
            return pd.DataFrame()

        # Open-Meteo returns timestamps as Unix epoch integers
        # when timeformat=unixtime
        # Unix epoch = seconds since January 1, 1970
        # We need to find the index of our target hour
        timestamps = hourly.get("time", [])
        target_unix = int(target_hour.timestamp())

        # Find which index in the array matches our target hour
        try:
            hour_index = timestamps.index(target_unix)
        except ValueError:
            # Target hour not in response
            # This can happen if target_hour is in the future
            # or if Open-Meteo does not have data for that time
            logger.warning(
                f"Target hour {target_hour.strftime('%Y-%m-%dT%H')} "
                f"not found in Open-Meteo response for {region}. "
                f"Available range: "
                f"{timestamps[0] if timestamps else 'none'} to "
                f"{timestamps[-1] if timestamps else 'none'}"
            )
            return pd.DataFrame()

        # Build a single-row dictionary with all weather values
        # at the target hour index
        row = {
            "period":         target_hour.strftime("%Y-%m-%dT%H"),
            "respondent":     region,
            "city":           city_name,
            "latitude":       api_response.get("latitude"),
            "longitude":      api_response.get("longitude"),
            "elevation_m":    api_response.get("elevation"),
        }

        # Extract each weather variable at our target index
        for variable in WEATHER_VARIABLES:
            values = hourly.get(variable, [])
            row[variable] = values[hour_index] if values else None

        # Add metadata columns — same pattern as eia_ingestor
        row["ingested_at"]    = datetime.now(timezone.utc).isoformat()
        row["ingestion_hour"] = target_hour.strftime("%Y-%m-%dT%H")
        row["pipeline_run"]   = "hourly_weather_ingestor"

        # Convert to single-row DataFrame
        df = pd.DataFrame([row])
        return df


    def ingest_single_region(
        self,
        region: str,
        target_hour: datetime
    ) -> dict:
        """
        Fetches and writes weather data for one region
        for one hour.

        Args:
            region:      Grid region code e.g. 'ERCO'
            target_hour: The hour to fetch weather for

        Returns:
            Result dictionary from adls_writer
        """
        if region not in REGION_COORDINATES:
            logger.warning(f"No coordinates defined for {region}")
            return {"status": "error", "reason": "no coordinates"}

        city_name, latitude, longitude = REGION_COORDINATES[region]

        logger.info(
            f"Fetching weather — {region} ({city_name}) "
            f"for {target_hour.strftime('%Y-%m-%dT%H')}"
        )

        # Call Open-Meteo
        api_response = self._call_open_meteo(
            latitude, longitude, target_hour
        )
        time.sleep(API_CALL_DELAY)

        # Extract target hour from response
        df = self._extract_hour_from_response(
            api_response, target_hour, region, city_name
        )

        # Write to ADLS Bronze
        result = self.writer.write_dataframe(
            df=df,
            source="weather",
            region=region,
            container=self.container,
            timestamp=target_hour
        )

        return result


    def ingest_all_regions(
        self,
        target_hour: datetime = None
    ) -> dict:
        """
        Fetches weather for all 13 regions for one hour.
        Entry point called by Azure Function.

        Args:
            target_hour: Hour to fetch. Defaults to
                         current hour (weather is real-time,
                         no publishing lag unlike EIA)

        Returns:
            Summary dictionary
        """
        # Weather is real-time — use current hour not previous
        # Open-Meteo has no publishing delay
        if target_hour is None:
            target_hour = datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )

        logger.info("=" * 60)
        logger.info(
            f"Starting weather ingestion for "
            f"{target_hour.strftime('%Y-%m-%dT%H')} UTC"
        )
        logger.info("=" * 60)

        results       = {}
        success_count = 0
        error_count   = 0

        for region in REGION_COORDINATES:
            try:
                result = self.ingest_single_region(
                    region, target_hour
                )
                results[region] = result

                if result.get("status") == "success":
                    success_count += 1

            except Exception as e:
                logger.error(f"Failed weather for {region}: {str(e)}")
                results[region] = {
                    "status": "error",
                    "error":  str(e)
                }
                error_count += 1

        summary = {
            "target_hour":   target_hour.isoformat(),
            "total_regions": len(REGION_COORDINATES),
            "success":       success_count,
            "errors":        error_count,
            "results":       results
        }

        logger.info("=" * 60)
        logger.info(
            f"Weather ingestion complete — "
            f"{success_count} success, "
            f"{error_count} errors"
        )
        logger.info("=" * 60)

        return summary


# ─────────────────────────────────────────────
# Entry point
# python ingestion/functions/weather_ingestor.py
# ─────────────────────────────────────────────
if __name__ == "__main__":
    ingestor = WeatherIngestor()
    summary  = ingestor.ingest_all_regions()

    print("\nFinal Summary:")
    print(f"  Target hour : {summary['target_hour']}")
    print(f"  Success     : {summary['success']}/{summary['total_regions']}")
    print(f"  Errors      : {summary['errors']}")

    print("\nPer region breakdown:")
    for region, result in summary['results'].items():
        print(
            f"  {region:<6} -> "
            f"status={result.get('status'):<10} "
            f"reason={result.get('reason','')}"
        )