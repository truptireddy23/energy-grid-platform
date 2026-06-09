# ingestion/batch/historical_backfill.py
#
# Purpose: One-time backfill of 2 years of historical EIA
#          electricity and Open-Meteo weather data into
#          ADLS Bronze zone.
#
# Run once to populate historical data for ML model training.
# Safe to re-run — skips files that already exist (resumable).
#
# Usage:
#   python ingestion/batch/historical_backfill.py
#   python ingestion/batch/historical_backfill.py --start 2024-01-01 --end 2024-06-30
#   python ingestion/batch/historical_backfill.py --eia-only
#   python ingestion/batch/historical_backfill.py --weather-only
#
# Estimated runtime: 4-8 hours for full 2-year backfill
# Safe to stop and restart at any time.

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)
    )))
)

from ingestion.functions.eia_ingestor     import EIAIngestor, GRID_REGIONS
from ingestion.functions.weather_ingestor import WeatherIngestor
from ingestion.functions.adls_writer      import ADLSWriter

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# Default backfill window — 2 years back from today
DEFAULT_END_DATE   = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(hours=1)

DEFAULT_START_DATE = DEFAULT_END_DATE - timedelta(days=365 * 2)

# Seconds to wait between hours during backfill
# Prevents hammering the API and hitting rate limits
# At 0.1s delay: 10 hours per second of processing
# Full 2yr backfill (~17,520 hours) takes ~30 minutes of API time
INTER_HOUR_DELAY = 0.1

# How often to log progress
# Logs a summary every N hours processed
LOG_EVERY_N_HOURS = 100

# How often to save a progress checkpoint
# Every N hours we write a checkpoint file so
# we know where to resume if the script crashes
CHECKPOINT_EVERY_N_HOURS = 500


class HistoricalBackfill:
    """
    Backfills historical EIA energy and weather data into
    ADLS Bronze zone.

    Designed to be resumable — skips hours that already
    have data in ADLS. Safe to stop and restart.
    """

    def __init__(self):
        self.eia_ingestor     = EIAIngestor()
        self.weather_ingestor = WeatherIngestor()
        self.writer           = ADLSWriter()
        self.container        = os.getenv(
            "AZURE_BRONZE_CONTAINER", "bronze"
        )

        logger.info("HistoricalBackfill initialised")


    def _generate_hours(
        self,
        start: datetime,
        end: datetime
    ) -> list:
        """
        Generates a list of every hour between start and end.

        Args:
            start: Start datetime (inclusive)
            end:   End datetime (inclusive)

        Returns:
            List of datetime objects, one per hour
        """
        hours   = []
        current = start.replace(minute=0, second=0, microsecond=0)

        while current <= end:
            hours.append(current)
            current += timedelta(hours=1)

        return hours


    def _check_eia_exists(
        self,
        region: str,
        hour: datetime
    ) -> bool:
        """
        Checks if EIA data already exists for a
        region and hour in ADLS Bronze.
        """
        return self.writer.check_file_exists(
            source="eia",
            region=region,
            container=self.container,
            timestamp=hour
        )


    def _check_weather_exists(
        self,
        region: str,
        hour: datetime
    ) -> bool:
        """
        Checks if weather data already exists for a
        region and hour in ADLS Bronze.
        """
        return self.writer.check_file_exists(
            source="weather",
            region=region,
            container=self.container,
            timestamp=hour
        )


    def _save_checkpoint(
        self,
        hour: datetime,
        stats: dict
    ):
        """
        Saves a checkpoint file so we know where
        to resume if the script crashes.

        The checkpoint is a simple text file in the
        config folder with the last completed hour.
        """
        checkpoint_path = os.path.join(
            os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )),
            "config",
            "backfill_checkpoint.txt"
        )

        with open(checkpoint_path, "w") as f:
            f.write(f"last_completed_hour={hour.isoformat()}\n")
            f.write(f"eia_success={stats['eia_success']}\n")
            f.write(f"eia_skipped={stats['eia_skipped']}\n")
            f.write(f"weather_success={stats['weather_success']}\n")
            f.write(f"weather_skipped={stats['weather_skipped']}\n")
            f.write(f"errors={stats['errors']}\n")

        logger.info(
            f"Checkpoint saved — last completed hour: "
            f"{hour.strftime('%Y-%m-%dT%H')}"
        )


    def _load_checkpoint(self) -> datetime:
        """
        Loads the last checkpoint if it exists.
        Returns the last completed hour so we can
        resume from where we left off.

        Returns:
            datetime of last completed hour, or None
            if no checkpoint exists
        """
        checkpoint_path = os.path.join(
            os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )),
            "config",
            "backfill_checkpoint.txt"
        )

        if not os.path.exists(checkpoint_path):
            return None

        with open(checkpoint_path, "r") as f:
            lines = f.readlines()

        for line in lines:
            if line.startswith("last_completed_hour="):
                hour_str = line.split("=")[1].strip()
                return datetime.fromisoformat(hour_str)

        return None


    def run_eia_backfill(
        self,
        start: datetime,
        end: datetime,
        resume: bool = True
    ) -> dict:
        """
        Backfills EIA data for all 13 regions
        between start and end dates.

        Args:
            start:  Start datetime
            end:    End datetime
            resume: If True skip existing files (default)
                    If False overwrite everything

        Returns:
            Statistics dictionary
        """
        hours = self._generate_hours(start, end)
        total_hours = len(hours)

        logger.info("=" * 60)
        logger.info(f"Starting EIA backfill")
        logger.info(f"Start : {start.strftime('%Y-%m-%dT%H')}")
        logger.info(f"End   : {end.strftime('%Y-%m-%dT%H')}")
        logger.info(f"Hours : {total_hours:,}")
        logger.info(
            f"Regions: {len(GRID_REGIONS)} "
            f"× {total_hours:,} hours = "
            f"{len(GRID_REGIONS) * total_hours:,} total files"
        )
        logger.info("=" * 60)

        stats = {
            "eia_success": 0,
            "eia_skipped": 0,
            "weather_success": 0,
            "weather_skipped": 0,
            "errors": 0
        }

        for hour_idx, hour in enumerate(hours):

            # Progress logging every N hours
            if hour_idx % LOG_EVERY_N_HOURS == 0:
                pct = (hour_idx / total_hours) * 100
                logger.info(
                    f"Progress: {hour_idx:,}/{total_hours:,} hours "
                    f"({pct:.1f}%) — "
                    f"current: {hour.strftime('%Y-%m-%dT%H')}"
                )

            # Process each region for this hour
            for region in GRID_REGIONS:
                try:
                    # Skip if file exists and resume mode is on
                    if resume and self._check_eia_exists(
                        region, hour
                    ):
                        stats["eia_skipped"] += 1
                        continue

                    # Use EIA ingestor's single region method
                    # Pass the hour explicitly — no lag detection
                    # needed for historical data
                    result = self.eia_ingestor.ingest_single_region(
                        region=region,
                        target_hour=hour
                    )

                    if result.get("status") == "success":
                        stats["eia_success"] += 1
                    elif result.get("status") == "skipped":
                        stats["eia_skipped"] += 1

                except Exception as e:
                    logger.error(
                        f"EIA backfill error — "
                        f"{region} {hour.strftime('%Y-%m-%dT%H')}: "
                        f"{str(e)}"
                    )
                    stats["errors"] += 1

                time.sleep(INTER_HOUR_DELAY)

            # Save checkpoint every N hours
            if hour_idx % CHECKPOINT_EVERY_N_HOURS == 0:
                self._save_checkpoint(hour, stats)

        # Final checkpoint
        self._save_checkpoint(end, stats)

        logger.info("=" * 60)
        logger.info(f"EIA backfill complete")
        logger.info(f"Success : {stats['eia_success']:,}")
        logger.info(f"Skipped : {stats['eia_skipped']:,}")
        logger.info(f"Errors  : {stats['errors']:,}")
        logger.info("=" * 60)

        return stats


    def run_weather_backfill(
        self,
        start: datetime,
        end: datetime,
        resume: bool = True
    ) -> dict:
        """
        Backfills weather data for all 13 regions
        between start and end dates.

        Note: Open-Meteo allows requesting up to 1 year
        of historical data on the free tier.
        For dates beyond 1 year we use the archive endpoint.

        Args:
            start:  Start datetime
            end:    End datetime
            resume: If True skip existing files

        Returns:
            Statistics dictionary
        """
        hours       = self._generate_hours(start, end)
        total_hours = len(hours)

        logger.info("=" * 60)
        logger.info(f"Starting weather backfill")
        logger.info(f"Start : {start.strftime('%Y-%m-%dT%H')}")
        logger.info(f"End   : {end.strftime('%Y-%m-%dT%H')}")
        logger.info(f"Hours : {total_hours:,}")
        logger.info("=" * 60)

        stats = {
            "eia_success":     0,
            "eia_skipped":     0,
            "weather_success": 0,
            "weather_skipped": 0,
            "errors":          0
        }

        # For weather we batch by DAY not by hour
        # Open-Meteo returns a full day per API call
        # so we call once per region per day and extract
        # all 24 hours from that one response
        # This reduces API calls from 17,520 to 730 for 2 years
        from ingestion.functions.weather_ingestor import (
        REGION_COORDINATES, WEATHER_VARIABLES, API_CALL_DELAY
        )
        import requests
        import pandas as pd

        # Use archive endpoint for historical data
        # The forecast endpoint only serves recent + future dates
        # Archive endpoint has full history back to 1940 — free, no key
        ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

        # Get unique days in the range
        days = sorted(set(h.date() for h in hours))
        logger.info(
            f"Weather batches by day: {len(days)} days × "
            f"{len(REGION_COORDINATES)} regions = "
            f"{len(days) * len(REGION_COORDINATES)} API calls"
        )

        for day_idx, day in enumerate(days):

            if day_idx % 30 == 0:
                pct = (day_idx / len(days)) * 100
                logger.info(
                    f"Weather progress: {day_idx}/{len(days)} days "
                    f"({pct:.1f}%) — {day}"
                )

            date_str = day.strftime("%Y-%m-%d")

            for region in REGION_COORDINATES:

                # Check if all 24 hours exist for this day/region
                # If first and last hour exist assume middle ones do too
                day_start = datetime(
                    day.year, day.month, day.day, 0,
                    tzinfo=timezone.utc
                )
                day_end = datetime(
                    day.year, day.month, day.day, 23,
                    tzinfo=timezone.utc
                )

                if resume and (
                    self._check_weather_exists(region, day_start) and
                    self._check_weather_exists(region, day_end)
                ):
                    stats["weather_skipped"] += 24
                    continue

                city_name, latitude, longitude = (
                    REGION_COORDINATES[region]
                )

                # Fetch full day from Open-Meteo
                params = {
                    "latitude":   latitude,
                    "longitude":  longitude,
                    "hourly":     ",".join(WEATHER_VARIABLES),
                    "start_date": date_str,
                    "end_date":   date_str,
                    "timezone":   "UTC",
                    "timeformat": "unixtime",
                }

                try:
                    response = requests.get(
                        ARCHIVE_URL,
                        params=params,
                        timeout=30
                    )

                    if response.status_code != 200:
                        logger.warning(
                            f"Weather API error {response.status_code} "
                            f"for {region} on {date_str}"
                        )
                        stats["errors"] += 1
                        continue

                    api_response = response.json()
                    hourly       = api_response.get("hourly", {})
                    timestamps   = hourly.get("time", [])

                    # Write each hour individually
                    for hour in [
                        h for h in hours
                        if h.date() == day
                    ]:
                        target_unix = int(hour.timestamp())

                        if target_unix not in timestamps:
                            continue

                        hour_index = timestamps.index(target_unix)

                        # Build row for this hour
                        row = {
                            "period":      hour.strftime(
                                "%Y-%m-%dT%H"
                            ),
                            "respondent":  region,
                            "city":        city_name,
                            "latitude":    api_response.get(
                                "latitude"
                            ),
                            "longitude":   api_response.get(
                                "longitude"
                            ),
                            "elevation_m": api_response.get(
                                "elevation"
                            ),
                        }

                        for variable in WEATHER_VARIABLES:
                            values = hourly.get(variable, [])
                            row[variable] = (
                                values[hour_index]
                                if values else None
                            )

                        from datetime import timezone as tz
                        row["ingested_at"]    = datetime.now(
                            tz.utc
                        ).isoformat()
                        row["ingestion_hour"] = hour.strftime(
                            "%Y-%m-%dT%H"
                        )
                        row["pipeline_run"]   = "historical_backfill"

                        df = pd.DataFrame([row])
                        weather_values = [row.get(v) for v in WEATHER_VARIABLES]
                        if all(v is None for v in weather_values):
                            logger.warning(
                                f"All weather values null for "
                                f"{region} {hour.strftime('%Y-%m-%dT%H')} — skipping"
                            )
                            continue

                        result = self.writer.write_dataframe(
                            df=df,
                            source="weather",
                            region=region,
                            container=self.container,
                            timestamp=hour
                        )

                        if result.get("status") == "success":
                            stats["weather_success"] += 1

                except Exception as e:
                    logger.error(
                        f"Weather backfill error — "
                        f"{region} {date_str}: {str(e)}"
                    )
                    stats["errors"] += 1

                time.sleep(API_CALL_DELAY)

        logger.info("=" * 60)
        logger.info(f"Weather backfill complete")
        logger.info(f"Success : {stats['weather_success']:,}")
        logger.info(f"Skipped : {stats['weather_skipped']:,}")
        logger.info(f"Errors  : {stats['errors']:,}")
        logger.info("=" * 60)

        return stats


    def run_full_backfill(
        self,
        start: datetime = None,
        end: datetime   = None,
        eia_only: bool     = False,
        weather_only: bool = False,
        resume: bool       = True
    ) -> dict:
        """
        Runs the full backfill — EIA and weather.

        Args:
            start:       Start datetime (default 2 years ago)
            end:         End datetime (default yesterday)
            eia_only:    Only backfill EIA data
            weather_only: Only backfill weather data
            resume:      Skip existing files (default True)
        """
        start = start or DEFAULT_START_DATE
        end   = end   or DEFAULT_END_DATE

        # Check for existing checkpoint and offer to resume
        checkpoint_hour = self._load_checkpoint()
        if checkpoint_hour and resume:
            logger.info(
                f"Checkpoint found — last completed hour: "
                f"{checkpoint_hour.strftime('%Y-%m-%dT%H')}"
            )
            logger.info(
                f"Resuming from checkpoint. "
                f"To start fresh use resume=False"
            )
            # Resume from the hour after last checkpoint
            start = checkpoint_hour + timedelta(hours=1)

        all_stats = {}

        if not weather_only:
            logger.info("Phase 1: EIA historical backfill")
            eia_stats      = self.run_eia_backfill(
                start, end, resume
            )
            all_stats["eia"] = eia_stats

        if not eia_only:
            logger.info("Phase 2: Weather historical backfill")
            weather_stats          = self.run_weather_backfill(
                start, end, resume
            )
            all_stats["weather"] = weather_stats

        logger.info("=" * 60)
        logger.info("FULL BACKFILL COMPLETE")
        if "eia" in all_stats:
            logger.info(
                f"EIA     — "
                f"success: {all_stats['eia']['eia_success']:,}, "
                f"skipped: {all_stats['eia']['eia_skipped']:,}, "
                f"errors: {all_stats['eia']['errors']:,}"
            )
        if "weather" in all_stats:
            logger.info(
                f"Weather — "
                f"success: {all_stats['weather']['weather_success']:,}, "
                f"skipped: {all_stats['weather']['weather_skipped']:,}, "
                f"errors: {all_stats['weather']['errors']:,}"
            )
        logger.info("=" * 60)

        return all_stats


# ─────────────────────────────────────────────
# Command line interface
# ─────────────────────────────────────────────
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Backfill historical EIA and weather data"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date YYYY-MM-DD (default: 2 years ago)"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date YYYY-MM-DD (default: yesterday)"
    )
    parser.add_argument(
        "--eia-only",
        action="store_true",
        help="Only backfill EIA data"
    )
    parser.add_argument(
        "--weather-only",
        action="store_true",
        help="Only backfill weather data"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore checkpoint"
    )

    args = parser.parse_args()

    # Parse date arguments if provided
    start_dt = None
    end_dt   = None

    if args.start:
        start_dt = datetime.strptime(
            args.start, "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

    if args.end:
        end_dt = datetime.strptime(
            args.end, "%Y-%m-%d"
        ).replace(hour=23, tzinfo=timezone.utc)

    # Run backfill
    backfill = HistoricalBackfill()
    backfill.run_full_backfill(
        start        = start_dt,
        end          = end_dt,
        eia_only     = args.eia_only,
        weather_only = args.weather_only,
        resume       = not args.no_resume
    )