# ingestion/functions/adls_writer.py
#
# Purpose: Reusable module for writing DataFrames to ADLS Gen2
#          as Parquet files with correct partition structure.
#
# Called by: eia_ingestor.py, weather_ingestor.py
# Never run directly.
#
# Partition structure created:
#   bronze/eia/year=YYYY/month=MM/day=DD/region=XX/data.parquet
#   bronze/weather/year=YYYY/month=MM/day=DD/region=XX/data.parquet

import os
import io
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

# Load environment variables from .env file
load_dotenv()

# Set up logging
# This means every action is recorded with a timestamp
# In production this goes to Azure Monitor — for now it prints to terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


class ADLSWriter:
    """
    Handles all write operations to Azure Data Lake Storage Gen2.

    Why a class?
    We need to hold the connection client as state — creating a new
    BlobServiceClient on every write would be wasteful. The class
    creates the connection once and reuses it for all writes.
    """

    def __init__(self):
        """
        Initialise the ADLS connection when the class is created.
        Reads credentials from environment variables — never hardcoded.
        """
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

        # Validate credentials exist before trying to connect
        # Fail fast with a clear message rather than a cryptic Azure error
        if not account_name or not account_key:
            raise ValueError(
                "AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY "
                "must be set in your .env file"
            )

        # Build connection string and create the client
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix=core.windows.net"
        )

        self.client       = BlobServiceClient.from_connection_string(connection_string)
        self.account_name = account_name

        logger.info(f"ADLSWriter initialised — connected to {account_name}")


    def _build_partition_path(
        self,
        source: str,
        region: str,
        timestamp: datetime
    ) -> str:
        """
        Builds the partition path for a given source, region, and timestamp.

        Why this partition structure?
        Spark and Synapse use partition pruning — when you query
        "give me Texas data from May 2026" they read ONLY the
        year=2026/month=05 folder, skipping everything else.
        This makes queries dramatically faster as data grows.

        Args:
            source:    Data source — 'eia' or 'weather'
            region:    Grid region code — e.g. 'ERCO', 'CALI'
            timestamp: The datetime this data belongs to

        Returns:
            Partition path string e.g.
            'eia/year=2026/month=05/day=31/region=ERCO/data.parquet'
        """
        return (
            f"{source}/"
            f"year={timestamp.strftime('%Y')}/"
            f"month={timestamp.strftime('%m')}/"
            f"day={timestamp.strftime('%d')}/"
            f"region={region.upper()}/"
            f"data.parquet"
        )


    def write_dataframe(
        self,
        df: pd.DataFrame,
        source: str,
        region: str,
        container: str,
        timestamp: datetime = None
    ) -> dict:
        """
        Converts a Pandas DataFrame to Parquet and writes it to ADLS.

        This is idempotent — running it multiple times for the same
        source/region/timestamp always produces exactly one file.
        Existing files are overwritten, never duplicated.

        Args:
            df:        DataFrame containing the data to write
            source:    'eia' or 'weather'
            region:    Grid region code e.g. 'ERCO'
            container: ADLS container name e.g. 'bronze'
            timestamp: Datetime for partition — defaults to now (UTC)

        Returns:
            Dictionary with details of what was written
        """

        # Default to current UTC time if no timestamp provided
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Validate the DataFrame is not empty
        # Writing an empty file wastes storage and confuses downstream jobs
        if df.empty:
            logger.warning(
                f"Empty DataFrame for {source}/{region} — nothing written"
            )
            return {"status": "skipped", "reason": "empty dataframe"}

        # Build the partition path
        blob_path = self._build_partition_path(source, region, timestamp)

        # Convert DataFrame to Parquet bytes in memory
        # We use a BytesIO buffer — this avoids writing a temp file to disk
        # io.BytesIO() is like a file in RAM — faster and cleaner
        parquet_buffer = io.BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine="pyarrow",    # PyArrow is the fastest Parquet engine
            index=False,         # Do not write the DataFrame index as a column
            compression="snappy" # Snappy compression — fast read/write, good ratio
        )
        parquet_buffer.seek(0)  # Reset buffer position to beginning before upload

        # Write to ADLS — overwrite=True makes this idempotent
        try:
            blob_client = self.client.get_blob_client(
                container=container,
                blob=blob_path
            )
            blob_client.upload_blob(
                parquet_buffer,
                overwrite=True,           # Overwrite if file exists — idempotent
                blob_type="BlockBlob",    # BlockBlob is standard for files
                content_type="application/octet-stream"
            )

            # Build result summary
            result = {
                "status":     "success",
                "container":  container,
                "path":       blob_path,
                "rows":       len(df),
                "columns":    list(df.columns),
                "timestamp":  timestamp.isoformat(),
                "account":    self.account_name
            }

            logger.info(
                f"✓ Written {len(df)} rows to "
                f"{container}/{blob_path}"
            )
            return result

        except AzureError as e:
            logger.error(f"ADLS write failed for {blob_path}: {str(e)}")
            raise


    def write_multiple_regions(
        self,
        data_by_region: dict,
        source: str,
        container: str,
        timestamp: datetime = None
    ) -> list:
        """
        Writes data for multiple regions in one call.

        The EIA ingestor pulls all 13 regions at once.
        This method loops through them and writes each
        to its own partition — one file per region per hour.

        Args:
            data_by_region: Dictionary of {region_code: DataFrame}
            source:         'eia' or 'weather'
            container:      ADLS container name
            timestamp:      Datetime for all partitions

        Returns:
            List of result dictionaries, one per region
        """
        results = []

        for region, df in data_by_region.items():
            result = self.write_dataframe(
                df=df,
                source=source,
                region=region,
                container=container,
                timestamp=timestamp
            )
            results.append(result)

        # Summary log
        successful = sum(1 for r in results if r.get("status") == "success")
        logger.info(
            f"Batch write complete — "
            f"{successful}/{len(results)} regions written successfully"
        )
        return results


    def check_file_exists(
        self,
        source: str,
        region: str,
        container: str,
        timestamp: datetime
    ) -> bool:
        """
        Checks if a file already exists for a given partition.

        Used by the ingestor to skip re-fetching data that
        was already successfully written — avoids unnecessary
        API calls when the pipeline retries after a partial failure.

        Args:
            source:    'eia' or 'weather'
            region:    Grid region code
            container: ADLS container name
            timestamp: Datetime to check

        Returns:
            True if file exists, False if not
        """
        blob_path   = self._build_partition_path(source, region, timestamp)
        blob_client = self.client.get_blob_client(
            container=container,
            blob=blob_path
        )

        try:
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False