# config/test_adls_connection.py
# Purpose: Verify Python can connect to ADLS Gen2 and write/read files
# Run with: python config/test_adls_connection.py

import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER    = os.getenv("AZURE_BRONZE_CONTAINER")

def test_adls_connection():
    print("=" * 50)
    print("Testing ADLS Gen2 Connection")
    print("=" * 50)

    # Build the connection string
    # This is how azure-storage-blob authenticates
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )

    # Create the client that talks to your storage account
    client = BlobServiceClient.from_connection_string(connection_string)

    print(f"\nConnecting to: {ACCOUNT_NAME}")
    print(f"Container: {CONTAINER}\n")

    # Write a small test file to Bronze
    test_content = f"ADLS connection test — {datetime.now(timezone.utc).isoformat()}"
    blob_path    = "eia/connection_test.txt"

    blob_client = client.get_blob_client(
        container=CONTAINER,
        blob=blob_path
    )
    blob_client.upload_blob(test_content, overwrite=True)
    print(f"✓ Successfully wrote test file to: bronze/{blob_path}")

    # Read it back to confirm
    downloaded = blob_client.download_blob().readall().decode("utf-8")
    print(f"✓ Successfully read it back: {downloaded}")

    # Clean up the test file
    blob_client.delete_blob()
    print(f"✓ Test file cleaned up")

    print("\n✓ ADLS Gen2 connection is working correctly")
    print("✓ Python can read and write to your Bronze container")
    print("✓ Ready to build the ingestion pipeline")

if __name__ == "__main__":
    test_adls_connection()