import asyncio
import logging
import os
from dotenv import load_dotenv
from prometheus_client import start_http_server

from api_client import OpenDotaClient
from storage import LocalStorageProvider
from m_pipeline import Pipeline
from pipeline.utils.logger import setup_logging

logger = logging.getLogger(__name__)

async def main():
    # Load environment variables
    load_dotenv()
    
    # Setup logging (Loki + Console)
    listener = setup_logging("opendota-ingestor")

    # --- Prometheus Metrics Server ---
    # Start a server on port 8000 to expose metrics for Grafana Agent/Prometheus
    start_http_server(8000)
    logging.info("Prometheus metrics server started on port 8000.")
    
    api_key = os.getenv("OPENDOTA_API_KEY")
    my_account_id = os.getenv("MY_ACCOUNT_ID")
    ace_account_id = os.getenv("ACE_ACCOUNT_ID", "97590558")
    
    if not my_account_id or my_account_id == "YOUR_ACCOUNT_ID_HERE":
        logger.error("Please set MY_ACCOUNT_ID in your .env file.")
        return

    account_ids = [my_account_id, ace_account_id]
    
    # Instantiate storage (Local for now, easily swappable to GCP later)
    storage = LocalStorageProvider(base_dir="data")
    
    # Use the async context manager for the client
    async with OpenDotaClient(api_key=api_key) as client:
        # Create pipeline: 10 matches per account to keep the test small and within free tier easily
        pipeline = Pipeline(
            client=client,
            storage=storage,
            account_ids=account_ids,
            matches_per_account=10, 
            batch_size=5,
            max_concurrent_batches=2  # Limits in-flight requests to 2 batches * 5 = 10 concurrent reqs
        )
        
        logger.info("Starting OpenDota data ingestion pipeline...")
        await pipeline.run()
        logger.info("Pipeline completed successfully.")
        
        # --- HOLD THE DOOR ---
        # We wait 70 seconds so the Prometheus scraper (which runs every 60s)
        # has time to capture our final metrics before the program exits.
        logger.info("Waiting 70s for Prometheus scrape before exiting...")
        await asyncio.sleep(70)
        
        if 'listener' in locals():
            listener.stop()

if __name__ == "__main__":
    try:
        # Python 3.7+ standard way to run top-level async code
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
