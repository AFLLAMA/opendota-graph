import asyncio
import logging
import os
import sys
import queue
from logging.handlers import QueueHandler, QueueListener

from dotenv import load_dotenv
import logging_loki
from prometheus_client import start_http_server

from api_client import OpenDotaClient
from storage import LocalStorageProvider
from pipeline import Pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

async def main():
    # Load environment variables
    load_dotenv()
    
    # --- Grafana Loki Setup ---
    loki_url = os.getenv("GRAFANA_LOKI_URL")
    loki_user = os.getenv("GRAFANA_LOKI_USER")
    loki_key = os.getenv("GRAFANA_LOKI_API_KEY")

    if loki_url and loki_user and loki_key:
        # 1. Create the actual Loki handler (which is synchronous)
        loki_inner_handler = logging_loki.LokiHandler(
            url=loki_url,
            tags={"application": "opendota-ingestor"},
            auth=(loki_user, loki_key),
            version="1",
        )
        
        # 2. Create a local queue for logs
        log_queue = queue.Queue(-1)  # Unlimited size
        
        # 3. Create a QueueHandler that just pushes logs to the local queue
        queue_handler = QueueHandler(log_queue)
        
        # 4. Create a QueueListener that runs in a background thread and sends logs from the queue to Loki
        listener = QueueListener(log_queue, loki_inner_handler)
        listener.start()
        
        # 5. Add the non-blocking queue_handler to the root logger
        logging.getLogger().addHandler(queue_handler)
        logger.info("Non-blocking Grafana Loki logging initialized.")

    # --- Prometheus Metrics Server ---
    # Start a server on port 8000 to expose metrics for Grafana Agent/Prometheus
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000.")
    
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
