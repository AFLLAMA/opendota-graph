import asyncio
import logging
from typing import List

from api_client import OpenDotaClient
from storage import StorageProvider
from prometheus_client import Counter, Gauge, Summary

# Prometheus Metrics Definitions
MATCHES_PROCESSED = Counter('opendota_matches_processed_total', 'Total number of matches processed', ['account_id'])
QUEUE_DEPTH = Gauge('opendota_queue_depth', 'Current items in the match queue')
BATCH_TIME = Summary('opendota_batch_processing_seconds', 'Time spent processing a microbatch')
API_ERRORS = Counter('opendota_api_errors_total', 'Total API errors encountered')

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Orchestrates the asynchronous ingestion pipeline.
    Uses a multi-stage producer-consumer architecture with microbatching.

    Architecture Diagram:
    +------------+       +-------------+       +--------------+       +-------------------+
    |  Producer  | ----> | Item Queue  | ----> | Microbatcher | ----> |    Batch Queue    |
    | (Fetch IDs)|       | (Match IDs) |       | (Group by 5) |       | (List of 5 IDs)   |
    +------------+       +-------------+       +--------------+       +---------+---------+
                                                                                |
                                                                                v
                                                                      +-------------------+
                                                                      |   Consumer Pool   |
                                                                      | (asyncio.TaskGroup)|
                                                                      +---------+---------+
                                                                                |
                                         +----------------+                     |
                                         | Storage (JSON) | <-------------------+
                                         +----------------+
    """
    def __init__(
        self,
        client: OpenDotaClient,
        storage: StorageProvider,
        account_ids: List[str],
        matches_per_account: int = 15,
        batch_size: int = 5,
        max_concurrent_batches: int = 3
    ):
        self.client = client
        self.storage = storage
        self.account_ids = account_ids
        self.matches_per_account = matches_per_account
        self.batch_size = batch_size
        
        # Queues
        self.item_queue: asyncio.Queue = asyncio.Queue()
        self.batch_queue: asyncio.Queue = asyncio.Queue()
        
        # Semaphore to limit concurrent batch processing
        self.semaphore = asyncio.Semaphore(max_concurrent_batches)

    async def producer(self):
        """
        STAGE 1: Producer
        Fetches match history from the API and pushes individual match IDs into the item queue.
        """
        logger.info("Producer started.")
        for account_id in self.account_ids:
            logger.info(f"Fetching matches for account {account_id}...")
            # We iterate over the match history provided by the client's async generator.
            # This is non-blocking; the producer yields control back to the event loop
            # while waiting for API responses.
            async for match in self.client.get_match_history(account_id, limit=self.matches_per_account):
                match_id = match.get("match_id")
                if match_id:
                    # Place the (account, match_id) tuple into the first queue.
                    await self.item_queue.put((account_id, match_id))
                    QUEUE_DEPTH.inc()  # Track queue depth
                    logger.debug(f"Produced match_id: {match_id}")
        
        # Sentinel value (None) signals the next stage (microbatcher) that production is finished.
        await self.item_queue.put(None)
        logger.info("Producer finished.")

    async def microbatcher(self):
        """
        STAGE 2: Microbatcher
        Reads single match IDs from the item queue and groups them into batches of exactly `batch_size`.
        This allows us to process games in chunks (e.g., 5 at a time) rather than one-by-one.
        """
        logger.info("Microbatcher started.")
        current_batch = []
        
        while True:
            item = await self.item_queue.get()
            if item is None:
                # Flush remaining items
                if current_batch:
                    await self.batch_queue.put(current_batch)
                # Signal consumers that batching is done
                await self.batch_queue.put(None)
                self.item_queue.task_done()
                break
                
            current_batch.append(item)
            self.item_queue.task_done()
            
            if len(current_batch) == self.batch_size:
                await self.batch_queue.put(current_batch)
                QUEUE_DEPTH.dec(len(current_batch)) # Decrease depth when batched
                logger.debug(f"Microbatcher created a batch of {self.batch_size}.")
                current_batch = []
                
        logger.info("Microbatcher finished.")

    async def _process_batch(self, batch: List[tuple], worker_id: int):
        """
        Processes a single batch of 5 matches.
        Uses a Semaphore to ensure we don't overwhelm the event loop or the API
        with too many concurrent batches, even if the queue grows large.
        """
        async with self.semaphore:
            logger.info(f"Worker {worker_id} processing batch of {len(batch)} matches.")
            
            # Use the Summary metric to track how long this batch takes
            with BATCH_TIME.time():
                # Within this batch, we fetch all 5 matches concurrently using gather.
                tasks = []
                for account_id, match_id in batch:
                    tasks.append(self.client.get_match_details(match_id))
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Save the results to storage.
                for (account_id, match_id), result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"Worker {worker_id} failed to process match {match_id}: {result}")
                        API_ERRORS.inc()
                    else:
                        filename = f"matches/{account_id}/{match_id}.json"
                        await self.storage.save(result, filename)
                        MATCHES_PROCESSED.labels(account_id=account_id).inc()

    async def consumer_pool(self):
        """
        STAGE 3: Consumer Pool
        Pull batches from the batch_queue and spawns tasks in a TaskGroup.
        TaskGroup (Python 3.11+) ensures that if one task fails, others are cleaned up,
        and it waits for all tasks to finish before exiting.
        """
        logger.info("Consumer pool started.")
        worker_id = 0
        
        async with asyncio.TaskGroup() as tg:
            while True:
                batch = await self.batch_queue.get()
                if batch is None:
                    # Producer and Batcher are done, so we can stop.
                    self.batch_queue.task_done()
                    break
                    
                # We spawn a background task for the batch and immediately return to wait for the next.
                tg.create_task(self._process_batch(batch, worker_id))
                self.batch_queue.task_done()
                worker_id += 1
                
        logger.info("Consumer pool finished.")

    async def run(self):
        """
        ORCHESTRATOR
        Runs all three stages of the pipeline concurrently.
        """
        # asyncio.gather waits for all three long-running coroutines to finish.
        await asyncio.gather(
            self.producer(),
            self.microbatcher(),
            self.consumer_pool()
        )
