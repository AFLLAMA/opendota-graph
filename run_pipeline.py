import os
import argparse
import logging
from pipeline.ingestion.provider import DataProvider
from pipeline.transformation.aggregator import MetricsAggregator
from pipeline.db.client import PostgresClient
from dotenv import load_dotenv
from pipeline.utils.logger import setup_logging

load_dotenv()

logger = logging.getLogger(__name__)

def run_pipeline(source_path: str, batch_size: int = 50):
    listener = setup_logging("offlane-pipeline")
    logger.info(f"Starting pipeline. Source: {source_path}")
    
    provider = DataProvider(source_path)
    db_client = PostgresClient()
    aggregator = MetricsAggregator()

    buffer = []
    processed_count = 0
    match_count = 0

    try:
        for match_data in provider.stream_matches():
            match_count += 1
            stats = aggregator.extract_offlane_stats(match_data)
            buffer.extend(stats)

            if len(buffer) >= batch_size:
                db_client.insert_stats(buffer)
                processed_count += len(buffer)
                logger.info(f"Processed {match_count} matches, inserted {processed_count} offlane records...")
                buffer = []

        # Final flush
        if buffer:
            db_client.insert_stats(buffer)
            processed_count += len(buffer)
            logger.info(f"Pipeline finished. Total matches: {match_count}, Total offlane records: {processed_count}")
    finally:
        db_client.close()
        if listener:
            listener.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dota 2 Offlane Performance Pipeline")
    parser.add_argument("--source", type=str, default="data/matches", help="Path to match files (Local or gs://...)")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records per DB insert")

    args = parser.parse_args()
    
    # Ensure source path is absolute if it's local
    source = args.source
    if not source.startswith("gs://") and not os.path.isabs(source):
        source = os.path.abspath(source)

    run_pipeline(source, args.batch_size)
