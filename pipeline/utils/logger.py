import logging
import os
import sys
import queue
from logging.handlers import QueueHandler, QueueListener
import logging_loki

def setup_logging(app_name: str = "opendota-pipeline"):
    """
    Configures logging with a StreamHandler and an optional non-blocking LokiHandler.
    """
    # 1. Base configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # 2. Loki configuration
    loki_url = os.getenv("GRAFANA_LOKI_URL")
    loki_user = os.getenv("GRAFANA_LOKI_USER")
    loki_key = os.getenv("GRAFANA_LOKI_API_KEY")

    listener = None
    if loki_url and loki_user and loki_key:
        try:
            loki_inner_handler = logging_loki.LokiHandler(
                url=loki_url,
                tags={"application": app_name},
                auth=(loki_user, loki_key),
                version="1",
            )
            
            log_queue = queue.Queue(-1)
            queue_handler = QueueHandler(log_queue)
            
            listener = QueueListener(log_queue, loki_inner_handler)
            listener.start()
            
            logging.getLogger().addHandler(queue_handler)
            logging.getLogger(__name__).info("Non-blocking Grafana Loki logging initialized.")
        except Exception as e:
            print(f"Failed to initialize Loki logging: {e}")
    
    return listener
