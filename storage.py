import os
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import aiofiles

logger = logging.getLogger(__name__)

class StorageProvider(ABC):
    """Abstract base class for storage providers."""
    
    @abstractmethod
    async def save(self, data: dict, filename: str) -> None:
        """Save data asynchronously."""
        pass

class LocalStorageProvider(StorageProvider):
    """Stores data as JSON files in a local directory."""
    
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        # Ensure the directory exists
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    async def save(self, data: dict, filename: str) -> None:
        file_path = self.base_dir / filename
        # Ensure subdirectories exist if filename contains slashes
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiofiles.open(file_path, mode='w') as f:
            await f.write(json.dumps(data, indent=2))
        logger.debug(f"Saved local file: {file_path}")

class GCPStorageProvider(StorageProvider):
    """
    Stub for Google Cloud Storage implementation.
    This demonstrates how the interface can be swapped later.
    """
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        # e.g., self.client = storage.Client()
        
    async def save(self, data: dict, filename: str) -> None:
        # GCP Cloud Storage python client doesn't fully support async yet without
        # workarounds like run_in_executor, or using `gcloud-aio-storage`.
        # For demonstration purposes, we'll just raise an exception.
        raise NotImplementedError("GCP Storage not implemented yet. Use gcloud-aio-storage later.")
