import os
import json
import fsspec
from typing import Generator, List

class DataProvider:
    """
    Abstracts data access from different storage backends (Local, GCS, etc.)
    """
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.fs, _ = fsspec.core.url_to_fs(base_path)

    def list_files(self) -> List[str]:
        """
        Recursively list all .json files in the base_path.
        """
        files = []
        # Use fs.walk to support both local and remote filesystems
        for root, dirs, filenames in self.fs.walk(self.base_path):
            for filename in filenames:
                if filename.endswith(".json"):
                    # Use a simple join that works for both local and cloud paths
                    separator = "/" if "/" in root or self.base_path.startswith("gs://") else os.sep
                    path = f"{root.rstrip(separator)}{separator}{filename}"
                    files.append(path)
        return files

    def get_match_data(self, file_path: str) -> dict:
        """
        Reads and returns the JSON content of a match file.
        """
        with self.fs.open(file_path, 'r') as f:
            return json.load(f)

    def stream_matches(self) -> Generator[dict, None, None]:
        """
        Generator that yields match data one by one for memory efficiency.
        """
        for file_path in self.list_files():
            try:
                yield self.get_match_data(file_path)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
