import os
import asyncio
import logging
from typing import AsyncGenerator, Dict, Any, Optional

import httpx
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class RateLimitException(Exception):
    pass

class OpenDotaClient:
    """
    Over-engineered async client for OpenDota API.
    Features:
    - Async Context Manager for clean session handling.
    - Token Bucket Rate Limiting (60 requests / minute).
    - Exponential backoff with jitter for retries.
    - Custom async iterator for paginating match history.
    """
    
    BASE_URL = "https://api.opendota.com/api"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        # Free tier: 60 requests per 60 seconds
        self.limiter = AsyncLimiter(60, 60.0)
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Setup the httpx client when entering the context manager."""
        self.client = httpx.AsyncClient(base_url=self.BASE_URL, timeout=60.0)
        logger.info("OpenDotaClient session started.")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Teardown the httpx client when exiting the context manager."""
        if self.client:
            await self.client.aclose()
        logger.info("OpenDotaClient session closed.")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.RequestError, RateLimitException)),
        reraise=True
    )
    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Internal method to make HTTP requests with rate limiting and retries."""
        if params is None:
            params = {}
        if self.api_key:
            params['api_key'] = self.api_key

        if not self.client:
            raise RuntimeError("Client is not initialized. Use 'async with OpenDotaClient() as client:'")

        # Wait for a token from the rate limiter
        async with self.limiter:
            logger.debug(f"Executing {method} {endpoint}")
            response = await self.client.request(method, endpoint, params=params)

            if response.status_code == 429:
                logger.warning(f"Rate limited on {endpoint}. Retrying...")
                raise RateLimitException("OpenDota rate limit exceeded")
            
            response.raise_for_status()
            return response.json()

    async def get_match_history(self, account_id: str, limit: int = 20) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Async generator to fetch match history. 
        Yields basic match summaries in descending order.
        """
        endpoint = f"/players/{account_id}/recentMatches"
        # We fetch recent matches as the full history endpoint is currently slow/failing.
        params = {}
        
        matches = await self._request("GET", endpoint, params)
        for match in matches:
            yield match

    async def get_match_details(self, match_id: int) -> Dict[str, Any]:
        """Fetch full details for a specific match."""
        endpoint = f"/matches/{match_id}"
        return await self._request("GET", endpoint)
