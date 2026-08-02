"""serving/auth.py – Phase 9 API key authentication.

Provides a simple API-key-based authentication dependency for FastAPI.
The valid API keys are configured via the ``STOCK_API_KEYS`` environment
variable (comma-separated).  If the variable is not set, a single default
dev key is used so the service works out-of-the-box locally.
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import Header, HTTPException, status

# Default dev key — only used when STOCK_API_KEYS is not set.
DEFAULT_API_KEY = "dev-key-12345"


def get_valid_api_keys() -> set[str]:
    """Return the set of valid API keys from the environment."""
    raw = os.environ.get("STOCK_API_KEYS", "")
    if raw:
        return {k.strip() for k in raw.split(",") if k.strip()}
    return {DEFAULT_API_KEY}


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """FastAPI dependency that validates the ``X-API-Key`` header.

    Raises a 401 if the key is missing or invalid.

    Returns
    -------
    str
        The validated API key.
    """
    valid_keys = get_valid_api_keys()
    if x_api_key is None or x_api_key not in valid_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key. Provide it via the 'X-API-Key' header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return x_api_key