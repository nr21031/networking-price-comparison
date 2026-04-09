"""
Base fetcher class and shared data model.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class PricePoint:
    """Normalised, provider-agnostic price record."""
    provider: str            # 'gcp' | 'aws' | 'azure'
    service: str             # 'dedicated_interconnect' | 'direct_connect' | 'expressroute' | …
    sku_id: str              # Provider-specific unique SKU identifier
    sku_name: str            # Human-readable SKU name
    description: str         # Full description from provider
    port_speed_gbps: float   # Numeric port speed (0 if not applicable, e.g. pure data transfer)
    price_monthly_usd: float # Normalised to $/month  (hourly × 730 if needed)
    price_per_gb_usd: float  # $/GB data-transfer out (0 if port-fee SKU)
    unit_original: str       # Provider's original unit string
    price_original_usd: float# Provider's original price value
    region_canonical: str    # Canonical region key  e.g. 'us_east'
    region_label: str        # Human label           e.g. 'US East'
    region_raw: str          # Provider's own region string
    plan_type: str           # 'metered' | 'unlimited' | 'dedicated' | 'hosted' | 'standard'
    currency: str            # Should be 'USD'
    effective_date: str      # ISO date from provider (or fetch date if unknown)
    fetched_at: str          # ISO timestamp of this fetch run
    source_url: str          # URL the data was retrieved from
    raw_data: str = field(default="", repr=False)  # JSON-serialised raw record

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


class BaseFetcher:
    """Shared HTTP helpers."""

    PROVIDER: str = ""
    SERVICE: str = ""
    SOURCE_URL: str = ""

    def __init__(self, config: dict):
        self.config = config
        self._session = None

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _get(self, url: str, params: Optional[dict] = None,
             headers: Optional[dict] = None, retries: int = 3) -> dict:
        import requests
        for attempt in range(retries):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=30)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.RequestException as exc:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)
        return {}

    # ── Timestamp ────────────────────────────────────────────────────────────

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Subclass contract ────────────────────────────────────────────────────

    def fetch(self) -> List[PricePoint]:
        """Return a list of normalised PricePoint records."""
        raise NotImplementedError
