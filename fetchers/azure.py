"""
Azure ExpressRoute price fetcher.

Source: Azure Retail Prices API (public, no authentication required).
Docs: https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices

Normalises:
  - Monthly circuit fees   → price_monthly_usd
  - Hourly gateway fees    → price_monthly_usd  (× 730)
  - Data transfer          → price_per_gb_usd
"""
from __future__ import annotations

import json
import re
from typing import List, Optional

from .base import BaseFetcher, PricePoint

BASE_URL = "https://prices.azure.com/api/retail/prices"

# Azure ExpressRoute uses geographic zones for circuit pricing, not named regions.
# We map Azure zone labels and ARM region names → canonical regions.
AZURE_ZONE_TO_CANONICAL = {
    "Zone 1": "us_east",        # US + W-Europe baseline
    "Zone 2": "europe_west",
    "Zone 3": "asia_pacific",
    "Zone 4": "australia",
    # Gov zones → closest
    "US Gov Zone 1": "us_east",
    "US Gov Zone 2": "us_west",
}

AZURE_REGION_TO_CANONICAL = {
    "eastus": "us_east", "eastus2": "us_east", "westus": "us_west",
    "westus2": "us_west", "westus3": "us_west", "centralus": "us_central",
    "northcentralus": "us_central", "southcentralus": "us_central",
    "westeurope": "europe_west", "northeurope": "europe_west",
    "uksouth": "europe_west", "ukwest": "europe_west",
    "francecentral": "europe_west", "germanywestcentral": "europe_west",
    "eastasia": "asia_pacific", "southeastasia": "asia_pacific",
    "japaneast": "asia_pacific", "japanwest": "asia_pacific",
    "koreacentral": "asia_pacific", "koreasouth": "asia_pacific",
    "australiaeast": "australia", "australiasoutheast": "australia",
    "australiacentral": "australia",
    "brazilsouth": "south_america", "brazilsoutheast": "south_america",
}

REGION_LABELS = {
    "us_east": "US East", "us_west": "US West", "us_central": "US Central",
    "europe_west": "Europe West", "asia_pacific": "Asia Pacific",
    "australia": "Australia", "south_america": "South America",
}

HOURS_PER_MONTH = 730

# Regex to extract Gbps speed from sku names like "10 Gbps Metered Data"
_SPEED_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(Gbps|Mbps|Tbps)", re.IGNORECASE
)


def _parse_speed_gbps(text: str) -> float:
    """Return speed in Gbps from a string like '10 Gbps' or '500 Mbps'."""
    m = _SPEED_RE.search(text)
    if not m:
        return 0.0
    val, unit = float(m.group(1)), m.group(2).lower()
    if unit == "mbps":
        return val / 1000.0
    if unit == "tbps":
        return val * 1000.0
    return val  # gbps


class AzureFetcher(BaseFetcher):
    PROVIDER = "azure"
    SERVICE = "expressroute"
    SOURCE_URL = BASE_URL

    def __init__(self, config: dict):
        super().__init__(config)
        self._azure_cfg = config.get("azure", {})
        self._service_names = self._azure_cfg.get("services", ["ExpressRoute"])
        self._price_types = self._azure_cfg.get("price_types", ["Consumption"])

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch(self) -> List[PricePoint]:
        points: List[PricePoint] = []
        for service in self._service_names:
            for price_type in self._price_types:
                points.extend(self._fetch_service(service, price_type))
        return points

    # ── Internal ──────────────────────────────────────────────────────────────

    def _fetch_service(self, service_name: str, price_type: str) -> List[PricePoint]:
        filter_str = (
            f"serviceName eq '{service_name}' and priceType eq '{price_type}'"
        )
        page_size = 100
        points = []
        skip = 0
        fetched_at = self._now()

        while True:
            params = {
                "$filter": filter_str,
                "$top": page_size,
                "$skip": skip,
            }
            try:
                data = self._get(BASE_URL, params=params)
            except Exception as exc:
                print(f"  [Azure] Fetch error for {service_name} (skip={skip}): {exc}")
                break

            items = data.get("Items", [])
            if not items:
                break

            for item in items:
                p = self._parse_item(item, fetched_at)
                if p:
                    points.append(p)

            if len(items) < page_size:
                break  # Last page

            skip += page_size

        return points

    def _parse_item(self, item: dict, fetched_at: str) -> Optional[PricePoint]:
        sku_name = item.get("skuName", "")
        product_name = item.get("productName", "")
        retail_price = item.get("retailPrice", 0.0)
        unit = item.get("unitOfMeasure", "")
        arm_region = item.get("armRegionName", "")
        location = item.get("location", "")
        meter_name = item.get("meterName", "")
        meter_id = item.get("meterId", "")
        eff_date = item.get("effectiveStartDate", fetched_at)

        if retail_price == 0:
            return None  # Skip $0 placeholders

        # ── Determine price type ────────────────────────────────────────────
        unit_lc = unit.lower()
        is_monthly = "month" in unit_lc
        is_hourly = "hour" in unit_lc
        # "data" in sku_name is unreliable — "10 Gbps Metered Data" is a monthly circuit fee,
        # not a per-GB charge. Only use the unit of measure to detect per-GB billing.
        is_data = "gb" in unit_lc

        price_monthly = 0.0
        price_per_gb = 0.0

        if is_data:
            price_per_gb = float(retail_price)
        elif is_hourly:
            price_monthly = float(retail_price) * HOURS_PER_MONTH
        elif is_monthly:
            price_monthly = float(retail_price)
        else:
            return None  # Unknown unit, skip

        # ── Region mapping ──────────────────────────────────────────────────
        # For circuit SKUs, Azure uses "Zone 1/2/3/4" as location.
        # For gateway SKUs, it uses ARM region names.
        region_canonical = (
            AZURE_ZONE_TO_CANONICAL.get(location)
            or AZURE_REGION_TO_CANONICAL.get(arm_region, "us_east")
        )
        region_label = REGION_LABELS.get(region_canonical, location or arm_region)

        # ── Port speed ──────────────────────────────────────────────────────
        port_speed_gbps = _parse_speed_gbps(sku_name) or _parse_speed_gbps(product_name)

        # ── Plan type ───────────────────────────────────────────────────────
        sku_lc = sku_name.lower()
        if "unlimited" in sku_lc:
            plan_type = "unlimited"
        elif "metered" in sku_lc:
            plan_type = "metered"
        elif "gateway" in sku_lc or "gateway" in product_name.lower():
            plan_type = "gateway"
        elif "global reach" in product_name.lower():
            plan_type = "global_reach"
        elif "direct" in product_name.lower() and "expressroute" in product_name.lower():
            plan_type = "expressroute_direct"
        else:
            plan_type = "standard"

        # ── Service classification ──────────────────────────────────────────
        if "Direct" in product_name and "ExpressRoute" in product_name:
            service = "expressroute_direct"
        elif "Gateway" in product_name:
            service = "expressroute_gateway"
        elif "Global Reach" in product_name:
            service = "expressroute_global_reach"
        else:
            service = "expressroute"

        human_name = f"{product_name} - {sku_name} ({location or arm_region})"

        raw = {
            "meterId": meter_id, "meterName": meter_name,
            "skuName": sku_name, "productName": product_name,
            "retailPrice": retail_price, "unitOfMeasure": unit,
            "armRegionName": arm_region, "location": location,
            "effectiveStartDate": eff_date,
        }

        return PricePoint(
            provider="azure",
            service=service,
            sku_id=meter_id,
            sku_name=human_name,
            description=f"Azure {product_name} {sku_name}",
            port_speed_gbps=port_speed_gbps,
            price_monthly_usd=price_monthly,
            price_per_gb_usd=price_per_gb,
            unit_original=unit,
            price_original_usd=float(retail_price),
            region_canonical=region_canonical,
            region_label=region_label,
            region_raw=location or arm_region,
            plan_type=plan_type,
            currency="USD",
            effective_date=eff_date,
            fetched_at=fetched_at,
            source_url=BASE_URL,
            raw_data=json.dumps(raw),
        )
