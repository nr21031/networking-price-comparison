"""
Azure ExpressRoute price fetcher.

Source: Azure Retail Prices API (public, no authentication required).
Docs: https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices

Zone → region mapping source:
https://learn.microsoft.com/en-us/azure/expressroute/expressroute-locations-providers

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

# ── Zone → canonical region(s) ────────────────────────────────────────────────
# Azure ExpressRoute billing zones span multiple geographic regions.
# A single zone price applies to ALL regions in that zone.
# Source: https://learn.microsoft.com/en-us/azure/expressroute/expressroute-locations-providers
#
# Zone 1: North America (US, Canada), most of Europe, Australia Government (Canberra)
# Zone 2: Asia Pacific (Japan, Korea, HK, SE Asia), India, Australia (excl. Gov), NZ
# Zone 3: South America, Middle East (UAE, Qatar, Israel), Africa (SA, Nigeria)
# Zone 4: Mexico (Queretaro) — not in our canonical region list, skipped
AZURE_ZONE_TO_CANONICALS: dict[str, list[str]] = {
    "Zone 1": ["us_east", "us_west", "us_central", "europe_west"],
    "Zone 2": ["asia_pacific", "australia"],
    "Zone 3": ["south_america"],
    "Zone 4": [],          # Mexico only — not in our canonical region list
    "US Gov Zone 1": ["us_east"],
    "US Gov Zone 2": ["us_west"],
}

# For single-region lookups (e.g. the "reference" region in service_type_comparison)
# — pick the most representative canonical region for each zone.
AZURE_ZONE_PRIMARY: dict[str, str] = {
    "Zone 1": "us_east",
    "Zone 2": "asia_pacific",
    "Zone 3": "south_america",
    "Zone 4": "",
    "US Gov Zone 1": "us_east",
    "US Gov Zone 2": "us_west",
}

# ARM region → canonical region (used for gateway / global-reach SKUs)
AZURE_REGION_TO_CANONICAL = {
    "eastus": "us_east",      "eastus2": "us_east",
    "westus": "us_west",      "westus2": "us_west",    "westus3": "us_west",
    "centralus": "us_central","northcentralus": "us_central",
    "southcentralus": "us_central",
    "westeurope": "europe_west", "northeurope": "europe_west",
    "uksouth": "europe_west", "ukwest": "europe_west",
    "francecentral": "europe_west", "germanywestcentral": "europe_west",
    "swedencentral": "europe_west", "switzerlandnorth": "europe_west",
    "polandcentral": "europe_west", "italynorth": "europe_west",
    "eastasia": "asia_pacific",    "southeastasia": "asia_pacific",
    "japaneast": "asia_pacific",   "japanwest": "asia_pacific",
    "koreacentral": "asia_pacific","koreasouth": "asia_pacific",
    "southindia": "asia_pacific",  "westindia": "asia_pacific",
    "centralindia": "asia_pacific","indonesiacentral": "asia_pacific",
    "australiaeast": "australia",  "australiasoutheast": "australia",
    "australiacentral": "australia","australiacentral2": "australia",
    "newzealandnorth": "australia",
    "brazilsouth": "south_america","brazilsoutheast": "south_america",
    "southafricanorth": "south_america","southafricawest": "south_america",
    "uaenorth": "south_america",   "uaecentral": "south_america",
    "qatarcentral": "south_america",
}

REGION_LABELS = {
    "us_east": "US East", "us_west": "US West", "us_central": "US Central",
    "europe_west": "Europe West", "asia_pacific": "Asia Pacific",
    "australia": "Australia", "south_america": "South America",
}

HOURS_PER_MONTH = 730

_SPEED_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(Gbps|Mbps|Tbps)", re.IGNORECASE)


def _parse_speed_gbps(text: str) -> float:
    m = _SPEED_RE.search(text)
    if not m:
        return 0.0
    val, unit = float(m.group(1)), m.group(2).lower()
    if unit == "mbps":
        return val / 1000.0
    if unit == "tbps":
        return val * 1000.0
    return val


def _is_zone_location(location: str) -> bool:
    """True for Zone-based circuit pricing locations (e.g. 'Zone 1', 'US Gov Zone 2')."""
    return bool(re.match(r"(US Gov )?Zone \d", location))


class AzureFetcher(BaseFetcher):
    PROVIDER = "azure"
    SERVICE = "expressroute"
    SOURCE_URL = BASE_URL

    def __init__(self, config: dict):
        super().__init__(config)
        self._azure_cfg = config.get("azure", {})
        self._service_names = self._azure_cfg.get("services", ["ExpressRoute"])
        self._price_types   = self._azure_cfg.get("price_types", ["Consumption"])

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
        points: List[PricePoint] = []
        skip = 0
        fetched_at = self._now()

        while True:
            params = {"$filter": filter_str, "$top": page_size, "$skip": skip}
            try:
                data = self._get(BASE_URL, params=params)
            except Exception as exc:
                print(f"  [Azure] Fetch error for {service_name} (skip={skip}): {exc}")
                break

            items = data.get("Items", [])
            if not items:
                break

            for item in items:
                # Each Zone-based item may expand to multiple PricePoints
                points.extend(self._parse_item(item, fetched_at))

            if len(items) < page_size:
                break
            skip += page_size

        return points

    def _parse_item(self, item: dict, fetched_at: str) -> List[PricePoint]:
        """
        Parse one Azure Retail Prices API item.

        Zone-based circuit SKUs are replicated once per canonical region
        that belongs to that zone so regional comparisons are accurate.
        ARM-region SKUs (gateways, global reach) produce a single record.
        """
        sku_name     = item.get("skuName", "")
        product_name = item.get("productName", "")
        retail_price = item.get("retailPrice", 0.0)
        unit         = item.get("unitOfMeasure", "")
        arm_region   = item.get("armRegionName", "")
        location     = item.get("location", "")
        meter_name   = item.get("meterName", "")
        meter_id     = item.get("meterId", "")
        eff_date     = item.get("effectiveStartDate", fetched_at)

        if retail_price == 0:
            return []

        # ── Price type ───────────────────────────────────────────────────────
        unit_lc   = unit.lower()
        is_data   = "gb" in unit_lc          # per-GB egress only
        is_hourly = "hour" in unit_lc
        is_monthly = "month" in unit_lc

        price_monthly = 0.0
        price_per_gb  = 0.0

        if is_data:
            price_per_gb = float(retail_price)
        elif is_hourly:
            price_monthly = float(retail_price) * HOURS_PER_MONTH
        elif is_monthly:
            price_monthly = float(retail_price)
        else:
            return []   # Unknown unit

        # ── Port speed ───────────────────────────────────────────────────────
        port_speed_gbps = _parse_speed_gbps(sku_name) or _parse_speed_gbps(product_name)

        # ── Plan / service type ──────────────────────────────────────────────
        sku_lc = sku_name.lower()
        prod_lc = product_name.lower()

        if "unlimited" in sku_lc:
            plan_type = "unlimited"
        elif "metered" in sku_lc:
            plan_type = "metered"
        elif "gateway" in sku_lc or "gateway" in prod_lc:
            plan_type = "gateway"
        elif "global reach" in prod_lc:
            plan_type = "global_reach"
        elif "direct" in prod_lc and "expressroute" in prod_lc:
            plan_type = "expressroute_direct"
        else:
            plan_type = "standard"

        if "Direct" in product_name and "ExpressRoute" in product_name:
            service = "expressroute_direct"
        elif "Gateway" in product_name:
            service = "expressroute_gateway"
        elif "Global Reach" in product_name:
            service = "expressroute_global_reach"
        else:
            service = "expressroute"

        raw = {
            "meterId": meter_id, "meterName": meter_name,
            "skuName": sku_name, "productName": product_name,
            "retailPrice": retail_price, "unitOfMeasure": unit,
            "armRegionName": arm_region, "location": location,
            "effectiveStartDate": eff_date,
        }

        def _make_point(region_canonical: str, region_label: str,
                        region_raw_override: str | None = None) -> PricePoint:
            human_name = f"{product_name} - {sku_name} ({location or arm_region})"
            # region_raw must be unique per DB row (UNIQUE key includes it).
            # For zone-based replicated entries we use the canonical label so that
            # each replicated copy gets a distinct key and survives INSERT OR IGNORE.
            region_raw_val = region_raw_override if region_raw_override else (location or arm_region)
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
                region_raw=region_raw_val,
                plan_type=plan_type,
                currency="USD",
                effective_date=eff_date,
                fetched_at=fetched_at,
                source_url=BASE_URL,
                raw_data=json.dumps(raw),
            )

        # ── Zone-based circuit/direct SKUs → replicate to all covered regions ─
        # Use "{Zone} ({Region Label})" as region_raw so each copy has a unique
        # key in the DB (the UNIQUE constraint includes region_raw).
        if _is_zone_location(location):
            canonicals = AZURE_ZONE_TO_CANONICALS.get(location, [])
            return [
                _make_point(
                    c,
                    REGION_LABELS.get(c, c),
                    region_raw_override=f"{location} ({REGION_LABELS.get(c, c)})",
                )
                for c in canonicals
            ]

        # ── ARM-region SKUs (gateways, global reach, etc.) ───────────────────
        region_canonical = AZURE_REGION_TO_CANONICAL.get(arm_region, "us_east")
        region_label     = REGION_LABELS.get(region_canonical, location or arm_region)
        return [_make_point(region_canonical, region_label)]
