"""
AWS Direct Connect price fetcher.

Source: AWS Price List API (public, no authentication required).
URL pattern: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDirectConnect/current/{region}/index.json

Normalises:
  - Port-hour fees  → price_monthly_usd  (× 730 h/month)
  - Data transfer   → price_per_gb_usd
"""
from __future__ import annotations

import json
import re
from typing import List

from .base import BaseFetcher, PricePoint

# Maps AWS location strings → canonical region keys defined in settings.yaml
AWS_LOCATION_TO_CANONICAL = {
    "US East (N. Virginia)":           "us_east",
    "US East (Ohio)":                  "us_central",
    "US West (N. California)":         "us_west",
    "US West (Oregon)":                "us_west",
    "Canada (Central)":                "us_east",   # closest zone
    "Europe (Ireland)":                "europe_west",
    "Europe (London)":                 "europe_west",
    "Europe (Frankfurt)":              "europe_west",
    "Europe (Paris)":                  "europe_west",
    "Europe (Stockholm)":              "europe_west",
    "Europe (Milan)":                  "europe_west",
    "Europe (Spain)":                  "europe_west",
    "Asia Pacific (Tokyo)":            "asia_pacific",
    "Asia Pacific (Seoul)":            "asia_pacific",
    "Asia Pacific (Singapore)":        "asia_pacific",
    "Asia Pacific (Hong Kong)":        "asia_pacific",
    "Asia Pacific (Mumbai)":           "asia_pacific",
    "Asia Pacific (Osaka)":            "asia_pacific",
    "Asia Pacific (Hyderabad)":        "asia_pacific",
    "Asia Pacific (Jakarta)":          "asia_pacific",
    "Asia Pacific (Sydney)":           "australia",
    "Asia Pacific (Melbourne)":        "australia",
    "South America (Sao Paulo)":       "south_america",
    "Middle East (UAE)":               "asia_pacific",
    "Middle East (Bahrain)":           "asia_pacific",
    "Africa (Cape Town)":              "europe_west",
}

REGION_LABELS = {
    "us_east": "US East", "us_west": "US West", "us_central": "US Central",
    "europe_west": "Europe West", "asia_pacific": "Asia Pacific",
    "australia": "Australia", "south_america": "South America",
}

# AWS region codes used in URL path
AWS_API_REGIONS = [
    "us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-2", "sa-east-1",
]

# Speed strings as they appear in AWS pricing attributes
SPEED_MAP = {
    "1G": 1.0, "10G": 10.0, "100G": 100.0, "400G": 400.0,
    # Hosted connection speeds
    "50M": 0.05, "100M": 0.1, "200M": 0.2, "300M": 0.3, "400M": 0.4,
    "500M": 0.5, "1000M": 1.0, "2G": 2.0, "5G": 5.0,
    "HC-50M": 0.05, "HC-100M": 0.1, "HC-200M": 0.2, "HC-300M": 0.3,
    "HC-400M": 0.4, "HC-500M": 0.5, "HC-1G": 1.0, "HC-2G": 2.0,
    "HC-5G": 5.0, "HC-10G": 10.0,
}

HOURS_PER_MONTH = 730


class AWSFetcher(BaseFetcher):
    PROVIDER = "aws"
    SERVICE = "direct_connect"
    SOURCE_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDirectConnect/"

    def __init__(self, config: dict):
        super().__init__(config)
        self._aws_cfg = config.get("aws", {})
        self._base = self._aws_cfg.get(
            "price_list_base",
            "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDirectConnect/current",
        )
        self._regions = self._aws_cfg.get("aws_regions", AWS_API_REGIONS)
        if not self._regions:
            self._regions = AWS_API_REGIONS

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch(self) -> List[PricePoint]:
        points: List[PricePoint] = []
        seen_skus: set = set()

        for region_code in self._regions:
            url = f"{self._base}/{region_code}/index.json"
            try:
                data = self._get(url)
            except Exception as exc:
                print(f"  [AWS] Could not fetch region {region_code}: {exc}")
                continue

            region_points = self._parse_region(data, region_code, url)
            for p in region_points:
                key = (p.sku_id, p.region_raw)
                if key not in seen_skus:
                    seen_skus.add(key)
                    points.append(p)

        return points

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_region(self, data: dict, region_code: str, url: str) -> List[PricePoint]:
        products = data.get("products", {})
        on_demand = data.get("terms", {}).get("OnDemand", {})
        fetched_at = self._now()
        pub_date = data.get("publicationDate", fetched_at)
        points = []

        for sku, product in products.items():
            attrs = product.get("attributes", {})
            port_speed_str = attrs.get("portSpeed", "")
            location = attrs.get("location", "")
            usage_type = attrs.get("usagetype", "")
            connection_type = attrs.get("connectionType", "")  # 'Dedicated' | 'Hosted'

            port_speed_gbps = SPEED_MAP.get(port_speed_str, 0.0)

            # Get pricing term for this SKU
            price_usd, unit_str = self._extract_price(sku, on_demand)
            if price_usd is None:
                continue

            # Determine if this is port usage or data transfer
            is_data_transfer = self._is_data_transfer(usage_type, unit_str)

            region_canonical = AWS_LOCATION_TO_CANONICAL.get(location, "us_east")
            region_label = REGION_LABELS.get(region_canonical, location)

            plan_type = "dedicated" if connection_type == "Dedicated" else "hosted"

            # Build human-readable SKU name
            if is_data_transfer:
                sku_name = f"Data Transfer Out - {location}"
            else:
                sku_name = f"Direct Connect {port_speed_str} - {location}"
                if connection_type:
                    sku_name += f" ({connection_type})"

            # Build direct-connect-location label if available
            dc_location = attrs.get("directConnectLocation", "")
            description = f"AWS Direct Connect {connection_type} {port_speed_str} @ {dc_location or location}"

            price_monthly = 0.0
            price_per_gb = 0.0

            if is_data_transfer:
                price_per_gb = float(price_usd)
            else:
                # Convert hourly → monthly
                if "hr" in unit_str.lower() or "hour" in unit_str.lower():
                    price_monthly = float(price_usd) * HOURS_PER_MONTH
                else:
                    price_monthly = float(price_usd)

            # Skip $0 port entries (some placeholder SKUs)
            if price_monthly == 0 and price_per_gb == 0:
                continue

            raw = {
                "sku": sku,
                "attributes": attrs,
                "price_per_unit": price_usd,
                "unit": unit_str,
                "publication_date": pub_date,
            }

            points.append(PricePoint(
                provider="aws",
                service=self.SERVICE,
                sku_id=sku,
                sku_name=sku_name,
                description=description,
                port_speed_gbps=port_speed_gbps,
                price_monthly_usd=price_monthly,
                price_per_gb_usd=price_per_gb,
                unit_original=unit_str,
                price_original_usd=float(price_usd),
                region_canonical=region_canonical,
                region_label=region_label,
                region_raw=location,
                plan_type=plan_type,
                currency="USD",
                effective_date=pub_date,
                fetched_at=fetched_at,
                source_url=url,
                raw_data=json.dumps(raw),
            ))

        return points

    def _extract_price(self, sku: str, on_demand: dict):
        """Return (price_str, unit_str) from the on-demand terms for a SKU."""
        terms = on_demand.get(sku, {})
        if not terms:
            return None, None
        # Grab the first offer term
        for offer_term in terms.values():
            for dim in offer_term.get("priceDimensions", {}).values():
                price_str = dim.get("pricePerUnit", {}).get("USD", "0")
                unit_str = dim.get("unit", "")
                return price_str, unit_str
        return None, None

    @staticmethod
    def _is_data_transfer(usage_type: str, unit_str: str) -> bool:
        ut = usage_type.lower()
        un = (unit_str or "").lower()
        return ("datatransfer" in ut or "data-transfer" in ut
                or "gb" in un or "out" in ut)
