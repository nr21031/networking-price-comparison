"""
GCP Cloud Interconnect price fetcher.

Primary source : Google Cloud Billing Catalog API
  https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list
  Requires a free API key:
    1. Go to https://console.cloud.google.com/apis/credentials
    2. Create API key → restrict to "Cloud Billing API"
    3. Enable the API: https://console.cloud.google.com/apis/library/cloudbilling.googleapis.com
  Set key in config/settings.yaml (gcp.api_key) or .env (GCP_API_KEY).

Fallback source (when no API key is set):
  Hardcoded reference prices derived from public GCP pricing documentation.
  Prices are accurate as of early 2025 but may be stale — always prefer the API.
"""
from __future__ import annotations

import json
import os
import re
from typing import List, Optional

from .base import BaseFetcher, PricePoint

BILLING_API_BASE = "https://cloudbilling.googleapis.com/v1"

# GCP pricing API returns prices as: units (integer dollars) + nanos (fractional)
def _nanos_to_float(price_info: dict) -> float:
    units = int(price_info.get("units", 0))
    nanos = int(price_info.get("nanos", 0))
    return units + nanos / 1_000_000_000

HOURS_PER_MONTH = 730

# GCP region → canonical region mapping
GCP_REGION_TO_CANONICAL = {
    "us-east1": "us_east", "us-east4": "us_east", "us-east5": "us_east",
    "us-west1": "us_west", "us-west2": "us_west", "us-west3": "us_west", "us-west4": "us_west",
    "us-central1": "us_central", "us-south1": "us_east",
    "europe-west1": "europe_west", "europe-west2": "europe_west",
    "europe-west3": "europe_west", "europe-west4": "europe_west",
    "europe-west6": "europe_west", "europe-west8": "europe_west",
    "europe-west9": "europe_west", "europe-north1": "europe_west",
    "europe-central2": "europe_west", "europe-southwest1": "europe_west",
    "asia-east1": "asia_pacific", "asia-east2": "asia_pacific",
    "asia-northeast1": "asia_pacific", "asia-northeast2": "asia_pacific",
    "asia-northeast3": "asia_pacific", "asia-southeast1": "asia_pacific",
    "asia-southeast2": "asia_pacific", "asia-south1": "asia_pacific",
    "asia-south2": "asia_pacific",
    "australia-southeast1": "australia", "australia-southeast2": "australia",
    "southamerica-east1": "south_america", "southamerica-west1": "south_america",
    "northamerica-northeast1": "us_east", "northamerica-northeast2": "us_east",
    "global": "us_east",  # fallback for globally-priced SKUs
}

REGION_LABELS = {
    "us_east": "US East", "us_west": "US West", "us_central": "US Central",
    "europe_west": "Europe West", "asia_pacific": "Asia Pacific",
    "australia": "Australia", "south_america": "South America",
}

# Regex to pull Gbps (or Mbps) from SKU description
_SPEED_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(Gbps|Mbps|G\b)", re.IGNORECASE)

def _parse_speed_gbps(text: str) -> float:
    m = _SPEED_RE.search(text)
    if not m:
        return 0.0
    val, unit = float(m.group(1)), m.group(2).lower()
    return val / 1000.0 if "mbps" in unit else val

# GCP Compute Engine billing service ID — contains InterconnectPort + Egress SKUs
COMPUTE_ENGINE_SVC = "services/6F81-5844-456A"

# Resource groups within Compute Engine that hold Interconnect pricing
INTERCONNECT_RESOURCE_GROUPS = {
    "InterconnectPort",          # Physical circuit/port fees (dedicated & partner)
    "InterconnectAttachment",    # VLAN attachment fees (per-speed, per-month)
    "PeeringOrInterconnectEgress",  # Data transfer egress via Interconnect
}

# Keywords that identify relevant SKUs in GCP billing catalog
INTERCONNECT_KEYWORDS = [
    "dedicated interconnect",
    "partner interconnect",
    "network interconnect egress",
    "interconnect attachment",
    "interconnect - port",
    "vlan attachment",
]

# ── Reference / fallback pricing ───────────────────────────────────────────────
# Source: https://cloud.google.com/vpc/network-pricing#interconnect-pricing
# Prices as of early 2025.  Used only when GCP_API_KEY is not configured.
#
# NOTE: These are approximations derived from GCP public docs and may not
# reflect the latest pricing. Set a GCP API key to get live data.
REFERENCE_PRICES = [
    # Dedicated Interconnect ports
    {
        "sku_id": "REF-GCP-DC-10G-US",
        "sku_name": "Dedicated Interconnect 10 Gbps Port (Reference)",
        "description": "Dedicated Interconnect - 10 Gbps port, US regions",
        "port_speed_gbps": 10.0,
        "price_monthly_usd": 1700.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    {
        "sku_id": "REF-GCP-DC-10G-EU",
        "sku_name": "Dedicated Interconnect 10 Gbps Port - Europe (Reference)",
        "description": "Dedicated Interconnect - 10 Gbps port, Europe",
        "port_speed_gbps": 10.0,
        "price_monthly_usd": 1700.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "europe_west",
        "region_label": "Europe West",
        "region_raw": "europe-west1",
    },
    {
        "sku_id": "REF-GCP-DC-10G-APAC",
        "sku_name": "Dedicated Interconnect 10 Gbps Port - Asia Pacific (Reference)",
        "description": "Dedicated Interconnect - 10 Gbps port, Asia Pacific",
        "port_speed_gbps": 10.0,
        "price_monthly_usd": 1700.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "asia_pacific",
        "region_label": "Asia Pacific",
        "region_raw": "asia-northeast1",
    },
    {
        "sku_id": "REF-GCP-DC-100G-US",
        "sku_name": "Dedicated Interconnect 100 Gbps Port (Reference)",
        "description": "Dedicated Interconnect - 100 Gbps port, US regions",
        "port_speed_gbps": 100.0,
        "price_monthly_usd": 12000.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    {
        "sku_id": "REF-GCP-DC-100G-EU",
        "sku_name": "Dedicated Interconnect 100 Gbps Port - Europe (Reference)",
        "description": "Dedicated Interconnect - 100 Gbps port, Europe",
        "port_speed_gbps": 100.0,
        "price_monthly_usd": 12000.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "europe_west",
        "region_label": "Europe West",
        "region_raw": "europe-west1",
    },
    {
        "sku_id": "REF-GCP-DC-100G-APAC",
        "sku_name": "Dedicated Interconnect 100 Gbps Port - Asia Pacific (Reference)",
        "description": "Dedicated Interconnect - 100 Gbps port, Asia Pacific",
        "port_speed_gbps": 100.0,
        "price_monthly_usd": 12000.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "dedicated",
        "region_canonical": "asia_pacific",
        "region_label": "Asia Pacific",
        "region_raw": "asia-northeast1",
    },
    # Partner Interconnect capacities
    {
        "sku_id": "REF-GCP-PI-50M-US",
        "sku_name": "Partner Interconnect 50 Mbps (Reference)",
        "description": "Partner Interconnect 50 Mbps VLAN attachment, US",
        "port_speed_gbps": 0.05,
        "price_monthly_usd": 10.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "hosted",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    {
        "sku_id": "REF-GCP-PI-1G-US",
        "sku_name": "Partner Interconnect 1 Gbps (Reference)",
        "description": "Partner Interconnect 1 Gbps VLAN attachment, US",
        "port_speed_gbps": 1.0,
        "price_monthly_usd": 200.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "hosted",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    {
        "sku_id": "REF-GCP-PI-10G-US",
        "sku_name": "Partner Interconnect 10 Gbps (Reference)",
        "description": "Partner Interconnect 10 Gbps VLAN attachment, US",
        "port_speed_gbps": 10.0,
        "price_monthly_usd": 2000.0,
        "price_per_gb_usd": 0.0,
        "unit_original": "per month",
        "plan_type": "hosted",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    # Egress via Interconnect (data transfer)
    {
        "sku_id": "REF-GCP-EGRESS-US",
        "sku_name": "Network Interconnect Egress - US (Reference)",
        "description": "Egress via Cloud Interconnect - North America",
        "port_speed_gbps": 0.0,
        "price_monthly_usd": 0.0,
        "price_per_gb_usd": 0.02,
        "unit_original": "per GB",
        "plan_type": "standard",
        "region_canonical": "us_east",
        "region_label": "US East",
        "region_raw": "us-east4",
    },
    {
        "sku_id": "REF-GCP-EGRESS-EU",
        "sku_name": "Network Interconnect Egress - Europe (Reference)",
        "description": "Egress via Cloud Interconnect - Europe",
        "port_speed_gbps": 0.0,
        "price_monthly_usd": 0.0,
        "price_per_gb_usd": 0.02,
        "unit_original": "per GB",
        "plan_type": "standard",
        "region_canonical": "europe_west",
        "region_label": "Europe West",
        "region_raw": "europe-west1",
    },
    {
        "sku_id": "REF-GCP-EGRESS-APAC",
        "sku_name": "Network Interconnect Egress - Asia Pacific (Reference)",
        "description": "Egress via Cloud Interconnect - Asia Pacific",
        "port_speed_gbps": 0.0,
        "price_monthly_usd": 0.0,
        "price_per_gb_usd": 0.08,
        "unit_original": "per GB",
        "plan_type": "standard",
        "region_canonical": "asia_pacific",
        "region_label": "Asia Pacific",
        "region_raw": "asia-northeast1",
    },
]


class GCPFetcher(BaseFetcher):
    PROVIDER = "gcp"
    SERVICE = "dedicated_interconnect"
    SOURCE_URL = BILLING_API_BASE

    def __init__(self, config: dict):
        super().__init__(config)
        self._gcp_cfg = config.get("gcp", {})
        self._api_key = (
            self._gcp_cfg.get("api_key", "")
            or os.environ.get("GCP_API_KEY", "")
        )
        self._sku_keywords = [k.lower() for k in self._gcp_cfg.get("sku_keywords", INTERCONNECT_KEYWORDS)]
        self._service_filter = self._gcp_cfg.get("service_filter", ["Cloud Interconnect"])
        self._use_fallback = self._gcp_cfg.get("use_reference_fallback", True)

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch(self) -> List[PricePoint]:
        if not self._api_key:
            if self._use_fallback:
                print("  [GCP] No API key configured — using reference pricing data.")
                print("        To get live prices, set gcp.api_key in config/settings.yaml")
                print("        or set the GCP_API_KEY environment variable.")
                return self._reference_prices()
            else:
                raise ValueError(
                    "GCP API key is required. Set gcp.api_key in config/settings.yaml "
                    "or the GCP_API_KEY environment variable.\n"
                    "Get a free key at https://console.cloud.google.com/apis/credentials"
                )

        points: List[PricePoint] = []

        # ── Source 1: Cloud Interconnect service (VLAN attachments) ──────────
        services = self._find_services()
        for svc in services:
            points.extend(self._fetch_skus_by_keyword(svc))

        # ── Source 2: Compute Engine service (circuit/port fees + egress) ────
        # GCP bills physical port fees and egress under Compute Engine, not
        # under the Cloud Interconnect service.
        points.extend(self._fetch_compute_engine_interconnect())

        if not points and self._use_fallback:
            print("  [GCP] No matching SKUs found via API; falling back to reference data.")
            return self._reference_prices()

        print(f"  [GCP] {len(points)} live SKUs fetched from Billing API.")
        return points

    # ── API calls ─────────────────────────────────────────────────────────────

    def _find_services(self) -> list:
        """Return list of service resource names matching our filter."""
        url = f"{BILLING_API_BASE}/services"
        params = {"key": self._api_key, "pageSize": 300}
        matching = []
        while True:
            try:
                data = self._get(url, params=params)
            except Exception as exc:
                print(f"  [GCP] Could not list services: {exc}")
                return []
            for svc in data.get("services", []):
                display_name = svc.get("displayName", "")
                if any(f.lower() in display_name.lower() for f in self._service_filter):
                    matching.append(svc)
            next_token = data.get("nextPageToken")
            if not next_token:
                break
            params["pageToken"] = next_token
        return matching

    def _fetch_skus_by_keyword(self, service: dict) -> List[PricePoint]:
        """Fetch SKUs from a service, filtering by description keywords."""
        name = service.get("name", "")
        svc_display = service.get("displayName", name)
        url = f"{BILLING_API_BASE}/{name}/skus"
        params = {"key": self._api_key, "pageSize": 500}
        points = []
        fetched_at = self._now()

        while True:
            try:
                data = self._get(url, params=params)
            except Exception as exc:
                print(f"  [GCP] Could not fetch SKUs for {svc_display}: {exc}")
                break

            for sku in data.get("skus", []):
                desc = sku.get("description", "").lower()
                if not any(kw in desc for kw in self._sku_keywords):
                    continue
                p = self._parse_sku(sku, fetched_at, url)
                if p:
                    points.append(p)

            next_token = data.get("nextPageToken")
            if not next_token:
                break
            params["pageToken"] = next_token

        return points

    def _fetch_compute_engine_interconnect(self) -> List[PricePoint]:
        """
        Fetch Interconnect-related SKUs from the Compute Engine billing service.

        GCP's physical circuit/port fees (InterconnectPort) and data egress
        (PeeringOrInterconnectEgress) are billed under Compute Engine, not the
        Cloud Interconnect service.
        """
        url = f"{BILLING_API_BASE}/{COMPUTE_ENGINE_SVC}/skus"
        params = {"key": self._api_key, "pageSize": 500}
        fetched_at = self._now()
        points = []

        while True:
            try:
                data = self._get(url, params=params)
            except Exception as exc:
                print(f"  [GCP] Could not fetch Compute Engine SKUs: {exc}")
                break

            for sku in data.get("skus", []):
                cat = sku.get("category", {})
                rg = cat.get("resourceGroup", "")
                if rg not in INTERCONNECT_RESOURCE_GROUPS:
                    continue
                desc = sku.get("description", "")
                # Skip MPS/Partner-side zero-dollar placeholder SKUs
                # (they represent charges that appear on the partner's invoice)
                if "mps" in desc.lower() and self._is_zero_priced(sku):
                    continue
                p = self._parse_sku(sku, fetched_at, url)
                if p:
                    points.append(p)

            next_token = data.get("nextPageToken")
            if not next_token:
                break
            params["pageToken"] = next_token

        return points

    @staticmethod
    def _is_zero_priced(sku: dict) -> bool:
        pi = sku.get("pricingInfo", [{}])[0]
        tiers = pi.get("pricingExpression", {}).get("tieredRates", [])
        return all(
            _nanos_to_float(t.get("unitPrice", {})) == 0 for t in tiers
        )

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_sku(self, sku: dict, fetched_at: str, source_url: str) -> Optional[PricePoint]:
        sku_id = sku.get("skuId", "")
        description = sku.get("description", "")
        service_regions = sku.get("serviceRegions", ["global"])
        pricing_info = sku.get("pricingInfo", [])
        category = sku.get("category", {})

        if not pricing_info:
            return None

        # Use the first pricing info (most recent)
        pi = pricing_info[0]
        expr = pi.get("pricingExpression", {})
        unit = expr.get("usageUnit", "")
        tiers = expr.get("tieredRates", [])
        if not tiers:
            return None

        # GCP often has a $0 first tier, pick the first non-zero rate
        price_raw = 0.0
        for tier in tiers:
            unit_price = tier.get("unitPrice", {})
            val = _nanos_to_float(unit_price)
            if val > 0:
                price_raw = val
                break
        if price_raw == 0:
            return None

        # Convert to monthly / per-GB
        unit_lc = unit.lower()
        rg = category.get("resourceGroup", "")
        # GCP uses "GiBy" (gibibytes) as the unit for data transfer SKUs.
        # "giby" does NOT contain "gb" — must check for "giby" explicitly.
        is_data = (
            rg == "PeeringOrInterconnectEgress"
            or "giby" in unit_lc
            or "gby" in unit_lc
            or (unit_lc == "gb")
        )
        is_hourly = unit_lc in ("h", "hour", "hr")

        price_monthly = 0.0
        price_per_gb = 0.0
        if is_data:
            # GCP uses GiBy (gibibytes) — convert to GB (÷ 1.0737)
            # For comparability with AWS/Azure we keep it as-is (GiBy ≈ GB for pricing display)
            price_per_gb = price_raw
        elif is_hourly:
            price_monthly = price_raw * HOURS_PER_MONTH
        else:
            # Assume monthly (unit 'mo' or '1')
            price_monthly = price_raw

        # Speed detection from description
        port_speed_gbps = _parse_speed_gbps(description)

        # Region — use the first serviceRegion
        region_raw = service_regions[0] if service_regions else "global"
        region_canonical = GCP_REGION_TO_CANONICAL.get(region_raw, "us_east")
        region_label = REGION_LABELS.get(region_canonical, region_raw)

        # Service and plan type — use resource group if available for accuracy
        # (rg already set above for is_data check)
        desc_lc = description.lower()

        if rg == "InterconnectPort":
            # Physical circuit fee
            if "partner" in desc_lc or "mps" in desc_lc:
                plan_type = "hosted"
                service = "partner_interconnect"
            else:
                plan_type = "dedicated"
                service = "dedicated_interconnect"
        elif rg == "InterconnectAttachment":
            # VLAN attachment fee
            if "partner" in desc_lc or "mps" in desc_lc:
                plan_type = "hosted"
                service = "partner_interconnect"
            else:
                plan_type = "dedicated"
                service = "dedicated_interconnect"
        elif rg == "PeeringOrInterconnectEgress":
            plan_type = "standard"
            service = "interconnect_egress"
        elif "partner" in desc_lc:
            plan_type = "hosted"
            service = "partner_interconnect"
        elif "dedicated" in desc_lc:
            plan_type = "dedicated"
            service = "dedicated_interconnect"
        else:
            plan_type = "standard"
            service = "interconnect_egress"

        effective_date = pi.get("effectiveTime", fetched_at)

        raw = {
            "skuId": sku_id, "description": description,
            "serviceRegions": service_regions, "unit": unit,
            "price_raw": price_raw, "category": category,
        }

        # Add subtype label to name for clarity in reports
        rg_label = {
            "InterconnectPort": "[Circuit]",
            "InterconnectAttachment": "[Attachment]",
            "PeeringOrInterconnectEgress": "[Egress]",
        }.get(rg, "")
        sku_name = f"GCP {rg_label} {description}".replace("  ", " ").strip()

        return PricePoint(
            provider="gcp",
            service=service,
            sku_id=sku_id,
            sku_name=sku_name,
            description=description,
            port_speed_gbps=port_speed_gbps,
            price_monthly_usd=price_monthly,
            price_per_gb_usd=price_per_gb,
            unit_original=unit,
            price_original_usd=price_raw,
            region_canonical=region_canonical,
            region_label=region_label,
            region_raw=region_raw,
            plan_type=plan_type,
            currency="USD",
            effective_date=effective_date,
            fetched_at=fetched_at,
            source_url=source_url,
            raw_data=json.dumps(raw),
        )

    # ── Fallback ──────────────────────────────────────────────────────────────

    def _reference_prices(self) -> List[PricePoint]:
        fetched_at = self._now()
        points = []
        for ref in REFERENCE_PRICES:
            points.append(PricePoint(
                provider="gcp",
                service=(
                    "dedicated_interconnect" if "Dedicated" in ref["sku_name"]
                    else "partner_interconnect" if "Partner" in ref["sku_name"]
                    else "interconnect_egress"
                ),
                sku_id=ref["sku_id"],
                sku_name=ref["sku_name"],
                description=ref["description"],
                port_speed_gbps=ref["port_speed_gbps"],
                price_monthly_usd=ref["price_monthly_usd"],
                price_per_gb_usd=ref["price_per_gb_usd"],
                unit_original=ref["unit_original"],
                price_original_usd=ref.get("price_monthly_usd") or ref.get("price_per_gb_usd", 0),
                region_canonical=ref["region_canonical"],
                region_label=ref["region_label"],
                region_raw=ref["region_raw"],
                plan_type=ref["plan_type"],
                currency="USD",
                effective_date="2025-01-01",
                fetched_at=fetched_at,
                source_url="https://cloud.google.com/vpc/network-pricing (reference)",
                raw_data=json.dumps(ref),
            ))
        return points
