"""
Price comparison and normalisation engine.

Produces structured DataFrames used by both the HTML report and the
Streamlit dashboard.
"""
from __future__ import annotations

from typing import Dict, List, Optional
import pandas as pd

# Provider display config
PROVIDER_META = {
    "gcp":   {"label": "GCP",   "color": "#4285F4", "light": "#D2E3FC"},
    "aws":   {"label": "AWS",   "color": "#FF9900", "light": "#FFF0D0"},
    "azure": {"label": "Azure", "color": "#008AD7", "light": "#CCE8F4"},
}

CANONICAL_REGIONS = [
    "us_east", "us_west", "us_central", "europe_west",
    "asia_pacific", "australia", "south_america",
]
REGION_LABELS = {
    "us_east": "US East", "us_west": "US West", "us_central": "US Central",
    "europe_west": "Europe West", "asia_pacific": "Asia Pacific",
    "australia": "Australia", "south_america": "South America",
}

HOURS_PER_MONTH = 730

# Services that are "directly comparable" across providers
SERVICE_GROUPS = {
    "dedicated": {
        "label": "Dedicated / Direct Private Connectivity",
        "gcp":   ["dedicated_interconnect"],
        "aws":   ["direct_connect"],
        "azure": ["expressroute", "expressroute_direct"],
    },
    "partner": {
        "label": "Partner / Hosted Private Connectivity",
        "gcp":   ["partner_interconnect"],
        "aws":   ["direct_connect"],   # AWS hosted connections
        "azure": ["expressroute"],
    },
}

# For AWS, hosted vs dedicated is in plan_type
AWS_DEDICATED_PLAN = "dedicated"
AWS_HOSTED_PLAN = "hosted"

# For Azure, metered is the fairest comparison for port fees (includes data allowance)
AZURE_METERED = "metered"
AZURE_UNLIMITED = "unlimited"
AZURE_DIRECT = "expressroute_direct"

# ── Redundancy / value normalisation ─────────────────────────────────────────
# Azure ExpressRoute purchases include a primary AND secondary circuit in each
# port purchase for built-in redundancy.  GCP and AWS provide 1 circuit per
# purchase.  Divide Azure list price by this factor to get a per-circuit
# normalised price for like-for-like comparison.
AZURE_CIRCUITS_PER_PURCHASE = 2

# GB transferred per Gbps at 100 % utilisation for one calendar month
# = 730 h × 3600 s/h ÷ 8 bits/byte = 328 500 GB / Gbps / month
GB_PER_GBPS_MONTH_FULL = 730 * 3600 / 8  # 328 500

# ── Service-tier metadata ─────────────────────────────────────────────────────
# Defines each interconnect flavour and its cross-provider equivalents.
SERVICE_TIERS = {
    "dedicated": {
        "label": "Dedicated / Direct",
        "description": "Physical dedicated port at a carrier-neutral colocation facility. Fixed monthly fee regardless of traffic.",
        "gcp_product":  "Dedicated Interconnect",
        "aws_product":  "Direct Connect (Dedicated Connection)",
        "azure_product": "ExpressRoute Direct",
        "azure_note":   f"Includes {AZURE_CIRCUITS_PER_PURCHASE} circuits (primary + secondary) per port purchase — built-in redundancy.",
        "gcp_available": True,
        "aws_available": True,
        "azure_available": True,
    },
    "partner": {
        "label": "Partner / Hosted",
        "description": "Virtual circuit provisioned via an authorised network service provider. Capacity from 50 Mbps to 10 Gbps.",
        "gcp_product":  "Partner Interconnect",
        "aws_product":  "Direct Connect (Hosted Connection)",
        "azure_product": "ExpressRoute Circuit (Metered)",
        "azure_note":   f"Metered model: circuit fee + per-GB egress. Includes {AZURE_CIRCUITS_PER_PURCHASE} circuits per purchase.",
        "gcp_available": True,
        "aws_available": True,
        "azure_available": True,
    },
    "partner_unlimited": {
        "label": "Partner / Hosted — Unlimited Egress",
        "description": "Fixed monthly fee that includes unlimited data transfer out. No per-GB egress charges.",
        "gcp_product":  "Not available",
        "aws_product":  "Not available",
        "azure_product": "ExpressRoute Circuit (Unlimited)",
        "azure_note":   f"Higher port fee than metered but no per-GB charges. Includes {AZURE_CIRCUITS_PER_PURCHASE} circuits per purchase.",
        "gcp_available": False,
        "aws_available": False,
        "azure_available": True,
    },
    "cross_cloud": {
        "label": "Cross-Cloud Interconnect",
        "description": "Physical dedicated link directly between two cloud providers (e.g. GCP ↔ AWS).",
        "gcp_product":  "Cross-Cloud Interconnect",
        "aws_product":  "No equivalent",
        "azure_product": "No equivalent",
        "azure_note":   "Use internet peering or partner solutions for multi-cloud connectivity.",
        "gcp_available": True,
        "aws_available": False,
        "azure_available": False,
    },
    "site_to_site": {
        "label": "Site-to-Site Extension",
        "description": "Extend private connectivity to link on-premises sites to each other via the cloud backbone (add-on fee on top of existing circuits).",
        "gcp_product":  "Cross-Site Interconnect / NCC",
        "aws_product":  "Direct Connect SiteLink",
        "azure_product": "ExpressRoute Global Reach",
        "azure_note":   "Per-port/circuit connection fee. Requires two ExpressRoute circuits.",
        "gcp_available": True,
        "aws_available": True,
        "azure_available": True,
    },
}


class PriceComparator:
    """Turns raw DB records into structured comparison DataFrames."""

    def __init__(self, records: List[dict]):
        if records:
            self.df = pd.DataFrame(records)
            self._clean()
        else:
            self.df = pd.DataFrame()

    # ── Public interface ──────────────────────────────────────────────────────

    def port_fee_comparison(
        self,
        speeds_gbps: Optional[List[float]] = None,
        plan_types: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Head-to-head port / circuit fee comparison ($/month).

        Returns a DataFrame with columns:
          region_label | speed_gbps | gcp_monthly | aws_monthly | azure_monthly
          | gcp_vs_aws_pct | gcp_vs_azure_pct
        """
        if self.df.empty:
            return pd.DataFrame()

        df = self.df.copy()

        # Filter to port-fee SKUs only (price_monthly > 0 and not pure data transfer)
        df = df[df["price_monthly_usd"] > 0]

        # Filter provider-specific service types
        # For GCP: only standard "[Circuit]" SKUs are comparable to AWS/Azure port fees.
        # Exclude:
        #   [Attachment] — VLAN attachment add-on charge, not the base port fee
        #   Application Awareness — optional add-on feature, not the base circuit
        #   Cross-Cloud Interconnect — premium product for multi-cloud, not like-for-like
        df_gcp = df[
            (df["provider"] == "gcp") &
            (df["service"].isin(["dedicated_interconnect", "partner_interconnect"])) &
            # Live API SKUs are named "... [Circuit]"; reference fallback SKUs say "(Reference)".
            # Exclude attachments, add-ons, and cross-cloud products in either case.
            (
                df["sku_name"].str.contains(r"\[Circuit\]", regex=True, na=False) |
                df["sku_name"].str.contains("Reference", na=False)
            ) &
            (~df["sku_name"].str.contains("Application Awareness", case=False, na=False)) &
            (~df["sku_name"].str.contains("Cross-Cloud", case=False, na=False))
        ]
        df_aws = df[
            (df["provider"] == "aws") &
            (df["service"] == "direct_connect") &
            (df["port_speed_gbps"] > 0)
        ]
        df_azure = df[
            (df["provider"] == "azure") &
            (df["service"].isin(["expressroute", "expressroute_direct"]))
        ]

        # Apply optional filters
        if speeds_gbps:
            df_gcp = df_gcp[df_gcp["port_speed_gbps"].isin(speeds_gbps)]
            df_aws = df_aws[df_aws["port_speed_gbps"].isin(speeds_gbps)]
            df_azure = df_azure[df_azure["port_speed_gbps"].isin(speeds_gbps)]

        if regions:
            df_gcp = df_gcp[df_gcp["region_canonical"].isin(regions)]
            df_aws = df_aws[df_aws["region_canonical"].isin(regions)]
            df_azure = df_azure[df_azure["region_canonical"].isin(regions)]

        if plan_types:
            df_gcp = df_gcp[df_gcp["plan_type"].isin(plan_types)]
            df_aws = df_aws[df_aws["plan_type"].isin(plan_types)]
            df_azure = df_azure[df_azure["plan_type"].isin(plan_types)]
        else:
            # Default: dedicated only
            df_gcp = df_gcp[df_gcp["plan_type"] == "dedicated"]
            df_aws = df_aws[df_aws["plan_type"] == "dedicated"]
            df_azure = df_azure[df_azure["plan_type"].isin(["metered", "standard"])]

        # Aggregate: take the min price per (region, speed) — closest comparable
        def _agg(sub: pd.DataFrame) -> pd.DataFrame:
            return (
                sub.groupby(["region_canonical", "region_label", "port_speed_gbps"])
                ["price_monthly_usd"]
                .min()
                .reset_index()
            )

        g = _agg(df_gcp).rename(columns={"price_monthly_usd": "gcp_monthly"})
        a = _agg(df_aws).rename(columns={"price_monthly_usd": "aws_monthly"})
        z = _agg(df_azure).rename(columns={"price_monthly_usd": "azure_monthly"})

        merged = (
            g.merge(a, on=["region_canonical", "port_speed_gbps"], how="outer",
                    suffixes=("", "_aws"))
             .merge(z, on=["region_canonical", "port_speed_gbps"], how="outer",
                    suffixes=("", "_az"))
        )

        # Unify region_label
        if "region_label_aws" in merged.columns:
            merged["region_label"] = merged["region_label"].combine_first(
                merged.pop("region_label_aws")
            )
        if "region_label_az" in merged.columns:
            merged["region_label"] = merged["region_label"].combine_first(
                merged.pop("region_label_az")
            )

        # Fill canonical region labels for any NaN
        merged["region_label"] = merged["region_label"].combine_first(
            merged["region_canonical"].map(REGION_LABELS)
        )

        # Compute % differences (positive = GCP is more expensive)
        merged["gcp_vs_aws_pct"] = (
            (merged["gcp_monthly"] - merged["aws_monthly"]) / merged["aws_monthly"] * 100
        ).round(1)
        merged["gcp_vs_azure_pct"] = (
            (merged["gcp_monthly"] - merged["azure_monthly"]) / merged["azure_monthly"] * 100
        ).round(1)

        # Sort
        merged["speed_sort"] = merged["port_speed_gbps"]
        merged = merged.sort_values(
            ["region_canonical", "speed_sort"]
        ).drop(columns=["speed_sort"])

        return merged.reset_index(drop=True)

    def data_transfer_comparison(
        self, regions: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Per-GB data transfer out comparison.
        Returns DataFrame with columns: region_label | gcp_per_gb | aws_per_gb | azure_per_gb
        """
        if self.df.empty:
            return pd.DataFrame()

        df = self.df[self.df["price_per_gb_usd"] > 0].copy()

        if regions:
            df = df[df["region_canonical"].isin(regions)]

        def _agg(sub: pd.DataFrame) -> pd.DataFrame:
            return (
                sub.groupby(["region_canonical", "region_label"])
                ["price_per_gb_usd"]
                .min()
                .reset_index()
            )

        g = _agg(df[df["provider"] == "gcp"]).rename(columns={"price_per_gb_usd": "gcp_per_gb"})
        a = _agg(df[df["provider"] == "aws"]).rename(columns={"price_per_gb_usd": "aws_per_gb"})
        z = _agg(df[df["provider"] == "azure"]).rename(columns={"price_per_gb_usd": "azure_per_gb"})

        merged = (
            g.merge(a, on=["region_canonical", "region_label"], how="outer",
                    suffixes=("", "_aws"))
             .merge(z, on=["region_canonical", "region_label"], how="outer",
                    suffixes=("", "_az"))
        )

        for col in ("region_label_aws", "region_label_az"):
            if col in merged.columns:
                merged.pop(col)

        merged["region_label"] = merged["region_label"].combine_first(
            merged["region_canonical"].map(REGION_LABELS)
        )

        merged["gcp_vs_aws_pct"] = (
            (merged["gcp_per_gb"] - merged["aws_per_gb"]) / merged["aws_per_gb"] * 100
        ).round(1)
        merged["gcp_vs_azure_pct"] = (
            (merged["gcp_per_gb"] - merged["azure_per_gb"]) / merged["azure_per_gb"] * 100
        ).round(1)

        return merged.reset_index(drop=True)

    @staticmethod
    def util_to_gb_month(speed_gbps: float, util_pct: float) -> float:
        """
        Convert circuit speed + utilisation percentage to GB transferred per month.

        Formula:  monthly_GB = speed_Gbps × (util_pct / 100) × 730 h × 3600 s/h ÷ 8 bits/byte
        Example:  10 Gbps at 30 % → 10 × 0.30 × 328 500 = 985 500 GB ≈ 962 TB/month
        """
        return speed_gbps * (util_pct / 100.0) * GB_PER_GBPS_MONTH_FULL

    def tco_scenarios(
        self,
        scenarios: List[dict],
        region: str = "us_east",
    ) -> pd.DataFrame:
        """
        Total cost of ownership for each scenario.

        Each scenario dict supports two input styles:
          Style A (legacy):  {label, port_speed_gbps, monthly_data_tb}
          Style B (new):     {label, port_speed_gbps, util_pct}   — util_pct takes precedence

        Azure columns include both metered (port + egress) and unlimited (flat fee).

        Returns DataFrame with columns:
          scenario | speed_gbps | util_pct | data_gb | data_tb
          | gcp_total | aws_total | azure_metered_total | azure_unlimited_total
          | azure_metered_per_circuit | cheapest | gcp_savings_vs_aws | gcp_savings_vs_azure_metered
        """
        port_df = self.port_fee_comparison()
        dt_df = self.data_transfer_comparison()

        # Build Azure unlimited port fee lookup (flat fee, no egress charge)
        az_unlimited_df = pd.DataFrame()
        if not self.df.empty:
            az_unl = self.df[
                (self.df["provider"] == "azure") &
                (self.df["service"].isin(["expressroute", "expressroute_direct"])) &
                (self.df["plan_type"] == "unlimited") &
                (self.df["price_monthly_usd"] > 0)
            ]
            if not az_unl.empty:
                az_unlimited_df = (
                    az_unl.groupby(["region_canonical", "port_speed_gbps"])
                    ["price_monthly_usd"]
                    .min()
                    .reset_index()
                    .rename(columns={"price_monthly_usd": "azure_unlimited_port"})
                )

        rows = []
        for sc in scenarios:
            speed = sc["port_speed_gbps"]

            # Resolve data volume
            if "util_pct" in sc:
                util_pct = float(sc["util_pct"])
                data_gb = self.util_to_gb_month(speed, util_pct)
                data_tb = data_gb / 1024.0
            else:
                data_tb = float(sc.get("monthly_data_tb", 0))
                data_gb = data_tb * 1024.0
                # Back-calculate utilisation for display
                max_gb = self.util_to_gb_month(speed, 100.0)
                util_pct = (data_gb / max_gb * 100.0) if max_gb > 0 else 0.0

            # Port fees for the requested region
            if port_df.empty or "port_speed_gbps" not in port_df.columns:
                pf_row = pd.DataFrame()
            else:
                pf_row = port_df[
                    (port_df["port_speed_gbps"] == speed) &
                    (port_df["region_canonical"] == region)
                ]
                if pf_row.empty:
                    pf_row = port_df[port_df["port_speed_gbps"] == speed]

            gcp_port = float(pf_row["gcp_monthly"].min()) if not pf_row.empty and pf_row["gcp_monthly"].notna().any() else None
            aws_port = float(pf_row["aws_monthly"].min()) if not pf_row.empty and pf_row["aws_monthly"].notna().any() else None
            az_port  = float(pf_row["azure_monthly"].min()) if not pf_row.empty and pf_row["azure_monthly"].notna().any() else None

            # Azure unlimited port fee
            az_unl_port = None
            if not az_unlimited_df.empty:
                sub = az_unlimited_df[
                    (az_unlimited_df["region_canonical"] == region) &
                    (az_unlimited_df["port_speed_gbps"] == speed)
                ]
                if sub.empty:
                    sub = az_unlimited_df[az_unlimited_df["port_speed_gbps"] == speed]
                if not sub.empty:
                    az_unl_port = float(sub["azure_unlimited_port"].min())

            # Data transfer fees
            if dt_df.empty or "region_canonical" not in dt_df.columns:
                dt_row = pd.DataFrame()
            else:
                dt_row = dt_df[dt_df["region_canonical"] == region]
            gcp_gb = float(dt_row["gcp_per_gb"].min()) if not dt_row.empty and dt_row["gcp_per_gb"].notna().any() else None
            aws_gb = float(dt_row["aws_per_gb"].min()) if not dt_row.empty and dt_row["aws_per_gb"].notna().any() else None
            az_gb  = float(dt_row["azure_per_gb"].min()) if not dt_row.empty and dt_row["azure_per_gb"].notna().any() else None

            def _total(port, per_gb, include_egress=True):
                if port is None:
                    return None
                dt = ((per_gb or 0) * data_gb) if include_egress else 0.0
                return round(port + dt, 2)

            gcp_tot = _total(gcp_port, gcp_gb)
            aws_tot = _total(aws_port, aws_gb)
            az_met_tot = _total(az_port, az_gb)                        # metered: port + egress
            az_unl_tot = _total(az_unl_port, None, include_egress=False)  # unlimited: port only

            # Per-circuit normalised Azure price (÷ AZURE_CIRCUITS_PER_PURCHASE)
            az_met_per_circuit = round(az_met_tot / AZURE_CIRCUITS_PER_PURCHASE, 2) if az_met_tot else None
            az_unl_per_circuit = round(az_unl_tot / AZURE_CIRCUITS_PER_PURCHASE, 2) if az_unl_tot else None

            # Cheapest based on available totals
            totals = {}
            if gcp_tot:  totals["GCP"] = gcp_tot
            if aws_tot:  totals["AWS"] = aws_tot
            if az_met_per_circuit: totals["Azure (per-circuit)"] = az_met_per_circuit
            cheapest = min(totals, key=totals.get) if totals else "N/A"

            rows.append({
                "scenario": sc["label"],
                "speed_gbps": speed,
                "util_pct": round(util_pct, 1),
                "data_gb": round(data_gb, 0),
                "data_tb": round(data_tb, 2),
                "gcp_total": gcp_tot,
                "aws_total": aws_tot,
                "azure_metered_total": az_met_tot,
                "azure_unlimited_total": az_unl_tot,
                "azure_metered_per_circuit": az_met_per_circuit,
                "azure_unlimited_per_circuit": az_unl_per_circuit,
                "cheapest": cheapest,
                "gcp_savings_vs_aws": round(aws_tot - gcp_tot, 2) if (gcp_tot and aws_tot) else None,
                "gcp_savings_vs_azure_metered": round(az_met_per_circuit - gcp_tot, 2) if (gcp_tot and az_met_per_circuit) else None,
            })

        return pd.DataFrame(rows)

    def service_type_comparison(
        self,
        speed_gbps: float = 10.0,
        region: str = "us_east",
    ) -> pd.DataFrame:
        """
        Overview table comparing all interconnect flavours across providers.

        Returns one row per service tier with columns:
          tier_label | description | gcp_product | gcp_price
          | aws_product | aws_price
          | azure_product | azure_list_price | azure_per_circuit | azure_note
          | notes
        """
        if self.df.empty:
            return pd.DataFrame()

        df = self.df.copy()
        region_label = REGION_LABELS.get(region, region)

        def _min_price(mask_df):
            """Return min monthly price or None."""
            sub = mask_df[
                (mask_df["price_monthly_usd"] > 0) &
                (mask_df["port_speed_gbps"] == speed_gbps) &
                (mask_df["region_canonical"] == region)
            ]
            if sub.empty:
                # Try without region constraint
                sub = mask_df[
                    (mask_df["price_monthly_usd"] > 0) &
                    (mask_df["port_speed_gbps"] == speed_gbps)
                ]
            return float(sub["price_monthly_usd"].min()) if not sub.empty else None

        def _fmt(val, per_circuit=False):
            if val is None:
                return "—"
            return f"${val:,.0f}/mo"

        rows = []

        # ── 1. Dedicated / Direct ─────────────────────────────────────────────
        gcp_ded = df[
            (df["provider"] == "gcp") &
            (df["service"] == "dedicated_interconnect") &
            (df["plan_type"] == "dedicated") &
            (~df["sku_name"].str.contains("Cross-Cloud", case=False, na=False)) &
            (~df["sku_name"].str.contains("Application Awareness", case=False, na=False)) &
            (df["sku_name"].str.contains(r"\[Circuit\]", regex=True, na=False) |
             df["sku_name"].str.contains("Port", case=False, na=False) |
             df["sku_name"].str.contains("Reference", case=False, na=False))
        ]
        aws_ded = df[(df["provider"] == "aws") & (df["service"] == "direct_connect") & (df["plan_type"] == "dedicated")]
        az_direct = df[(df["provider"] == "azure") & (df["service"] == "expressroute_direct")]

        gcp_ded_price = _min_price(gcp_ded)
        aws_ded_price = _min_price(aws_ded)
        az_direct_price = _min_price(az_direct)
        az_direct_per_ckt = round(az_direct_price / AZURE_CIRCUITS_PER_PURCHASE, 0) if az_direct_price else None

        rows.append({
            "Service Tier": "Dedicated / Direct",
            "Description": "Physical port at colo facility. Fixed monthly fee.",
            "GCP Product": "Dedicated Interconnect",
            "GCP Price": _fmt(gcp_ded_price),
            "AWS Product": "Direct Connect (Dedicated)",
            "AWS Price": _fmt(aws_ded_price),
            "Azure Product": "ExpressRoute Direct",
            "Azure List Price": _fmt(az_direct_price),
            "Azure Per-Circuit": _fmt(az_direct_per_ckt),
            "Notes": f"Azure includes {AZURE_CIRCUITS_PER_PURCHASE} circuits per purchase (primary + secondary).",
            "Pricing Scope": "GCP: Global flat rate | AWS: Regional | Azure: Zone/Continental",
            "Models Available": "Metered only (port fee + egress billed separately)",
        })

        # ── 2. Partner / Hosted (Metered) ─────────────────────────────────────
        gcp_part = df[
            (df["provider"] == "gcp") &
            (df["service"] == "partner_interconnect") &
            (df["sku_name"].str.contains(r"\[Circuit\]", regex=True, na=False) |
             df["sku_name"].str.contains("Reference", case=False, na=False))
        ]
        aws_hosted = df[(df["provider"] == "aws") & (df["service"] == "direct_connect") & (df["plan_type"] == "hosted")]
        az_met = df[(df["provider"] == "azure") & (df["service"] == "expressroute") & (df["plan_type"].isin(["metered", "standard"]))]

        gcp_part_price = _min_price(gcp_part)
        aws_hosted_price = _min_price(aws_hosted)
        az_met_price = _min_price(az_met)
        az_met_per_ckt = round(az_met_price / AZURE_CIRCUITS_PER_PURCHASE, 0) if az_met_price else None

        rows.append({
            "Service Tier": "Partner / Hosted (Metered)",
            "Description": "Virtual circuit via service provider. Port + per-GB egress.",
            "GCP Product": "Partner Interconnect",
            "GCP Price": _fmt(gcp_part_price),
            "AWS Product": "Direct Connect (Hosted)",
            "AWS Price": _fmt(aws_hosted_price),
            "Azure Product": "ExpressRoute Circuit (Metered)",
            "Azure List Price": _fmt(az_met_price),
            "Azure Per-Circuit": _fmt(az_met_per_ckt),
            "Notes": f"Azure includes {AZURE_CIRCUITS_PER_PURCHASE} circuits. GCP & AWS: port fee only; egress billed separately.",
            "Pricing Scope": "GCP: Global flat rate | AWS: Regional | Azure: Zone/Continental",
            "Models Available": "Metered (port + per-GB egress)",
        })

        # ── 3. Partner / Hosted (Unlimited) ───────────────────────────────────
        az_unl = df[(df["provider"] == "azure") & (df["service"] == "expressroute") & (df["plan_type"] == "unlimited")]
        az_unl_price = _min_price(az_unl)
        az_unl_per_ckt = round(az_unl_price / AZURE_CIRCUITS_PER_PURCHASE, 0) if az_unl_price else None

        rows.append({
            "Service Tier": "Partner / Hosted (Unlimited Egress)",
            "Description": "Flat monthly fee — no per-GB egress charges.",
            "GCP Product": "Not available",
            "GCP Price": "—",
            "AWS Product": "Not available",
            "AWS Price": "—",
            "Azure Product": "ExpressRoute Circuit (Unlimited)",
            "Azure List Price": _fmt(az_unl_price),
            "Azure Per-Circuit": _fmt(az_unl_per_ckt),
            "Notes": f"Azure-only offering. Includes {AZURE_CIRCUITS_PER_PURCHASE} circuits. No per-GB egress fee.",
            "Pricing Scope": "Azure only — Zone/Continental",
            "Models Available": "Unlimited (flat port fee, no per-GB egress)",
        })

        # ── 4. Cross-Cloud Interconnect ────────────────────────────────────────
        gcp_cc = df[
            (df["provider"] == "gcp") &
            (df["sku_name"].str.contains("Cross-Cloud", case=False, na=False)) &
            (df["price_monthly_usd"] > 0)
        ]
        gcp_cc_price = _min_price(gcp_cc) if not gcp_cc.empty else None

        rows.append({
            "Service Tier": "Cross-Cloud Interconnect",
            "Description": "Physical link between two cloud providers (e.g. GCP ↔ AWS).",
            "GCP Product": "Cross-Cloud Interconnect",
            "GCP Price": _fmt(gcp_cc_price) if gcp_cc_price else "See GCP pricing",
            "AWS Product": "No equivalent",
            "AWS Price": "—",
            "Azure Product": "No equivalent",
            "Azure List Price": "—",
            "Azure Per-Circuit": "—",
            "Notes": "GCP-exclusive. AWS/Azure: use internet peering or partner solutions.",
            "Pricing Scope": "GCP: Global flat rate",
            "Models Available": "Metered only",
        })

        # ── 5. Site-to-Site Extension ──────────────────────────────────────────
        az_gr = df[(df["provider"] == "azure") & (df["service"] == "expressroute_global_reach") & (df["price_monthly_usd"] > 0)]
        az_gr_price = None
        if not az_gr.empty:
            sub = az_gr[az_gr["region_canonical"] == region]
            if sub.empty:
                sub = az_gr
            az_gr_price = float(sub["price_monthly_usd"].min()) if not sub.empty else None

        rows.append({
            "Service Tier": "Site-to-Site Extension",
            "Description": "Link on-prem sites via cloud backbone. Add-on fee on existing circuits.",
            "GCP Product": "Cross-Site Interconnect / NCC",
            "GCP Price": "Add-on",
            "AWS Product": "Direct Connect SiteLink",
            "AWS Price": "Add-on",
            "Azure Product": "ExpressRoute Global Reach",
            "Azure List Price": _fmt(az_gr_price) if az_gr_price else "Add-on",
            "Azure Per-Circuit": "—",
            "Notes": "Billed as add-on per port/circuit. Requires existing interconnect.",
            "Pricing Scope": "GCP: Global | AWS: Regional | Azure: Zone/Continental",
            "Models Available": "Add-on fee on existing circuit",
        })

        result = pd.DataFrame(rows)
        result.attrs["region_label"] = region_label
        result.attrs["speed_gbps"] = speed_gbps
        return result

    def regional_breakdown(
        self,
        speeds_gbps: Optional[List[float]] = None,
        regions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Full regional price breakdown: all regions × speeds × providers.

        Shows GCP, AWS, Azure metered (list + per-circuit), and Azure unlimited.

        Returns DataFrame with columns:
          region_label | port_speed_gbps
          | gcp_monthly | aws_monthly
          | azure_metered | azure_metered_per_circuit
          | azure_unlimited | azure_unlimited_per_circuit
          | gcp_vs_aws_pct | gcp_vs_azure_per_circuit_pct
        """
        if self.df.empty:
            return pd.DataFrame()

        df = self.df.copy()

        # ── GCP dedicated circuits ─────────────────────────────────────────────
        df_gcp = df[
            (df["provider"] == "gcp") &
            (df["service"].isin(["dedicated_interconnect", "partner_interconnect"])) &
            (df["price_monthly_usd"] > 0) &
            (df["sku_name"].str.contains(r"\[Circuit\]", regex=True, na=False) |
             df["sku_name"].str.contains("Reference", case=False, na=False) |
             df["sku_name"].str.contains("Port", case=False, na=False)) &
            (~df["sku_name"].str.contains("Cross-Cloud", case=False, na=False)) &
            (~df["sku_name"].str.contains("Application Awareness", case=False, na=False))
        ]

        # ── AWS dedicated ──────────────────────────────────────────────────────
        df_aws = df[
            (df["provider"] == "aws") &
            (df["service"] == "direct_connect") &
            (df["plan_type"] == "dedicated") &
            (df["price_monthly_usd"] > 0)
        ]

        # ── Azure metered ──────────────────────────────────────────────────────
        df_az_met = df[
            (df["provider"] == "azure") &
            (df["service"].isin(["expressroute", "expressroute_direct"])) &
            (df["plan_type"].isin(["metered", "standard", "expressroute_direct"])) &
            (df["price_monthly_usd"] > 0)
        ]

        # ── Azure unlimited ────────────────────────────────────────────────────
        df_az_unl = df[
            (df["provider"] == "azure") &
            (df["service"] == "expressroute") &
            (df["plan_type"] == "unlimited") &
            (df["price_monthly_usd"] > 0)
        ]

        # Propagate GCP global SKUs to every canonical region
        df_gcp = self._propagate_gcp_global(df_gcp, regions)

        # Apply optional filters
        if speeds_gbps:
            df_gcp    = df_gcp[df_gcp["port_speed_gbps"].isin(speeds_gbps)]
            df_aws    = df_aws[df_aws["port_speed_gbps"].isin(speeds_gbps)]
            df_az_met = df_az_met[df_az_met["port_speed_gbps"].isin(speeds_gbps)]
            df_az_unl = df_az_unl[df_az_unl["port_speed_gbps"].isin(speeds_gbps)]

        if regions:
            df_gcp    = df_gcp[df_gcp["region_canonical"].isin(regions)]
            df_aws    = df_aws[df_aws["region_canonical"].isin(regions)]
            df_az_met = df_az_met[df_az_met["region_canonical"].isin(regions)]
            df_az_unl = df_az_unl[df_az_unl["region_canonical"].isin(regions)]

        def _agg(sub, price_col="price_monthly_usd", name=None):
            result = (
                sub.groupby(["region_canonical", "region_label", "port_speed_gbps"])
                [price_col]
                .min()
                .reset_index()
            )
            if name:
                result = result.rename(columns={price_col: name})
            return result

        g = _agg(df_gcp, name="gcp_monthly")
        a = _agg(df_aws, name="aws_monthly")
        m = _agg(df_az_met, name="azure_metered")
        u = _agg(df_az_unl, name="azure_unlimited")

        merged = (
            g.merge(a, on=["region_canonical", "port_speed_gbps"], how="outer",
                    suffixes=("", "_aws"))
             .merge(m, on=["region_canonical", "port_speed_gbps"], how="outer",
                    suffixes=("", "_azm"))
             .merge(u, on=["region_canonical", "port_speed_gbps"], how="outer",
                    suffixes=("", "_azu"))
        )

        # Consolidate region_label columns
        for suffix in ("_aws", "_azm", "_azu"):
            col = f"region_label{suffix}"
            if col in merged.columns:
                merged["region_label"] = merged["region_label"].combine_first(merged.pop(col))

        merged["region_label"] = merged["region_label"].combine_first(
            merged["region_canonical"].map(REGION_LABELS)
        )

        # Per-circuit Azure normalised prices
        merged["azure_metered_per_circuit"] = (merged["azure_metered"] / AZURE_CIRCUITS_PER_PURCHASE).round(0)
        merged["azure_unlimited_per_circuit"] = (merged["azure_unlimited"] / AZURE_CIRCUITS_PER_PURCHASE).round(0)

        # % difference (GCP vs others, per-circuit basis for Azure)
        merged["gcp_vs_aws_pct"] = (
            (merged["gcp_monthly"] - merged["aws_monthly"]) / merged["aws_monthly"] * 100
        ).round(1)
        merged["gcp_vs_azure_pct"] = (
            (merged["gcp_monthly"] - merged["azure_metered_per_circuit"])
            / merged["azure_metered_per_circuit"] * 100
        ).round(1)

        merged = merged.sort_values(["region_canonical", "port_speed_gbps"]).reset_index(drop=True)
        return merged

    def regional_coverage(self) -> pd.DataFrame:
        """
        Which providers offer service in which canonical regions?
        Returns pivot: region_label | gcp | aws | azure (bool)
        """
        if self.df.empty:
            return pd.DataFrame()

        coverage = (
            self.df[self.df["price_monthly_usd"] > 0]
            .groupby(["region_canonical", "provider"])
            .size()
            .reset_index(name="sku_count")
        )
        pivot = coverage.pivot(
            index="region_canonical", columns="provider", values="sku_count"
        ).fillna(0).astype(int)

        pivot.index = pivot.index.map(lambda r: REGION_LABELS.get(r, r))
        pivot.index.name = "Region"
        result = pivot.reset_index()

        # Add pricing scope notes per provider
        scope_notes = {
            "gcp":   "Global flat rate (same price in all regions)",
            "aws":   "Regional (per-location pricing)",
            "azure": "Zone-based / Continental (Zone 1–4 for circuits)",
        }
        # Add model type notes per provider
        model_notes = {
            "gcp":   "Metered (port + egress)",
            "aws":   "Metered (port + egress)",
            "azure": "Metered & Unlimited",
        }
        result.attrs["scope_notes"]  = scope_notes
        result.attrs["model_notes"]  = model_notes
        return result

    def headline_metrics(self) -> dict:
        """
        Key metrics for the executive summary card.
        Returns dict with pre-formatted strings.
        """
        port_df = self.port_fee_comparison(speeds_gbps=[10.0])
        dt_df = self.data_transfer_comparison()

        def _pct_str(val) -> str:
            if val is None or pd.isna(val):
                return "N/A"
            sign = "+" if val > 0 else ""
            return f"{sign}{val:.1f}%"

        def _dollar(val) -> str:
            if val is None or pd.isna(val):
                return "N/A"
            return f"${val:,.0f}/mo"

        # 10G US East comparison
        us_row = port_df[port_df["region_canonical"] == "us_east"] if not port_df.empty else pd.DataFrame()

        gcp_10g = us_row["gcp_monthly"].values[0] if not us_row.empty else None
        aws_10g = us_row["aws_monthly"].values[0] if not us_row.empty else None
        az_10g = us_row["azure_monthly"].values[0] if not us_row.empty else None

        pct_vs_aws = ((gcp_10g - aws_10g) / aws_10g * 100) if (gcp_10g and aws_10g) else None
        pct_vs_az = ((gcp_10g - az_10g) / az_10g * 100) if (gcp_10g and az_10g) else None

        # Data transfer US East
        dt_us = dt_df[dt_df["region_canonical"] == "us_east"] if not dt_df.empty else pd.DataFrame()
        gcp_gb = dt_us["gcp_per_gb"].values[0] if not dt_us.empty else None
        aws_gb = dt_us["aws_per_gb"].values[0] if not dt_us.empty else None
        az_gb = dt_us["azure_per_gb"].values[0] if not dt_us.empty else None

        # Overall position — rank-based so the headline is always accurate.
        # Averaging pct vs each competitor is misleading when GCP is cheaper
        # than one but more expensive than another.
        def _is_valid(v) -> bool:
            return v is not None and not (isinstance(v, float) and pd.isna(v)) and v > 0

        valid = [(v, n) for v, n in [(gcp_10g, "GCP"), (aws_10g, "AWS"), (az_10g, "Azure")]
                 if _is_valid(v)]
        if len(valid) >= 2 and _is_valid(gcp_10g):
            ranked = sorted(valid, key=lambda x: x[0])
            gcp_rank = next(i + 1 for i, (v, n) in enumerate(ranked) if n == "GCP")
            n_providers = len(ranked)

            if gcp_rank == 1:
                # GCP is cheapest — how much cheaper than next?
                next_price = ranked[1][0]
                pct_vs_next = (next_price - gcp_10g) / next_price * 100
                if pct_vs_next > 10:
                    position, position_color = "Significantly Cheaper", "#34A853"
                elif pct_vs_next > 2:
                    position, position_color = "Slightly Cheaper", "#34A853"
                else:
                    position, position_color = "Roughly at Parity", "#FBBC05"
            elif gcp_rank == n_providers:
                # GCP is most expensive
                position, position_color = "Most Expensive", "#EA4335"
            else:
                # GCP is in the middle — report how close it is to the cheapest
                cheapest = ranked[0][0]
                pct_above_cheapest = (gcp_10g - cheapest) / cheapest * 100
                if pct_above_cheapest <= 5:
                    position, position_color = "Competitive (Mid-Priced)", "#FBBC05"
                else:
                    position, position_color = "Mid-Priced", "#FBBC05"
        else:
            position, position_color = "See Details", "#666"

        return {
            "gcp_10g_us": _dollar(gcp_10g),
            "aws_10g_us": _dollar(aws_10g),
            "azure_10g_us": _dollar(az_10g),
            "gcp_vs_aws_pct": _pct_str(pct_vs_aws),
            "gcp_vs_azure_pct": _pct_str(pct_vs_az),
            "gcp_vs_aws_raw": pct_vs_aws,
            "gcp_vs_azure_raw": pct_vs_az,
            "gcp_data_gb": f"${gcp_gb:.4f}/GB" if gcp_gb else "N/A",
            "aws_data_gb": f"${aws_gb:.4f}/GB" if aws_gb else "N/A",
            "azure_data_gb": f"${az_gb:.4f}/GB" if az_gb else "N/A",
            "competitive_position": position,
            "position_color": position_color,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _clean(self):
        num_cols = ["port_speed_gbps", "price_monthly_usd", "price_per_gb_usd"]
        for col in num_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce").fillna(0)
        if "region_label" not in self.df.columns:
            self.df["region_label"] = self.df["region_canonical"].map(REGION_LABELS)

    @staticmethod
    def _propagate_gcp_global(df_gcp: pd.DataFrame, target_regions: Optional[List[str]] = None) -> pd.DataFrame:
        """
        GCP prices most Interconnect SKUs as global flat-rate (region_raw='global').
        Replicate those rows to every canonical region so regional comparisons show
        a GCP price in every region rather than only in 'us_east' (the fallback).
        """
        if df_gcp.empty or "region_raw" not in df_gcp.columns:
            return df_gcp

        gcp_global  = df_gcp[df_gcp["region_raw"] == "global"].copy()
        gcp_regional = df_gcp[df_gcp["region_raw"] != "global"].copy()

        if gcp_global.empty:
            return df_gcp

        regions = target_regions or list(REGION_LABELS.keys())
        copies = []
        for canon in regions:
            c = gcp_global.copy()
            c["region_canonical"] = canon
            c["region_label"]     = REGION_LABELS.get(canon, canon)
            copies.append(c)

        return pd.concat([gcp_regional] + copies, ignore_index=True)
