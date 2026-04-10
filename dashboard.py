"""
Streamlit interactive dashboard for GCP Networking Price Intelligence.

Run:
    streamlit run dashboard.py

Features:
  - Live provider/region/speed filters
  - Port fee comparison charts
  - Data transfer comparison
  - TCO scenario analysis
  - Regional coverage heatmap
  - Price change history log
  - On-demand fetch + report button
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from storage.store import PriceStore
from analysis.compare import (
    PriceComparator, PROVIDER_META, REGION_LABELS,
    AZURE_CIRCUITS_PER_PURCHASE, SERVICE_TIERS,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GCP Networking Price Intelligence",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour palette ────────────────────────────────────────────────────────────
GCP_COLOR = "#4285F4"
AWS_COLOR = "#FF9900"
AZ_COLOR = "#008AD7"
GREEN = "#34A853"
RED = "#D93025"
YELLOW = "#F9AB00"

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  [data-testid="stSidebar"] { background: #F8F9FA; }
  .metric-card {
    background: white; border: 1px solid #E8EAED; border-radius: 12px;
    padding: 16px 20px; margin: 4px 0;
  }
  .metric-label { font-size: .72rem; font-weight: 600; color: #5F6368;
                  text-transform: uppercase; letter-spacing: .5px; }
  .metric-value { font-size: 1.6rem; font-weight: 500; }
  .metric-sub   { font-size: .75rem; color: #5F6368; margin-top: 2px; }
  div[data-testid="stMetric"] { background: white; border: 1px solid #E8EAED;
                                 border-radius: 10px; padding: 12px 16px; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

@st.cache_resource
def get_store() -> PriceStore:
    return PriceStore()


def _load_config() -> dict:
    import yaml, os
    cfg_path = Path(__file__).parent / "config" / "settings.yaml"
    if cfg_path.exists():
        with open(cfg_path) as f:
            return yaml.safe_load(f)
    return {}


def _get_comparator(store: PriceStore, run_id: str) -> PriceComparator:
    records = store.get_prices_for_run(run_id)
    return PriceComparator(records)


def _pct_delta_color(val) -> str:
    if val is None or pd.isna(val):
        return "off"
    return "normal" if val < 0 else "inverse"


def _fmt_pct(val) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


# ── Sidebar ────────────────────────────────────────────────────────────────────

def render_sidebar(store: PriceStore) -> dict:
    st.sidebar.image(
        "https://www.gstatic.com/images/branding/googlelogo/svg/googlelogo_clr_74x24px.svg",
        width=80,
    )
    st.sidebar.markdown("## ☁️ Network Price Intel")
    st.sidebar.markdown("---")

    runs = store.list_runs(limit=10)
    completed_runs = [r for r in runs if r["status"] == "completed"]

    if not completed_runs:
        st.sidebar.warning("No data yet. Run a fetch first.")
        if st.sidebar.button("🔄 Fetch Prices Now", type="primary", use_container_width=True):
            st.session_state["trigger_fetch"] = True
        return {}

    run_options = {r["run_id"][:19].replace("T", " "): r["run_id"] for r in completed_runs}
    selected_label = st.sidebar.selectbox(
        "Data Snapshot", list(run_options.keys()), index=0,
        help="Select which fetch run to analyse"
    )
    run_id = run_options[selected_label]

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Filters**")

    all_regions = list(REGION_LABELS.values())
    selected_regions = st.sidebar.multiselect(
        "Regions", all_regions, default=all_regions[:4],
        help="Filter by geographic region"
    )
    region_keys = [k for k, v in REGION_LABELS.items() if v in selected_regions]

    speed_options = [1.0, 10.0, 100.0, 400.0]
    selected_speeds = st.sidebar.multiselect(
        "Port Speeds (Gbps)", speed_options, default=[1.0, 10.0, 100.0],
        help="Filter by connection speed"
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Actions**")

    col1, col2 = st.sidebar.columns(2)
    fetch_clicked = col1.button("🔄 Fetch", use_container_width=True, help="Fetch latest prices")
    report_clicked = col2.button("📄 Report", use_container_width=True, help="Generate HTML report")

    if fetch_clicked:
        st.session_state["trigger_fetch"] = True
    if report_clicked:
        st.session_state["trigger_report"] = run_id

    return {
        "run_id": run_id,
        "regions": region_keys,
        "speeds": selected_speeds,
    }


# ── Fetch action ──────────────────────────────────────────────────────────────

def do_fetch(config: dict, store: PriceStore):
    from fetchers import GCPFetcher, AWSFetcher, AzureFetcher
    from notifications import Notifier

    progress = st.progress(0, text="Starting fetch...")
    status = st.empty()
    total_points = 0

    run_id = store.start_run(["gcp", "aws", "azure"])
    prev_run = store.get_previous_run(run_id)

    fetchers = [
        ("GCP Cloud Interconnect", GCPFetcher(config)),
        ("AWS Direct Connect", AWSFetcher(config)),
        ("Azure ExpressRoute", AzureFetcher(config)),
    ]

    all_points = []
    for i, (label, fetcher) in enumerate(fetchers):
        status.info(f"Fetching {label}…")
        progress.progress((i) / len(fetchers), text=f"Fetching {label}…")
        try:
            points = fetcher.fetch()
            saved = store.save_prices(run_id, points)
            all_points.extend(points)
            total_points += saved
            status.success(f"✓ {label}: {saved} price points")
        except Exception as e:
            status.error(f"✗ {label}: {e}")

    progress.progress(1.0, text="Detecting changes…")
    changes = store.detect_and_save_changes(run_id, prev_run)
    store.complete_run(run_id, total_points)

    if changes:
        notifier = Notifier(config)
        notifier.send(changes)
        unnotified_ids = [c["id"] for c in store.get_unnotified_changes() if c.get("id")]
        if unnotified_ids:
            store.mark_changes_notified(unnotified_ids)

    status.success(f"✅ Fetch complete: {total_points} price points, {len(changes)} changes detected.")
    progress.empty()
    st.cache_resource.clear()
    st.rerun()


def do_report(run_id: str, config: dict, store: PriceStore):
    from reports import HTMLReportGenerator
    records = store.get_prices_for_run(run_id)
    comp = PriceComparator(records)
    changes = store.get_recent_changes(50)
    gen = HTMLReportGenerator(comp, run_id, changes, config)
    path = gen.generate()
    st.success(f"📄 Report generated: `{path}`")
    # Offer download
    with open(path, "rb") as f:
        st.download_button(
            "⬇️ Download HTML Report",
            f.read(),
            file_name=Path(path).name,
            mime="text/html",
            use_container_width=True,
        )


# ── Chart helpers ─────────────────────────────────────────────────────────────

def _bar_chart(df: pd.DataFrame, x: str, y_cols: list,
               names: list, colors: list, title: str, y_label: str = "") -> go.Figure:
    fig = go.Figure()
    for col, name, color in zip(y_cols, names, colors):
        if col in df.columns:
            fig.add_trace(go.Bar(
                name=name, x=df[x], y=df[col],
                marker_color=color, marker_line_width=0,
                hovertemplate=f"<b>{name}</b><br>{y_label} %{{y:,.2f}}<extra></extra>",
            ))
    fig.update_layout(
        title=title, barmode="group", plot_bgcolor="white",
        paper_bgcolor="white", font_family="Google Sans, Segoe UI, sans-serif",
        title_font_size=13, legend=dict(orientation="h", y=-0.2),
        margin=dict(t=40, b=60, l=50, r=20),
        xaxis=dict(gridcolor="#F1F3F4"),
        yaxis=dict(gridcolor="#F1F3F4", title=y_label),
    )
    return fig


# ── Tabs ──────────────────────────────────────────────────────────────────────

def tab_overview(comp: PriceComparator):
    st.subheader("Executive Overview")
    m = comp.headline_metrics()

    # Position badge
    pos = m["competitive_position"]
    pos_colors = {
        "Significantly Cheaper": GREEN, "Slightly Cheaper": GREEN,
        "Roughly at Parity": YELLOW, "Competitive (Mid-Priced)": YELLOW,
        "Mid-Priced": YELLOW, "Most Expensive": RED, "More Expensive": RED,
    }
    color = pos_colors.get(pos, "#666")
    st.markdown(
        f'<div style="background:#F8F9FA;border-left:4px solid {color};padding:12px 20px;'
        f'border-radius:0 8px 8px 0;margin-bottom:16px">'
        f'<strong style="color:{color};font-size:1.05rem">GCP is {pos}</strong> vs. competitors '
        f'for 10 Gbps dedicated connectivity (US East)</div>',
        unsafe_allow_html=True,
    )

    # KPI row
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("GCP Interconnect (10G, US)", m["gcp_10g_us"], help="Monthly port fee, dedicated")
    with c2:
        delta_str = m.get("gcp_vs_aws_pct", "N/A")
        raw = m.get("gcp_vs_aws_raw")
        st.metric(
            "AWS Direct Connect (10G, US)", m["aws_10g_us"],
            delta=delta_str if raw else None,
            delta_color=_pct_delta_color(raw),
            help="Positive delta = GCP is more expensive than AWS",
        )
    with c3:
        raw_az = m.get("gcp_vs_azure_raw")
        st.metric(
            "Azure ExpressRoute (10G, US)", m["azure_10g_us"],
            delta=m.get("gcp_vs_azure_pct", "N/A") if raw_az else None,
            delta_color=_pct_delta_color(raw_az),
            help="Positive delta = GCP is more expensive than Azure",
        )

    st.markdown("---")
    c4, c5, c6 = st.columns(3)
    c4.metric("GCP Data Transfer Out", m["gcp_data_gb"], help="$/GB via Cloud Interconnect")
    c5.metric("AWS Data Transfer Out", m["aws_data_gb"], help="$/GB via Direct Connect")
    c6.metric("Azure Data Transfer Out", m["azure_data_gb"], help="$/GB via ExpressRoute Metered")

    # ── Service-type comparison table ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Interconnect Flavour Comparison")
    st.caption(
        "All 4 interconnect types compared across providers for a single reference point. "
        "Prices shown at 10 Gbps, US East equivalent region for each provider."
    )

    # Controls in a compact row
    ov_c1, ov_c2, _ = st.columns([1, 1, 3])
    with ov_c1:
        ov_speed = st.selectbox(
            "Speed", [1.0, 10.0, 100.0], index=1, key="ov_speed",
            format_func=lambda x: f"{x:.0f} Gbps",
        )
    with ov_c2:
        ov_region_label = st.selectbox(
            "Reference Region", list(REGION_LABELS.values()), index=0, key="ov_region",
        )
    ov_region = next(k for k, v in REGION_LABELS.items() if v == ov_region_label)

    svc_df = comp.service_type_comparison(speed_gbps=ov_speed, region=ov_region)

    if not svc_df.empty:
        # ── per-row Azure price cell: show per-circuit prominently ─────────────
        def _render_azure_cell(list_price, per_ckt):
            if list_price in ("—", None) or (isinstance(list_price, float) and pd.isna(list_price)):
                return "—"
            return f"**{per_ckt}**/ckt  \n<small style='color:#9AA0A6'>list: {list_price} for 2 ckts</small>"

        # Build display table with restructured Azure column
        rows_out = []
        for _, row in svc_df.iterrows():
            azure_combined = (
                f"{row['Azure Per-Circuit']}/ckt  (list {row['Azure List Price']})"
                if row["Azure List Price"] not in ("—",) else "—"
            )
            rows_out.append({
                "Service Tier":      row["Service Tier"],
                "GCP Product":       row["GCP Product"],
                "GCP Price":         row["GCP Price"],
                "AWS Product":       row["AWS Product"],
                "AWS Price":         row["AWS Price"],
                "Azure Product":     row["Azure Product"],
                "Azure Price (per-circuit)": azure_combined,
                "Pricing Scope":     row.get("Pricing Scope", ""),
                "Models Available":  row.get("Models Available", ""),
            })
        display_df = pd.DataFrame(rows_out)

        def _style_svc(val):
            if isinstance(val, str) and val in ("—", "Not available", "No equivalent"):
                return "color:#9AA0A6; font-style:italic"
            if isinstance(val, str) and val.startswith("$"):
                return "font-weight:500"
            return ""

        st.dataframe(
            display_df.style.map(_style_svc),
            use_container_width=True,
            hide_index=True,
            height=240,
        )

        with st.expander("ℹ️  How to read this table"):
            st.markdown(f"""
- **Azure Price (per-circuit)** = list price ÷ {AZURE_CIRCUITS_PER_PURCHASE}. Each Azure port purchase includes a **primary and secondary circuit** for built-in redundancy. This normalises to per-circuit for fair comparison with GCP/AWS (which include 1 circuit each).
- **Pricing Scope** shows how each provider structures geographic pricing:
  - 🌍 **GCP: Global flat rate** — same price in every region
  - 📍 **AWS: Regional** — price varies by AWS Direct Connect location
  - 🌐 **Azure: Zone/Continental** — Zone 1 (Americas/Europe), Zone 2 (Europe), Zone 3 (Asia), Zone 4 (Australia)
- **Models Available**: Azure offers both Metered (port + per-GB egress) and Unlimited (fixed fee, all egress included). GCP and AWS are metered only.
- **Partner / Hosted (Unlimited)** is Azure-only — breakeven vs Metered at moderate-to-high utilisation.
- **Cross-Cloud Interconnect** is GCP-exclusive; AWS/Azure have no direct equivalent.
            """)
    else:
        st.info("No data available. Run a fetch first.")


def tab_port_fees(comp: PriceComparator, filters: dict):
    st.subheader("Port & Circuit Fee Comparison")
    st.caption("Dedicated private connectivity — monthly fees in USD.")

    port_df = comp.port_fee_comparison(
        speeds_gbps=filters.get("speeds"),
        regions=filters.get("regions"),
    )

    if port_df.empty:
        st.info("No data available for selected filters.")
        return

    # Chart: 10G by region
    df10 = port_df[port_df["port_speed_gbps"] == 10.0]
    if not df10.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = _bar_chart(
                df10, "region_label",
                ["gcp_monthly", "aws_monthly", "azure_monthly"],
                ["GCP Interconnect", "AWS Direct Connect", "Azure ExpressRoute"],
                [GCP_COLOR, AWS_COLOR, AZ_COLOR],
                "10 Gbps Port Fee by Region", "$/month",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Chart: all speeds, US East
            df_us = port_df[port_df["region_canonical"] == "us_east"].sort_values("port_speed_gbps")
            if not df_us.empty:
                fig2 = _bar_chart(
                    df_us, "port_speed_gbps",
                    ["gcp_monthly", "aws_monthly", "azure_monthly"],
                    ["GCP Interconnect", "AWS Direct Connect", "Azure ExpressRoute"],
                    [GCP_COLOR, AWS_COLOR, AZ_COLOR],
                    "Port Fee by Speed — US East", "$/month",
                )
                fig2.update_xaxes(title="Speed (Gbps)")
                st.plotly_chart(fig2, use_container_width=True)

    # Detailed table
    st.markdown("**Detailed Comparison Table**")
    display = port_df[["region_label", "port_speed_gbps",
                        "gcp_monthly", "aws_monthly", "azure_monthly",
                        "gcp_vs_aws_pct", "gcp_vs_azure_pct"]].copy()
    display.columns = ["Region", "Speed (Gbps)", "GCP ($/mo)", "AWS ($/mo)", "Azure ($/mo)",
                        "GCP vs AWS", "GCP vs Azure"]

    def _highlight_pct(val):
        if pd.isna(val):
            return ""
        if val < -5:
            return "color: #1E8E3E; font-weight: bold"
        if val < 0:
            return "color: #34A853"
        if val < 5:
            return "color: #F9AB00"
        return "color: #D93025; font-weight: bold"

    styled = (
        display.style
        .format({
            "Speed (Gbps)": "{:.0f}",
            "GCP ($/mo)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "AWS ($/mo)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "Azure ($/mo)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "GCP vs AWS": lambda v: f"{v:+.1f}%" if pd.notna(v) else "—",
            "GCP vs Azure": lambda v: f"{v:+.1f}%" if pd.notna(v) else "—",
        })
        .map(_highlight_pct, subset=["GCP vs AWS", "GCP vs Azure"])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    with st.expander("Interpretation guide"):
        st.markdown("""
- **Negative %** = GCP is *cheaper* than competitor (good for GCP)
- **Positive %** = GCP is *more expensive* (area for improvement)
- AWS price = per-hour fee × 730 hours/month
- Azure price = metered circuit monthly fee (closest to GCP/AWS model)
        """)


def tab_tco(comp: PriceComparator, config: dict):
    st.subheader("Total Cost of Ownership Scenarios")

    # ── Scenario builder ──────────────────────────────────────────────────────
    st.markdown("#### Build Scenarios")
    st.caption(
        "Monthly cost = port/circuit fee + data transfer. "
        "Specify utilisation **%** — the tool converts to GB/month automatically."
    )

    tco_region_label = st.selectbox(
        "Reference Region", list(REGION_LABELS.values()), index=0, key="tco_region",
    )
    tco_region = next(k for k, v in REGION_LABELS.items() if v == tco_region_label)

    # Default preset scenarios (utilisation-based)
    default_scenarios = [
        {"label": "SMB",            "port_speed_gbps": 1.0,  "util_pct": 10},
        {"label": "Mid-Market",     "port_speed_gbps": 10.0, "util_pct": 20},
        {"label": "Enterprise",     "port_speed_gbps": 10.0, "util_pct": 50},
        {"label": "Large Enterprise","port_speed_gbps": 100.0, "util_pct": 30},
    ]

    # ── Custom scenario builder ───────────────────────────────────────────────
    with st.expander("➕ Add / edit custom scenario"):
        c1, c2, c3, c4 = st.columns(4)
        cust_label = c1.text_input("Label", "Custom", key="tco_clabel")
        cust_speed = c2.selectbox(
            "Port Speed (Gbps)", [0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 100.0],
            index=6, key="tco_cspeed",
        )
        cust_util = c3.slider("Utilisation %", 1, 100, 30, 5, key="tco_cutil")
        cust_gb = comp.util_to_gb_month(float(cust_speed), cust_util)
        c4.metric(
            "Monthly transfer",
            f"{cust_gb/1024:.1f} TB" if cust_gb >= 1024 else f"{cust_gb:,.0f} GB",
            help=f"speed × util% × 730h × 3600s ÷ 8",
        )

        if st.button("Add to scenarios", key="tco_add"):
            default_scenarios = default_scenarios + [{
                "label": cust_label,
                "port_speed_gbps": float(cust_speed),
                "util_pct": float(cust_util),
            }]

    # Display current scenario inputs with live GB preview
    st.markdown("**Active scenarios** *(utilisation → GB/month shown)*")
    prev_cols = st.columns(len(default_scenarios))
    for i, sc in enumerate(default_scenarios):
        gb = comp.util_to_gb_month(sc["port_speed_gbps"], sc["util_pct"])
        tb = gb / 1024.0
        prev_cols[i].markdown(
            f"**{sc['label']}**  \n"
            f"{sc['port_speed_gbps']:.0f} Gbps · {sc['util_pct']}% util  \n"
            f"≈ {tb:.1f} TB/mo"
        )

    st.markdown("---")

    tco_df = comp.tco_scenarios(default_scenarios, region=tco_region)

    if tco_df.empty:
        st.info("Insufficient data for TCO scenarios.")
        return

    # ── Chart — show GCP, AWS, Azure metered per-circuit, Azure unlimited per-circuit ──
    fig = go.Figure()
    chart_series = [
        ("GCP",                     GCP_COLOR, "gcp_total"),
        ("AWS",                     AWS_COLOR, "aws_total"),
        ("Azure (metered/circuit)", AZ_COLOR,  "azure_metered_per_circuit"),
        ("Azure (unlimited/circuit)", "#0078D4", "azure_unlimited_per_circuit"),
    ]
    for name, color, col in chart_series:
        if col in tco_df.columns and tco_df[col].notna().any():
            fig.add_trace(go.Bar(
                name=name, x=tco_df["scenario"], y=tco_df[col],
                marker_color=color, marker_line_width=0,
                text=tco_df[col].apply(lambda v: f"${v:,.0f}" if pd.notna(v) else ""),
                textposition="outside",
                hovertemplate=f"<b>{name}</b><br>$%{{y:,.0f}}/mo<extra></extra>",
            ))

    fig.update_layout(
        title="Monthly TCO by Scenario (Azure = per-circuit normalised)",
        barmode="group", plot_bgcolor="white", paper_bgcolor="white",
        font_family="Google Sans, Segoe UI, sans-serif",
        title_font_size=13, legend=dict(orientation="h", y=-0.25),
        yaxis=dict(title="$/month", gridcolor="#F1F3F4"),
        xaxis=dict(gridcolor="rgba(0,0,0,0)"),
        margin=dict(t=50, b=100),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Detailed table ─────────────────────────────────────────────────────────
    st.markdown("**Detailed TCO Breakdown**")
    st.caption(
        f"Azure list price ÷ {AZURE_CIRCUITS_PER_PURCHASE} = per-circuit price "
        f"(Azure includes {AZURE_CIRCUITS_PER_PURCHASE} circuits per purchase)."
    )

    display = tco_df[[
        "scenario", "speed_gbps", "util_pct", "data_tb",
        "gcp_total", "aws_total",
        "azure_metered_total", "azure_metered_per_circuit",
        "azure_unlimited_total", "azure_unlimited_per_circuit",
        "cheapest",
    ]].copy()
    display.columns = [
        "Scenario", "Speed (Gbps)", "Util %", "Data (TB/mo)",
        "GCP", "AWS",
        "Azure Metered (list)", "Azure Metered (per-ckt)",
        "Azure Unlimited (list)", "Azure Unlimited (per-ckt)",
        "Cheapest",
    ]

    def _fmt_dollar_tco(v):
        return f"${v:,.0f}" if pd.notna(v) and v is not None else "—"

    def _fmt_util(v):
        return f"{v:.0f}%" if pd.notna(v) else "—"

    def _highlight_cheapest(row):
        styles = [""] * len(row)
        cheapest_val = row.get("Cheapest", "")
        col_map = {
            "GCP": "GCP",
            "AWS": "AWS",
            "Azure (per-circuit)": "Azure Metered (per-ckt)",
        }
        target = col_map.get(cheapest_val)
        if target and target in row.index:
            idx = list(row.index).index(target)
            styles[idx] = "background:#E6F4EA; font-weight:bold"
        return styles

    st.dataframe(
        display.style
        .format({
            "GCP": _fmt_dollar_tco,
            "AWS": _fmt_dollar_tco,
            "Azure Metered (list)": _fmt_dollar_tco,
            "Azure Metered (per-ckt)": _fmt_dollar_tco,
            "Azure Unlimited (list)": _fmt_dollar_tco,
            "Azure Unlimited (per-ckt)": _fmt_dollar_tco,
            "Speed (Gbps)": "{:.0f}",
            "Util %": _fmt_util,
            "Data (TB/mo)": "{:.1f}",
        })
        .apply(_highlight_cheapest, axis=1),
        use_container_width=True, hide_index=True,
    )

    with st.expander("ℹ️ TCO methodology"):
        st.markdown(f"""
**Data volume formula:**
```
monthly_GB = speed_Gbps × (util% ÷ 100) × 730 hours × 3600 sec/hr ÷ 8 bits/byte
           = speed_Gbps × util% × 328,500 GB per Gbps
```

**Azure normalisation:**
- Azure ExpressRoute includes **{AZURE_CIRCUITS_PER_PURCHASE} circuits** (primary + secondary) per port purchase.
- GCP and AWS include **1 circuit** per purchase.
- "Azure per-circuit" = list price ÷ {AZURE_CIRCUITS_PER_PURCHASE} for like-for-like comparison.

**Azure Metered vs Unlimited:**
- **Metered**: Lower port fee, per-GB egress charges apply.
- **Unlimited**: Higher port fee, no per-GB charges — cost-effective at high utilisation.
        """)


def tab_regional(comp: PriceComparator):
    st.subheader("Regional Price Breakdown")
    st.caption(
        "All regions and port speeds. "
        "Azure prices shown both as list (per-purchase) and per-circuit (÷ 2 for like-for-like)."
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    fc1, fc2 = st.columns(2)
    with fc1:
        all_regions = list(REGION_LABELS.values())
        sel_region_labels = st.multiselect(
            "Regions", all_regions, default=all_regions,
            key="reg_regions",
        )
        sel_regions = [k for k, v in REGION_LABELS.items() if v in sel_region_labels]
    with fc2:
        speed_options = [0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 100.0, 400.0]
        sel_speeds = st.multiselect(
            "Port Speeds (Gbps)", speed_options, default=[1.0, 10.0, 100.0],
            key="reg_speeds",
            format_func=lambda x: f"{x*1000:.0f} Mbps" if x < 1 else f"{x:.0f} Gbps",
        )

    if not sel_regions or not sel_speeds:
        st.info("Select at least one region and one speed to display data.")
        return

    rb_df = comp.regional_breakdown(speeds_gbps=sel_speeds, regions=sel_regions)

    if rb_df.empty:
        st.info("No data available for selected filters.")
        return

    # ── Summary chart — pick first selected speed for bar chart ───────────────
    chart_speed = sel_speeds[0] if len(sel_speeds) == 1 else None
    if chart_speed is None and 10.0 in sel_speeds:
        chart_speed = 10.0
    elif chart_speed is None:
        chart_speed = sel_speeds[0]

    df_chart = rb_df[rb_df["port_speed_gbps"] == chart_speed].copy()
    if not df_chart.empty:
        fig = go.Figure()
        bar_series = [
            ("GCP",                     GCP_COLOR, "gcp_monthly"),
            ("AWS",                     AWS_COLOR, "aws_monthly"),
            ("Azure Metered (per-ckt)", AZ_COLOR,  "azure_metered_per_circuit"),
            ("Azure Unlimited (per-ckt)", "#0078D4", "azure_unlimited_per_circuit"),
        ]
        for name, color, col in bar_series:
            if col in df_chart.columns and df_chart[col].notna().any():
                fig.add_trace(go.Bar(
                    name=name, x=df_chart["region_label"], y=df_chart[col],
                    marker_color=color, marker_line_width=0,
                    hovertemplate=f"<b>{name}</b><br>${{y:,.0f}}/mo<extra></extra>",
                ))
        spd_label = f"{chart_speed:.0f} Gbps" if chart_speed >= 1 else f"{chart_speed*1000:.0f} Mbps"
        fig.update_layout(
            title=f"Monthly Port Fee by Region — {spd_label} (Azure metered = per-circuit; Unlimited = Standard tier, per-circuit)",
            barmode="group", plot_bgcolor="white", paper_bgcolor="white",
            font_family="Google Sans, Segoe UI, sans-serif",
            title_font_size=13, legend=dict(orientation="h", y=-0.25),
            yaxis=dict(title="$/month", gridcolor="#F1F3F4"),
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            margin=dict(t=50, b=100),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Full table ─────────────────────────────────────────────────────────────
    st.markdown("**Full Price Table**")

    def _speed_fmt(v):
        if v < 1:
            return f"{v*1000:.0f} Mbps"
        return f"{v:.0f} Gbps"

    display = rb_df[[
        "region_label", "port_speed_gbps",
        "gcp_monthly", "aws_monthly",
        "azure_metered", "azure_metered_per_circuit",
        "azure_unlimited", "azure_unlimited_per_circuit",
        "gcp_vs_aws_pct", "gcp_vs_azure_pct",
    ]].copy()
    display.columns = [
        "Region", "Speed",
        "GCP ($/mo)", "AWS ($/mo)",
        "Azure Metered (list)", "Azure Metered (per-ckt)",
        "Azure Unlimited (list)", "Azure Unlimited (per-ckt)",
        "GCP vs AWS", "GCP vs Azure (per-ckt)",
    ]

    def _highlight_pct(val):
        if pd.isna(val):
            return ""
        if val < -5:
            return "color: #1E8E3E; font-weight: bold"
        if val < 0:
            return "color: #34A853"
        if val < 5:
            return "color: #F9AB00"
        return "color: #D93025; font-weight: bold"

    st.dataframe(
        display.style
        .format({
            "Speed": _speed_fmt,
            "GCP ($/mo)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "AWS ($/mo)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "Azure Metered (list)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "Azure Metered (per-ckt)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "Azure Unlimited (list)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "Azure Unlimited (per-ckt)": lambda v: f"${v:,.0f}" if pd.notna(v) else "—",
            "GCP vs AWS": lambda v: f"{v:+.1f}%" if pd.notna(v) else "—",
            "GCP vs Azure (per-ckt)": lambda v: f"{v:+.1f}%" if pd.notna(v) else "—",
        })
        .map(_highlight_pct, subset=["GCP vs AWS", "GCP vs Azure (per-ckt)"]),
        use_container_width=True, hide_index=True,
        height=min(600, 40 + len(display) * 36),
    )

    with st.expander("ℹ️ Table notes"):
        st.markdown(f"""
- **Azure Metered (per-ckt)** = Azure metered list price ÷ {AZURE_CIRCUITS_PER_PURCHASE}.
  Azure includes primary + secondary circuit per purchase; GCP/AWS include 1 circuit.
- **Negative %** = GCP is cheaper than the competitor (green = good for GCP).
- **Positive %** = GCP is more expensive.
- AWS pricing = hourly port fee × 730 h/month.
- Azure "—" = that speed/model not offered in that region.
        """)


def tab_changes(store: PriceStore):
    st.subheader("Price Change History")

    changes = store.get_recent_changes(100)
    if not changes:
        st.info("No price changes detected yet. Run multiple fetches to see history.")
        return

    df = pd.DataFrame(changes)
    df["detected_at"] = pd.to_datetime(df["detected_at"]).dt.strftime("%Y-%m-%d %H:%M")

    # Summary metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Changes", len(df))
    c2.metric("Price Changes", len(df[df["change_type"] == "price_change"]))
    c3.metric("New SKUs", len(df[df["change_type"] == "new_sku"]))

    # Filter
    change_types = df["change_type"].unique().tolist()
    selected_types = st.multiselect("Filter by type", change_types, default=change_types)
    providers = df["provider"].unique().tolist()
    selected_providers = st.multiselect("Filter by provider", providers, default=providers)

    mask = df["change_type"].isin(selected_types) & df["provider"].isin(selected_providers)
    display = df[mask][[
        "detected_at", "provider", "change_type", "sku_name",
        "region_raw", "old_price_monthly", "new_price_monthly", "pct_change_monthly",
    ]].copy()
    display.columns = [
        "Detected", "Provider", "Type", "SKU",
        "Region", "Old Price", "New Price", "% Change",
    ]

    st.dataframe(
        display.style.format({
            "Old Price": lambda v: f"${v:,.2f}" if pd.notna(v) else "—",
            "New Price": lambda v: f"${v:,.2f}" if pd.notna(v) else "—",
            "% Change": lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
        }),
        use_container_width=True, hide_index=True,
    )


def tab_coverage(comp: PriceComparator):
    st.subheader("Regional Coverage")
    cov_df = comp.regional_coverage()
    if cov_df.empty:
        st.info("No coverage data available.")
        return

    providers = [c for c in ["gcp", "aws", "azure"] if c in cov_df.columns]
    heatmap_data = cov_df.set_index("Region")[providers].copy()

    fig = px.imshow(
        heatmap_data,
        color_continuous_scale=[[0, "#F8F9FA"], [0.01, "#D2E3FC"], [1, "#1A73E8"]],
        labels=dict(x="Provider", y="Region", color="SKUs"),
        title="SKU Coverage by Provider and Region",
        text_auto=True,
    )
    fig.update_layout(
        xaxis=dict(tickvals=list(range(len(providers))),
                   ticktext=[p.upper() for p in providers]),
        coloraxis_showscale=False,
        font_family="Google Sans, Segoe UI, sans-serif",
        margin=dict(t=50, l=120),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(heatmap_data, use_container_width=True)

    # Show scope and model type notes from attrs
    scope_notes = cov_df.attrs.get("scope_notes", {})
    model_notes = cov_df.attrs.get("model_notes", {})

    if scope_notes or model_notes:
        st.markdown("---")
        st.markdown("**Pricing Scope & Model Summary**")
        note_rows = []
        for p in ["gcp", "aws", "azure"]:
            meta = PROVIDER_META.get(p, {})
            note_rows.append({
                "Provider": meta.get("label", p.upper()),
                "Pricing Scope": scope_notes.get(p, "—"),
                "Billing Model": model_notes.get(p, "—"),
            })
        st.dataframe(pd.DataFrame(note_rows), use_container_width=True, hide_index=True)

        with st.expander("About SKU counts"):
            st.markdown("""
The SKU count above reflects the total number of pricing records fetched from each provider's API.
High SKU counts (e.g. Azure ~7,000+) reflect zone × speed × model combinations, not unique locations.

| Provider | Scope | Why many SKUs? |
|----------|-------|----------------|
| GCP | Global flat rate — 1 global price covers all regions | Few SKUs; same price everywhere |
| AWS | Regional — priced per Direct Connect location | Medium SKUs; 6 regions × speeds |
| Azure | Zone-based — Zone 1/2/3/4 circuit pricing + per-ARM-region gateways | High SKU count due to gateway + circuit × zone × model combinations |
            """)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    config = _load_config()
    store = get_store()

    # Handle triggered actions
    if st.session_state.get("trigger_fetch"):
        del st.session_state["trigger_fetch"]
        with st.spinner("Fetching prices from all providers…"):
            do_fetch(config, store)

    if "trigger_report" in st.session_state:
        run_id = st.session_state.pop("trigger_report")
        do_report(run_id, config, store)

    # Header
    st.title("☁️ GCP Networking Price Intelligence")
    st.markdown("*Cloud Interconnect vs AWS Direct Connect vs Azure ExpressRoute*")

    # Sidebar
    filters = render_sidebar(store)

    if not filters:
        st.info("👈 Click **Fetch Prices Now** in the sidebar to load pricing data.")
        st.markdown("""
### Getting started
1. (Optional) Add your GCP API key to `config/settings.yaml` for live GCP prices
2. Click **Fetch** in the sidebar — AWS and Azure pull live data, GCP uses reference pricing by default
3. Explore the tabs below
4. Click **Report** to generate a shareable HTML report

### What you'll see
| Tab | Contents |
|-----|----------|
| Overview | Executive KPIs + all-flavour comparison table (Dedicated, Partner, Cross-Cloud, Site-to-Site) |
| Port Fees | Head-to-head port/circuit fee comparison by region |
| TCO | All-in monthly cost with utilisation % input; Azure metered vs unlimited |
| Regional | Full regional price table with filters — all regions × speeds |
| Changes | Price change history and alerts |
| Coverage | Regional availability map |
        """)
        return

    run_id = filters["run_id"]
    comp = _get_comparator(store, run_id)

    # Info bar
    records = store.get_prices_for_run(run_id)
    providers = sorted({r["provider"] for r in records})
    st.caption(
        f"Data snapshot: `{run_id[:19]}`  ·  "
        f"{len(records)} SKUs  ·  "
        f"Providers: {', '.join(p.upper() for p in providers)}"
    )

    # Tabs
    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📊 Overview", "🔌 Port Fees",
        "💰 TCO Scenarios", "🌍 Regional", "🔔 Changes", "🗺️ Coverage",
    ])

    with t1:
        tab_overview(comp)
    with t2:
        tab_port_fees(comp, filters)
    with t3:
        tab_tco(comp, config)
    with t4:
        tab_regional(comp)
    with t5:
        tab_changes(store)
    with t6:
        tab_coverage(comp)


if __name__ == "__main__":
    main()
