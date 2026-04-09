"""
Executive-grade HTML report generator.

Produces a fully self-contained single-file HTML report.
Charts are powered by Chart.js (loaded from CDN).
No server-side rendering dependencies.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import pandas as pd

from analysis.compare import PriceComparator, PROVIDER_META, AZURE_CIRCUITS_PER_PURCHASE

OUTPUT_DIR = Path(__file__).parent.parent / "output"


def _fmt_dollar(val, decimals: int = 0) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    if decimals == 0:
        return f"${val:,.0f}"
    return f"${val:,.{decimals}f}"


def _fmt_pct(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


def _pct_color(val) -> str:
    """CSS colour: green if GCP is cheaper, red if more expensive."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "#666"
    if val < -5:
        return "#1E8E3E"   # GCP significantly cheaper
    if val < 0:
        return "#34A853"   # GCP slightly cheaper
    if val < 5:
        return "#F9AB00"   # roughly at parity
    return "#D93025"       # GCP more expensive


def _badge(val) -> str:
    """HTML badge for a % delta."""
    color = _pct_color(val)
    txt = _fmt_pct(val)
    bg = {"#1E8E3E": "#E6F4EA", "#34A853": "#E6F4EA",
          "#F9AB00": "#FEF7E0", "#D93025": "#FCE8E6", "#666": "#F1F3F4"}
    bg_color = bg.get(color, "#F1F3F4")
    return (
        f'<span style="background:{bg_color};color:{color};padding:2px 8px;'
        f'border-radius:12px;font-size:0.8rem;font-weight:600">{txt}</span>'
    )


class HTMLReportGenerator:
    def __init__(self, comparator: PriceComparator, run_id: str,
                 changes: Optional[list] = None, config: Optional[dict] = None):
        self.comp = comparator
        self.run_id = run_id
        self.changes = changes or []
        self.config = config or {}
        self._scenarios = self.config.get("analysis", {}).get("tco_scenarios", [
            {"label": "SMB",            "port_speed_gbps": 1,   "monthly_data_tb": 1},
            {"label": "Mid-Market",     "port_speed_gbps": 10,  "monthly_data_tb": 10},
            {"label": "Enterprise",     "port_speed_gbps": 10,  "monthly_data_tb": 50},
            {"label": "Large Enterprise","port_speed_gbps": 100, "monthly_data_tb": 200},
        ])

    def generate(self, output_path: Optional[str] = None) -> str:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if output_path is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(OUTPUT_DIR / f"networking_price_report_{ts}.html")

        html = self._render()
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        return output_path

    # ── Main renderer ─────────────────────────────────────────────────────────

    def _render(self) -> str:
        metrics = self.comp.headline_metrics()
        svc_df = self.comp.service_type_comparison(speed_gbps=10.0, region="us_east")
        port_df = self.comp.port_fee_comparison()
        dt_df = self.comp.data_transfer_comparison()
        tco_df = self.comp.tco_scenarios(self._scenarios)
        cov_df = self.comp.regional_coverage()

        report_date = datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GCP Networking Price Intelligence | {datetime.now().strftime("%b %Y")}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --gcp: #4285F4; --aws: #FF9900; --azure: #008AD7;
    --gcp-light: #D2E3FC; --aws-light: #FFF0D0; --azure-light: #CCE8F4;
    --green: #34A853; --red: #D93025; --yellow: #F9AB00;
    --bg: #F8F9FA; --card: #FFFFFF; --border: #E8EAED;
    --text: #202124; --sub: #5F6368;
    --font: 'Google Sans', 'Segoe UI', system-ui, sans-serif;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: var(--font); background: var(--bg); color: var(--text); font-size: 14px; }}

  /* Layout */
  .page {{ max-width: 1200px; margin: 0 auto; padding: 0 24px 48px; }}

  /* Header */
  .header {{ background: #1A73E8; color: white; padding: 32px 24px 28px; }}
  .header-inner {{ max-width: 1200px; margin: 0 auto; display: flex; justify-content: space-between; align-items: flex-end; }}
  .header h1 {{ font-size: 1.8rem; font-weight: 400; letter-spacing: -.5px; }}
  .header .subtitle {{ opacity: .85; margin-top: 4px; font-size: .9rem; }}
  .header .meta {{ text-align: right; font-size: .8rem; opacity: .8; }}

  /* Section */
  .section {{ margin-top: 32px; }}
  .section-title {{
    font-size: 1rem; font-weight: 600; color: var(--sub);
    text-transform: uppercase; letter-spacing: .8px;
    border-bottom: 2px solid var(--border); padding-bottom: 8px; margin-bottom: 20px;
  }}

  /* Cards */
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }}
  .card {{
    background: var(--card); border: 1px solid var(--border); border-radius: 12px;
    padding: 20px 24px; box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }}
  .card-label {{ font-size: .75rem; font-weight: 600; color: var(--sub); text-transform: uppercase; letter-spacing: .5px; }}
  .card-value {{ font-size: 1.75rem; font-weight: 500; margin-top: 4px; }}
  .card-sub {{ font-size: .8rem; color: var(--sub); margin-top: 4px; }}

  /* Position banner */
  .position-banner {{
    border-radius: 12px; padding: 20px 28px;
    display: flex; align-items: center; gap: 16px; margin-bottom: 8px;
    border: 1px solid var(--border);
  }}
  .position-icon {{ font-size: 2rem; }}
  .position-text h2 {{ font-size: 1.2rem; font-weight: 500; }}
  .position-text p {{ font-size: .85rem; color: var(--sub); margin-top: 4px; }}

  /* Provider chips */
  .provider-chip {{
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: .75rem; font-weight: 700; margin: 0 2px;
  }}
  .chip-gcp   {{ background: var(--gcp-light); color: var(--gcp); }}
  .chip-aws   {{ background: var(--aws-light); color: #7A4800; }}
  .chip-azure {{ background: var(--azure-light); color: #005C8A; }}

  /* Tables */
  .tbl-wrap {{ overflow-x: auto; border-radius: 10px; border: 1px solid var(--border); }}
  table {{ width: 100%; border-collapse: collapse; background: white; }}
  thead tr {{ background: #F8F9FA; }}
  th {{ padding: 11px 14px; text-align: left; font-size: .8rem; font-weight: 600;
        color: var(--sub); text-transform: uppercase; letter-spacing: .4px; white-space: nowrap; }}
  td {{ padding: 10px 14px; border-top: 1px solid var(--border); font-size: .88rem; vertical-align: middle; }}
  tr:hover td {{ background: #F8F9FA; }}
  .num {{ font-variant-numeric: tabular-nums; }}
  .cheapest {{ font-weight: 700; }}

  /* Chart containers */
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 700px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
  .chart-card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }}
  .chart-card h3 {{ font-size: .85rem; font-weight: 600; color: var(--sub);
                    text-transform: uppercase; letter-spacing: .5px; margin-bottom: 16px; }}
  canvas {{ max-height: 280px; }}

  /* Change log */
  .change-item {{
    border-left: 3px solid var(--border); padding: 10px 16px; margin-bottom: 8px;
    border-radius: 0 8px 8px 0; background: white; font-size: .85rem;
  }}
  .change-new    {{ border-color: var(--green); }}
  .change-price  {{ border-color: var(--yellow); }}
  .change-removed {{ border-color: var(--red); }}

  /* Coverage table */
  .cov-yes {{ color: var(--green); font-weight: 700; text-align: center; }}
  .cov-no  {{ color: #CCC; text-align: center; }}

  /* Footer */
  .footer {{ margin-top: 48px; padding-top: 24px; border-top: 1px solid var(--border);
             font-size: .78rem; color: var(--sub); display: flex; justify-content: space-between; }}

  /* Print */
  @media print {{
    .header {{ background: #1A73E8 !important; -webkit-print-color-adjust: exact; }}
    .page {{ padding: 0 12px 24px; max-width: 100%; }}
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-inner">
    <div>
      <div class="subtitle">GCP Networking · Pricing Intelligence</div>
      <h1>Cloud Interconnect vs Direct Connect vs ExpressRoute</h1>
    </div>
    <div class="meta">
      Generated: {report_date}<br>
      Run ID: <code style="font-size:.75rem">{self.run_id[:19]}</code><br>
      <span style="opacity:.7">CONFIDENTIAL — INTERNAL USE ONLY</span>
    </div>
  </div>
</div>

<div class="page">

  {self._section_executive_summary(metrics)}
  {self._section_service_types(svc_df)}
  {self._section_port_fees(port_df)}
  {self._section_data_transfer(dt_df)}
  {self._section_tco(tco_df)}
  {self._section_coverage(cov_df)}
  {self._section_changes()}
  {self._section_methodology()}

  <div class="footer">
    <span>Sources: GCP Cloud Billing API · AWS Price List API · Azure Retail Prices API</span>
    <span>GCP Networking Pricing &amp; Packaging · {datetime.now().year}</span>
  </div>
</div>

{self._chart_scripts(port_df, dt_df)}

</body>
</html>"""

    # ── Sections ──────────────────────────────────────────────────────────────

    def _section_executive_summary(self, m: dict) -> str:
        pos_color = m["position_color"]
        pos = m["competitive_position"]

        # Choose icon
        icons = {
            "Significantly Cheaper": "🟢", "Slightly Cheaper": "🟡",
            "Roughly at Parity": "🟡", "More Expensive": "🔴",
        }
        icon = icons.get(pos, "ℹ️")

        # KPI cards
        kpi_rows = [
            ("GCP Interconnect (10G, US)", m["gcp_10g_us"], "Dedicated port, monthly"),
            ("AWS Direct Connect (10G, US)", m["aws_10g_us"], "Dedicated port, monthly"),
            ("Azure ExpressRoute (10G, US)", m["azure_10g_us"], "Circuit fee, monthly"),
        ]
        cards_html = ""
        colors = ["var(--gcp)", "var(--aws)", "var(--azure)"]
        for (label, val, sub), color in zip(kpi_rows, colors):
            cards_html += f"""
      <div class="card">
        <div class="card-label">{label}</div>
        <div class="card-value" style="color:{color}">{val}</div>
        <div class="card-sub">{sub}</div>
      </div>"""

        pct_vs_aws = m["gcp_vs_aws_raw"]
        pct_vs_az = m["gcp_vs_azure_raw"]
        vs_aws_str = _fmt_pct(pct_vs_aws)
        vs_az_str = _fmt_pct(pct_vs_az)
        aws_c = _pct_color(pct_vs_aws)
        az_c = _pct_color(pct_vs_az)

        banner_bg = {
            "Significantly Cheaper": "#E6F4EA", "Slightly Cheaper": "#E6F4EA",
            "Roughly at Parity": "#FEF7E0", "More Expensive": "#FCE8E6",
        }.get(pos, "#F1F3F4")

        return f"""
  <div class="section">
    <div class="section-title">Executive Summary</div>
    <div class="position-banner" style="background:{banner_bg}">
      <div class="position-icon">{icon}</div>
      <div class="position-text">
        <h2 style="color:{pos_color}">GCP is <strong>{pos}</strong> vs. Cloud Competitors (10G, US East)</h2>
        <p>
          vs. AWS Direct Connect: <strong style="color:{aws_c}">{vs_aws_str}</strong>
          &nbsp;&nbsp;|&nbsp;&nbsp;
          vs. Azure ExpressRoute: <strong style="color:{az_c}">{vs_az_str}</strong>
        </p>
      </div>
    </div>
    <div class="cards" style="margin-top:16px">
      {cards_html}
      <div class="card">
        <div class="card-label">GCP Data Transfer Out</div>
        <div class="card-value" style="color:var(--gcp);font-size:1.3rem">{m["gcp_data_gb"]}</div>
        <div class="card-sub">via Cloud Interconnect</div>
      </div>
      <div class="card">
        <div class="card-label">AWS Data Transfer Out</div>
        <div class="card-value" style="color:var(--aws);font-size:1.3rem">{m["aws_data_gb"]}</div>
        <div class="card-sub">via Direct Connect</div>
      </div>
      <div class="card">
        <div class="card-label">Azure Data Transfer Out</div>
        <div class="card-value" style="color:var(--azure);font-size:1.3rem">{m["azure_data_gb"]}</div>
        <div class="card-sub">via ExpressRoute Metered</div>
      </div>
    </div>
  </div>"""

    def _section_service_types(self, svc_df: pd.DataFrame) -> str:
        if svc_df.empty:
            return ""

        region_label = svc_df.attrs.get("region_label", "US East")
        speed_gbps = svc_df.attrs.get("speed_gbps", 10.0)
        speed_str = f"{int(speed_gbps)} Gbps" if speed_gbps >= 1 else f"{speed_gbps*1000:.0f} Mbps"

        def _avail_cell(product, price, note=""):
            if product in ("Not available", "No equivalent"):
                return f'<td style="color:#9AA0A6;font-style:italic">{product}</td><td style="color:#9AA0A6">—</td>'
            price_html = f'<strong>{price}</strong>' if price not in ("—", "See GCP pricing", "Add-on") else f'<span style="color:#9AA0A6">{price}</span>'
            note_html = f'<br><span style="font-size:.75rem;color:var(--sub)">{note}</span>' if note else ""
            return f"<td>{product}{note_html}</td><td class='num'>{price_html}</td>"

        rows_html = ""
        for _, row in svc_df.iterrows():
            tier = row["Service Tier"]
            gcp_product = row["GCP Product"]
            gcp_price = row["GCP Price"]
            aws_product = row["AWS Product"]
            aws_price = row["AWS Price"]
            azure_product = row["Azure Product"]
            azure_list = row["Azure List Price"]
            azure_per_ckt = row["Azure Per-Circuit"]
            notes = row.get("Notes", "")

            gcp_td = _avail_cell(gcp_product, gcp_price)
            aws_td = _avail_cell(aws_product, aws_price)

            if azure_product in ("No equivalent",):
                azure_td = f'<td style="color:#9AA0A6;font-style:italic">{azure_product}</td><td style="color:#9AA0A6">—</td><td style="color:#9AA0A6">—</td>'
            else:
                note_html = f'<br><span style="font-size:.75rem;color:var(--sub)">{notes}</span>' if notes else ""
                azure_td = (
                    f"<td>{azure_product}{note_html}</td>"
                    f'<td class="num"><strong>{azure_list}</strong></td>'
                    f'<td class="num" style="color:var(--sub)">{azure_per_ckt}</td>'
                )

            rows_html += f"""<tr>
          <td><strong>{tier}</strong></td>
          {gcp_td}
          {aws_td}
          {azure_td}
        </tr>"""

        return f"""
  <div class="section">
    <div class="section-title">Interconnect Flavour Comparison — {speed_str}, {region_label}</div>
    <p style="font-size:.82rem;color:var(--sub);margin-bottom:16px">
      All four interconnect types compared across GCP, AWS, and Azure.
      Azure prices shown at list (per port purchase) and per-circuit
      (÷ {AZURE_CIRCUITS_PER_PURCHASE} — Azure includes primary + secondary circuit per purchase).
    </p>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Service Tier</th>
          <th><span class="provider-chip chip-gcp">GCP</span> Product</th>
          <th><span class="provider-chip chip-gcp">GCP</span> Price</th>
          <th><span class="provider-chip chip-aws">AWS</span> Product</th>
          <th><span class="provider-chip chip-aws">AWS</span> Price</th>
          <th><span class="provider-chip chip-azure">Azure</span> Product</th>
          <th><span class="provider-chip chip-azure">Azure</span> List Price</th>
          <th><span class="provider-chip chip-azure">Azure</span> Per-Circuit</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    <p style="font-size:.75rem;color:var(--sub);margin-top:8px">
      † Azure Per-Circuit = list price ÷ {AZURE_CIRCUITS_PER_PURCHASE}.
      GCP and AWS include 1 circuit per purchase; redundancy requires buying two ports.
      Azure Metered plans add per-GB egress; Unlimited plans include all egress.
    </p>
  </div>"""

    def _section_port_fees(self, port_df: pd.DataFrame) -> str:
        if port_df.empty:
            return '<div class="section"><div class="section-title">Port / Circuit Fees</div><p>No data available.</p></div>'

        rows_html = ""
        for _, row in port_df.iterrows():
            speed = row.get("port_speed_gbps", 0)
            speed_str = f"{int(speed) if speed == int(speed) else speed} Gbps"
            gcp = _fmt_dollar(row.get("gcp_monthly"))
            aws = _fmt_dollar(row.get("aws_monthly"))
            az = _fmt_dollar(row.get("azure_monthly"))
            b_aws = _badge(row.get("gcp_vs_aws_pct"))
            b_az = _badge(row.get("gcp_vs_azure_pct"))

            # Bold cheapest
            vals = {k: v for k, v in {
                "gcp": row.get("gcp_monthly"), "aws": row.get("aws_monthly"),
                "azure": row.get("azure_monthly"),
            }.items() if v is not None and not pd.isna(v)}
            cheapest_p = min(vals, key=vals.get) if vals else None

            def _cell(p, val_str):
                cls = ' class="cheapest"' if p == cheapest_p else ''
                c = PROVIDER_META[p]["color"] if p == cheapest_p else ''
                color = f' style="color:{c}"' if c else ''
                return f'<td class="num"{cls}{color}>{val_str}</td>'

            rows_html += f"""<tr>
          <td>{row.get("region_label","—")}</td>
          <td><strong>{speed_str}</strong></td>
          {_cell("gcp", gcp)}
          {_cell("aws", aws)}
          {_cell("azure", az)}
          <td>{b_aws}</td>
          <td>{b_az}</td>
        </tr>"""

        return f"""
  <div class="section">
    <div class="section-title">Port & Circuit Fee Comparison ($/month) — Dedicated Connectivity</div>
    <div class="chart-grid" style="margin-bottom:20px">
      <div class="chart-card">
        <h3>10 Gbps Port Fee by Region</h3>
        <canvas id="chartPort10G"></canvas>
      </div>
      <div class="chart-card">
        <h3>Port Fee by Speed (US East)</h3>
        <canvas id="chartPortSpeed"></canvas>
      </div>
    </div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Region</th><th>Speed</th>
          <th><span class="provider-chip chip-gcp">GCP</span> Interconnect</th>
          <th><span class="provider-chip chip-aws">AWS</span> Direct Connect</th>
          <th><span class="provider-chip chip-azure">Azure</span> ExpressRoute</th>
          <th>GCP vs AWS</th>
          <th>GCP vs Azure</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    <p style="font-size:.75rem;color:var(--sub);margin-top:8px">
      * AWS: per-hour fee × 730 h/month. Azure: metered circuit monthly fee. GCP: dedicated port monthly fee.
      Bold = cheapest option. Percentage = GCP price relative to competitor (negative = GCP is cheaper).
    </p>
  </div>"""

    def _section_data_transfer(self, dt_df: pd.DataFrame) -> str:
        if dt_df.empty:
            return ""

        rows_html = ""
        for _, row in dt_df.iterrows():
            gcp = f"${row.get('gcp_per_gb',0):.4f}" if row.get("gcp_per_gb") else "—"
            aws = f"${row.get('aws_per_gb',0):.4f}" if row.get("aws_per_gb") else "—"
            az = f"${row.get('azure_per_gb',0):.4f}" if row.get("azure_per_gb") else "—"
            b_aws = _badge(row.get("gcp_vs_aws_pct"))
            b_az = _badge(row.get("gcp_vs_azure_pct"))
            rows_html += f"""<tr>
          <td>{row.get("region_label","—")}</td>
          <td class="num">{gcp}</td><td class="num">{aws}</td><td class="num">{az}</td>
          <td>{b_aws}</td><td>{b_az}</td>
        </tr>"""

        return f"""
  <div class="section">
    <div class="section-title">Data Transfer Out ($/GB)</div>
    <div class="chart-grid" style="margin-bottom:20px">
      <div class="chart-card">
        <h3>Data Transfer Out by Region</h3>
        <canvas id="chartDT"></canvas>
      </div>
      <div class="chart-card" style="display:flex;align-items:center;justify-content:center">
        <div style="text-align:center;padding:24px">
          <div style="font-size:.8rem;color:var(--sub);text-transform:uppercase;margin-bottom:12px">Note on Data Transfer</div>
          <p style="font-size:.85rem;color:#444;line-height:1.6">
            AWS charges DTO separately from port fees.<br>
            Azure metered plans include a data allowance; overage applies.<br>
            GCP charges egress separately from the port fee.<br><br>
            <strong>See TCO section for all-in cost scenarios.</strong>
          </p>
        </div>
      </div>
    </div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Region</th>
          <th><span class="provider-chip chip-gcp">GCP</span> Interconnect</th>
          <th><span class="provider-chip chip-aws">AWS</span> Direct Connect</th>
          <th><span class="provider-chip chip-azure">Azure</span> ExpressRoute</th>
          <th>GCP vs AWS</th><th>GCP vs Azure</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>"""

    def _section_tco(self, tco_df: pd.DataFrame) -> str:
        if tco_df.empty:
            return ""

        rows_html = ""
        for _, row in tco_df.iterrows():
            g  = _fmt_dollar(row.get("gcp_total"))
            a  = _fmt_dollar(row.get("aws_total"))
            zm = _fmt_dollar(row.get("azure_metered_per_circuit"))
            zu = _fmt_dollar(row.get("azure_unlimited_per_circuit"))
            cheapest = row.get("cheapest", "")
            sav_aws = row.get("gcp_savings_vs_aws")
            sav_az  = row.get("gcp_savings_vs_azure_metered")
            util    = row.get("util_pct", 0)
            data_tb = row.get("data_tb", 0)
            speed   = row.get("speed_gbps", 0)
            spd_str = f"{int(speed)} Gbps" if speed >= 1 else f"{speed*1000:.0f} Mbps"

            def _sav(v):
                if v is None or pd.isna(v):
                    return "—"
                # Positive = Azure/AWS is more expensive (GCP saves money)
                if v > 0:
                    return f'<span style="color:var(--red)">GCP costs ${v:,.0f}/mo more</span>'
                elif v < 0:
                    return f'<span style="color:var(--green)">GCP saves ${abs(v):,.0f}/mo</span>'
                return "At parity"

            cheapest_color = "#1E8E3E" if cheapest == "GCP" else _pct_color(1)

            rows_html += f"""<tr>
          <td><strong>{row.get("scenario","")}</strong></td>
          <td>{spd_str} / {util:.0f}% util<br>
              <span style="font-size:.75rem;color:var(--sub)">{data_tb:.1f} TB/mo</span></td>
          <td class="num" style="color:var(--gcp)">{g}</td>
          <td class="num" style="color:var(--aws)">{a}</td>
          <td class="num" style="color:var(--azure)">{zm}</td>
          <td class="num" style="color:#0078D4">{zu}</td>
          <td><strong style="color:{cheapest_color}">{cheapest}</strong></td>
          <td style="font-size:.82rem">{_sav(sav_aws)}</td>
          <td style="font-size:.82rem">{_sav(sav_az)}</td>
        </tr>"""

        return f"""
  <div class="section">
    <div class="section-title">Total Cost of Ownership — All-In Monthly (Port + Data Transfer)</div>
    <p style="font-size:.82rem;color:var(--sub);margin-bottom:16px">
      Azure prices are per-circuit (list ÷ {AZURE_CIRCUITS_PER_PURCHASE}).
      Utilisation % is converted to TB/month using:
      <code>GB = speed × util% × 328,500</code>.
      Azure Metered = port + egress. Azure Unlimited = port only (no per-GB charge).
    </p>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Scenario</th><th>Config</th>
          <th><span class="provider-chip chip-gcp">GCP</span></th>
          <th><span class="provider-chip chip-aws">AWS</span></th>
          <th><span class="provider-chip chip-azure">Azure Metered†</span></th>
          <th><span class="provider-chip chip-azure">Azure Unlimited†</span></th>
          <th>Cheapest</th>
          <th>GCP vs AWS</th>
          <th>GCP vs Azure (met.)</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    <p style="font-size:.75rem;color:var(--sub);margin-top:8px">
      † Azure per-circuit prices shown (list ÷ {AZURE_CIRCUITS_PER_PURCHASE}).
      GCP and AWS are 1 circuit per purchase. Cheapest compares GCP, AWS, Azure per-circuit.
    </p>
  </div>"""

    def _section_coverage(self, cov_df: pd.DataFrame) -> str:
        if cov_df.empty:
            return ""

        providers_in_df = [c for c in ["gcp", "aws", "azure"] if c in cov_df.columns]
        header = "<th>Region</th>" + "".join(
            f'<th style="text-align:center"><span class="provider-chip chip-{p}">{p.upper()}</span></th>'
            for p in providers_in_df
        )
        rows_html = ""
        for _, row in cov_df.iterrows():
            cells = f"<td>{row['Region']}</td>"
            for p in providers_in_df:
                count = row.get(p, 0)
                if count > 0:
                    cells += f'<td class="cov-yes">✓ <span style="font-size:.7rem;color:var(--sub)">({count})</span></td>'
                else:
                    cells += '<td class="cov-no">—</td>'
            rows_html += f"<tr>{cells}</tr>"

        return f"""
  <div class="section">
    <div class="section-title">Regional Coverage (SKU Count)</div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>{header}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>"""

    def _section_changes(self) -> str:
        if not self.changes:
            return ""

        items_html = ""
        for ch in self.changes[:20]:  # Cap at 20 for report brevity
            ct = ch.get("change_type", "")
            css = {"new_sku": "change-new", "price_change": "change-price",
                   "removed_sku": "change-removed"}.get(ct, "")
            p = ch.get("provider", "").upper()
            sku = ch.get("sku_name", ch.get("sku_id", ""))[:80]
            region = ch.get("region_raw", "")

            if ct == "price_change":
                old_m = ch.get("old_price_monthly") or 0
                new_m = ch.get("new_price_monthly") or 0
                pct = ch.get("pct_change_monthly")
                detail = f"Monthly: {_fmt_dollar(old_m)} → {_fmt_dollar(new_m)} {_badge(pct)}"
            elif ct == "new_sku":
                detail = f"New SKU @ {_fmt_dollar(ch.get('new_price_monthly'))}/mo"
            else:
                detail = f"Removed (was {_fmt_dollar(ch.get('old_price_monthly'))}/mo)"

            items_html += f"""
      <div class="change-item {css}">
        <strong>[{p}]</strong> {sku} <span style="color:var(--sub)">({region})</span><br>
        <span style="font-size:.8rem">{detail}</span>
      </div>"""

        return f"""
  <div class="section">
    <div class="section-title">Detected Changes Since Last Run ({len(self.changes)} total)</div>
    {items_html}
    {'<p style="font-size:.8rem;color:var(--sub)">…and ' + str(len(self.changes)-20) + ' more. See notifications for full list.</p>' if len(self.changes) > 20 else ""}
  </div>"""

    def _section_methodology(self) -> str:
        return """
  <div class="section">
    <div class="section-title">Methodology & Data Sources</div>
    <div class="card" style="font-size:.83rem;line-height:1.8">
      <table style="border:none;font-size:inherit">
        <tr><td style="font-weight:600;padding:4px 16px 4px 0;border:none;white-space:nowrap">GCP Cloud Interconnect</td>
            <td style="border:none">Google Cloud Billing Catalog API
            (<code>cloudbilling.googleapis.com/v1/services/{id}/skus</code>).
            If no API key is configured, reference prices from
            <a href="https://cloud.google.com/vpc/network-pricing" target="_blank">cloud.google.com/vpc/network-pricing</a> are used.</td></tr>
        <tr><td style="font-weight:600;padding:4px 16px 4px 0;border:none;white-space:nowrap">AWS Direct Connect</td>
            <td style="border:none">AWS Price List API
            (<code>pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDirectConnect</code>).
            Port fees converted from hourly to monthly (× 730 h). Dedicated connections only for port-fee comparison.</td></tr>
        <tr><td style="font-weight:600;padding:4px 16px 4px 0;border:none;white-space:nowrap">Azure ExpressRoute</td>
            <td style="border:none">Azure Retail Prices API
            (<code>prices.azure.com/api/retail/prices</code>).
            Metered circuit fees used for port comparison; data transfer charged separately via Azure metered overage.</td></tr>
        <tr><td style="font-weight:600;padding:4px 16px 4px 0;border:none;white-space:nowrap">Comparability</td>
            <td style="border:none">All three services provide dedicated private network connectivity between on-premises and cloud.
            Azure metered is the closest comparator to GCP/AWS dedicated (both charge separately for data).
            Port fees normalised to USD/month. Data transfer in USD/GB egress.</td></tr>
      </table>
    </div>
  </div>"""

    # ── Chart data ────────────────────────────────────────────────────────────

    def _chart_scripts(self, port_df: pd.DataFrame, dt_df: pd.DataFrame) -> str:
        # Chart 1: 10G port fee by region
        if not port_df.empty:
            df10 = port_df[port_df["port_speed_gbps"] == 10.0]
            labels = df10["region_label"].tolist()
            gcp_vals = [round(v, 0) if pd.notna(v) else None for v in df10["gcp_monthly"].tolist()]
            aws_vals = [round(v, 0) if pd.notna(v) else None for v in df10["aws_monthly"].tolist()]
            az_vals = [round(v, 0) if pd.notna(v) else None for v in df10["azure_monthly"].tolist()]
        else:
            labels, gcp_vals, aws_vals, az_vals = [], [], [], []

        # Chart 2: port fee by speed (US East)
        if not port_df.empty:
            df_us = port_df[port_df["region_canonical"] == "us_east"].copy()
            df_us = df_us.sort_values("port_speed_gbps")
            speed_labels = [f"{int(s) if s == int(s) else s} Gbps" for s in df_us["port_speed_gbps"]]
            gcp_speed = [round(v, 0) if pd.notna(v) else None for v in df_us["gcp_monthly"]]
            aws_speed = [round(v, 0) if pd.notna(v) else None for v in df_us["aws_monthly"]]
            az_speed = [round(v, 0) if pd.notna(v) else None for v in df_us["azure_monthly"]]
        else:
            speed_labels, gcp_speed, aws_speed, az_speed = [], [], [], []

        # Chart 3: data transfer by region
        if not dt_df.empty:
            dt_labels = dt_df["region_label"].tolist()
            dt_gcp = [round(v, 4) if pd.notna(v) else None for v in dt_df["gcp_per_gb"]]
            dt_aws = [round(v, 4) if pd.notna(v) else None for v in dt_df["aws_per_gb"]]
            dt_az = [round(v, 4) if pd.notna(v) else None for v in dt_df["azure_per_gb"]]
        else:
            dt_labels, dt_gcp, dt_aws, dt_az = [], [], [], []

        common_opts = """
        plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } } },
        scales: { y: { beginAtZero: true, grid: { color: '#F1F3F4' },
                        ticks: { font: { size: 10 } } },
                  x: { grid: { display: false }, ticks: { font: { size: 10 } } } }"""

        return f"""<script>
const COLORS = {{
  gcp: '#4285F4', gcpA: 'rgba(66,133,244,.15)',
  aws: '#FF9900', awsA: 'rgba(255,153,0,.15)',
  az:  '#008AD7', azA:  'rgba(0,138,215,.15)',
}};
function barDS(label, data, color) {{
  return {{ label, data, backgroundColor: color + 'CC', borderColor: color, borderWidth: 1.5,
            borderRadius: 4 }};
}}

// Chart 1: 10G by region
(function() {{
  const el = document.getElementById('chartPort10G');
  if (!el || !{json.dumps(labels)}.length) return;
  new Chart(el, {{
    type: 'bar',
    data: {{
      labels: {json.dumps(labels)},
      datasets: [
        barDS('GCP Interconnect', {json.dumps(gcp_vals)}, COLORS.gcp),
        barDS('AWS Direct Connect', {json.dumps(aws_vals)}, COLORS.aws),
        barDS('Azure ExpressRoute', {json.dumps(az_vals)}, COLORS.az),
      ]
    }},
    options: {{ responsive: true, {common_opts},
      plugins: {{ ...{{legend: {{ position: 'bottom', labels: {{ boxWidth:12, font:{{size:11}} }} }} }},
        tooltip: {{ callbacks: {{ label: ctx => ' $' + ctx.raw?.toLocaleString() + '/mo' }} }} }} }}
  }});
}})();

// Chart 2: speed comparison
(function() {{
  const el = document.getElementById('chartPortSpeed');
  if (!el || !{json.dumps(speed_labels)}.length) return;
  new Chart(el, {{
    type: 'bar',
    data: {{
      labels: {json.dumps(speed_labels)},
      datasets: [
        barDS('GCP', {json.dumps(gcp_speed)}, COLORS.gcp),
        barDS('AWS', {json.dumps(aws_speed)}, COLORS.aws),
        barDS('Azure', {json.dumps(az_speed)}, COLORS.az),
      ]
    }},
    options: {{ responsive: true, {common_opts},
      plugins: {{ ...{{legend: {{ position: 'bottom', labels: {{ boxWidth:12, font:{{size:11}} }} }} }},
        tooltip: {{ callbacks: {{ label: ctx => ' $' + ctx.raw?.toLocaleString() + '/mo' }} }} }} }}
  }});
}})();

// Chart 3: data transfer
(function() {{
  const el = document.getElementById('chartDT');
  if (!el || !{json.dumps(dt_labels)}.length) return;
  new Chart(el, {{
    type: 'bar',
    data: {{
      labels: {json.dumps(dt_labels)},
      datasets: [
        barDS('GCP', {json.dumps(dt_gcp)}, COLORS.gcp),
        barDS('AWS', {json.dumps(dt_aws)}, COLORS.aws),
        barDS('Azure', {json.dumps(dt_az)}, COLORS.az),
      ]
    }},
    options: {{ responsive: true, {common_opts},
      plugins: {{ ...{{legend: {{ position: 'bottom', labels: {{ boxWidth:12, font:{{size:11}} }} }} }},
        tooltip: {{ callbacks: {{ label: ctx => ' $' + (ctx.raw || 0).toFixed(4) + '/GB' }} }} }} }}
  }});
}})();
</script>"""
