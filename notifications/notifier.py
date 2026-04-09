"""
Notification system for price changes, new SKUs, and region launches.

Supported channels:
  - Email (SMTP / Gmail app password)
  - Slack (incoming webhook)
  - Generic HTTP webhook (Teams, PagerDuty, custom)

All channels are optional and configured in config/settings.yaml or via
environment variables.
"""
from __future__ import annotations

import json
import os
import smtplib
import textwrap
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import requests

CHANGE_EMOJI = {
    "price_change": "💰",
    "new_sku": "🆕",
    "removed_sku": "🗑️",
    "new_region": "🌍",
}

CHANGE_LABEL = {
    "price_change": "Price Changed",
    "new_sku": "New SKU",
    "removed_sku": "SKU Removed",
    "new_region": "New Region",
}


def _fmt_dollar(val) -> str:
    if val is None:
        return "—"
    return f"${val:,.2f}"


def _fmt_pct(val) -> str:
    if val is None:
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


class Notifier:
    def __init__(self, config: dict):
        self._cfg = config.get("notifications", {})
        self._thresholds = self._cfg.get("thresholds", {})
        self._min_price_change_pct = self._thresholds.get("price_change_pct", 1.0)
        self._notify_new_sku = self._thresholds.get("new_sku", True)
        self._notify_removed = self._thresholds.get("removed_sku", True)
        self._notify_new_region = self._thresholds.get("new_region", True)

    # ── Public ────────────────────────────────────────────────────────────────

    def send(self, changes: List[dict]) -> dict:
        """
        Filter changes by thresholds and dispatch to all enabled channels.
        Returns summary dict: {channel: sent_count}.
        """
        filtered = self._filter(changes)
        if not filtered:
            return {}

        summary = {}
        email_cfg = self._cfg.get("email", {})
        slack_cfg = self._cfg.get("slack", {})
        webhook_cfg = self._cfg.get("webhook", {})

        if email_cfg.get("enabled") or os.environ.get("SMTP_USER"):
            sent = self._send_email(filtered)
            summary["email"] = sent

        if slack_cfg.get("enabled") or os.environ.get("SLACK_WEBHOOK_URL"):
            sent = self._send_slack(filtered)
            summary["slack"] = sent

        if webhook_cfg.get("enabled") or os.environ.get("WEBHOOK_URL"):
            sent = self._send_webhook(filtered)
            summary["webhook"] = sent

        return summary

    # ── Filtering ─────────────────────────────────────────────────────────────

    def _filter(self, changes: List[dict]) -> List[dict]:
        result = []
        for ch in changes:
            ct = ch.get("change_type", "")
            if ct == "price_change":
                pct = abs(ch.get("pct_change_monthly") or 0) or abs(ch.get("pct_change_per_gb") or 0)
                if pct >= self._min_price_change_pct:
                    result.append(ch)
            elif ct == "new_sku" and self._notify_new_sku:
                result.append(ch)
            elif ct == "removed_sku" and self._notify_removed:
                result.append(ch)
            elif ct == "new_region" and self._notify_new_region:
                result.append(ch)
        return result

    # ── Email ─────────────────────────────────────────────────────────────────

    def _send_email(self, changes: List[dict]) -> int:
        cfg = self._cfg.get("email", {})
        host = cfg.get("smtp_host", "smtp.gmail.com")
        port = cfg.get("smtp_port", 587)
        user = cfg.get("smtp_user") or os.environ.get("SMTP_USER", "")
        password = cfg.get("smtp_password") or os.environ.get("SMTP_PASSWORD", "")
        from_addr = cfg.get("from_address") or user
        to_addrs = list(cfg.get("to_addresses") or [])
        # NOTIFICATION_TO_EMAIL env var keeps recipient addresses out of the repo.
        # Accepts a comma-separated list, e.g. "a@example.com,b@example.com".
        env_to = os.environ.get("NOTIFICATION_TO_EMAIL", "")
        if env_to:
            to_addrs += [a.strip() for a in env_to.split(",") if a.strip()]

        if not user or not password or not to_addrs:
            print("  [Notify] Email: missing credentials or recipients, skipping.")
            return 0

        subject = self._email_subject(changes)
        html_body = self._email_html(changes)
        text_body = self._email_text(changes)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(host, port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(user, password)
                smtp.sendmail(from_addr, to_addrs, msg.as_string())
            print(f"  [Notify] Email sent to {len(to_addrs)} recipient(s).")
            return len(to_addrs)
        except Exception as exc:
            print(f"  [Notify] Email failed: {exc}")
            return 0

    def _email_subject(self, changes: List[dict]) -> str:
        providers = sorted({ch.get("provider", "").upper() for ch in changes})
        types = sorted({ch.get("change_type", "") for ch in changes})
        date_str = datetime.now().strftime("%b %d, %Y")
        type_summary = " & ".join([CHANGE_LABEL.get(t, t) for t in types[:2]])
        return f"[Cloud Pricing] {type_summary} detected for {', '.join(providers)} — {date_str}"

    def _email_text(self, changes: List[dict]) -> str:
        lines = [
            "Cloud Networking Price Change Alert",
            f"Detected: {datetime.now(timezone.utc).isoformat()}",
            f"Changes: {len(changes)}",
            "=" * 60,
        ]
        for ch in changes:
            ct = ch.get("change_type", "")
            em = CHANGE_EMOJI.get(ct, "")
            p = ch.get("provider", "").upper()
            sku = ch.get("sku_name", ch.get("sku_id", ""))[:70]
            region = ch.get("region_raw", "")
            lines.append(f"\n{em} [{p}] {ct.upper()}")
            lines.append(f"   SKU: {sku}")
            lines.append(f"   Region: {region}")
            if ct == "price_change":
                lines.append(
                    f"   Monthly: {_fmt_dollar(ch.get('old_price_monthly'))} → "
                    f"{_fmt_dollar(ch.get('new_price_monthly'))} "
                    f"({_fmt_pct(ch.get('pct_change_monthly'))})"
                )
            elif ct == "new_sku":
                lines.append(f"   Price: {_fmt_dollar(ch.get('new_price_monthly'))}/mo")
            elif ct == "removed_sku":
                lines.append(f"   Was: {_fmt_dollar(ch.get('old_price_monthly'))}/mo")
        return "\n".join(lines)

    def _email_html(self, changes: List[dict]) -> str:
        rows = ""
        for ch in changes:
            ct = ch.get("change_type", "")
            em = CHANGE_EMOJI.get(ct, "")
            label = CHANGE_LABEL.get(ct, ct)
            p = ch.get("provider", "").upper()
            sku = (ch.get("sku_name") or ch.get("sku_id", ""))[:80]
            region = ch.get("region_raw", "")

            if ct == "price_change":
                detail = (
                    f"{_fmt_dollar(ch.get('old_price_monthly'))} → "
                    f"<strong>{_fmt_dollar(ch.get('new_price_monthly'))}</strong> "
                    f"({_fmt_pct(ch.get('pct_change_monthly'))})"
                )
            elif ct == "new_sku":
                detail = f"<strong>{_fmt_dollar(ch.get('new_price_monthly'))}/mo</strong>"
            else:
                detail = f"Was {_fmt_dollar(ch.get('old_price_monthly'))}/mo"

            color = {"price_change": "#F9AB00", "new_sku": "#34A853",
                     "removed_sku": "#D93025"}.get(ct, "#666")
            rows += f"""
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #eee">
            {em} <strong style="color:{color}">{label}</strong>
          </td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee">
            <strong>[{p}]</strong> {sku}
          </td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee;color:#666">{region}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee">{detail}</td>
        </tr>"""

        return f"""<!DOCTYPE html><html><body style="font-family:sans-serif;color:#202124;max-width:700px">
  <div style="background:#1A73E8;padding:20px 24px;border-radius:8px 8px 0 0">
    <h2 style="color:white;margin:0;font-weight:400">Cloud Networking Price Alert</h2>
    <p style="color:rgba(255,255,255,.8);margin:4px 0 0;font-size:.85rem">
      {len(changes)} change(s) detected · {datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")}
    </p>
  </div>
  <table style="width:100%;border-collapse:collapse;border:1px solid #eee;border-top:none">
    <thead>
      <tr style="background:#F8F9FA">
        <th style="padding:8px 12px;text-align:left;font-size:.8rem">Type</th>
        <th style="padding:8px 12px;text-align:left;font-size:.8rem">SKU</th>
        <th style="padding:8px 12px;text-align:left;font-size:.8rem">Region</th>
        <th style="padding:8px 12px;text-align:left;font-size:.8rem">Price</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <p style="font-size:.75rem;color:#999;margin-top:16px">
    Run the dashboard for full analysis: <code>streamlit run dashboard.py</code>
  </p>
</body></html>"""

    # ── Slack ─────────────────────────────────────────────────────────────────

    def _send_slack(self, changes: List[dict]) -> int:
        cfg = self._cfg.get("slack", {})
        url = cfg.get("webhook_url") or os.environ.get("SLACK_WEBHOOK_URL", "")
        if not url:
            print("  [Notify] Slack: no webhook URL, skipping.")
            return 0

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"☁️ Cloud Networking Price Alert — {len(changes)} change(s)",
                },
            },
            {"type": "divider"},
        ]

        for ch in changes[:10]:  # Slack has message size limits
            ct = ch.get("change_type", "")
            em = CHANGE_EMOJI.get(ct, "")
            p = ch.get("provider", "").upper()
            sku = (ch.get("sku_name") or ch.get("sku_id", ""))[:60]
            region = ch.get("region_raw", "")

            if ct == "price_change":
                detail = (
                    f"{_fmt_dollar(ch.get('old_price_monthly'))} → "
                    f"{_fmt_dollar(ch.get('new_price_monthly'))} "
                    f"({_fmt_pct(ch.get('pct_change_monthly'))})"
                )
            elif ct == "new_sku":
                detail = f"New @ {_fmt_dollar(ch.get('new_price_monthly'))}/mo"
            else:
                detail = f"Removed (was {_fmt_dollar(ch.get('old_price_monthly'))}/mo)"

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{em} *[{p}]* {CHANGE_LABEL.get(ct, ct)}\n*{sku}* ({region})\n{detail}",
                },
            })

        if len(changes) > 10:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"…and {len(changes)-10} more changes."}],
            })

        try:
            r = requests.post(url, json={"blocks": blocks}, timeout=10)
            r.raise_for_status()
            print("  [Notify] Slack message sent.")
            return 1
        except Exception as exc:
            print(f"  [Notify] Slack failed: {exc}")
            return 0

    # ── Generic webhook ───────────────────────────────────────────────────────

    def _send_webhook(self, changes: List[dict]) -> int:
        cfg = self._cfg.get("webhook", {})
        url = cfg.get("url") or os.environ.get("WEBHOOK_URL", "")
        headers = cfg.get("headers", {})
        if not url:
            print("  [Notify] Webhook: no URL, skipping.")
            return 0

        payload = {
            "event": "cloud_price_change",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "change_count": len(changes),
            "changes": changes,
        }

        try:
            r = requests.post(url, json=payload, headers=headers, timeout=15)
            r.raise_for_status()
            print(f"  [Notify] Webhook sent to {url}.")
            return 1
        except Exception as exc:
            print(f"  [Notify] Webhook failed: {exc}")
            return 0
