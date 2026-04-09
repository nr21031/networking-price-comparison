"""
CLI entry point for the GCP Networking Price Comparison tool.

Usage examples:

    # Fetch latest prices from all providers
    python main.py fetch

    # Fetch and immediately generate a report
    python main.py fetch --report

    # Fetch, report, and send notifications
    python main.py fetch --report --notify

    # Just generate a report from the most recent stored data
    python main.py report

    # List recent fetch runs
    python main.py runs

    # Show detected changes
    python main.py changes

    # Run the Streamlit dashboard
    python main.py dashboard
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.table import Table
from rich import print as rprint

import io
_stdout_utf8 = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
console = Console(file=_stdout_utf8, highlight=False, emoji=False, legacy_windows=False)

# ── Config ────────────────────────────────────────────────────────────────────

def load_config(config_path: str = None) -> dict:
    path = Path(config_path) if config_path else Path(__file__).parent / "config" / "settings.yaml"
    if not path.exists():
        console.print(f"[yellow]Config not found at {path}, using defaults.[/yellow]")
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ── CLI ───────────────────────────────────────────────────────────────────────

@click.group()
@click.option("--config", "-c", default=None, help="Path to settings.yaml")
@click.pass_context
def cli(ctx, config):
    """GCP Networking Price Comparison Tool"""
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config)


@cli.command()
@click.option("--providers", "-p", multiple=True,
              type=click.Choice(["gcp", "aws", "azure"]),
              help="Providers to fetch (default: all)")
@click.option("--report", is_flag=True, help="Generate HTML report after fetching")
@click.option("--notify", is_flag=True, help="Send notifications for detected changes")
@click.option("--output", "-o", default=None, help="Custom output path for the report")
@click.pass_context
def fetch(ctx, providers, report, notify, output):
    """Fetch latest prices from all (or selected) providers."""
    config = ctx.obj["config"]
    providers = list(providers) or ["gcp", "aws", "azure"]

    from storage.store import PriceStore
    from fetchers import GCPFetcher, AWSFetcher, AzureFetcher
    from notifications import Notifier

    store = PriceStore()
    run_id = store.start_run(providers)
    prev_run = store.get_previous_run(run_id)

    console.print(f"\n[bold blue]>> Fetch run started:[/bold blue] {run_id[:19]}")
    console.print(f"  Providers: {', '.join(p.upper() for p in providers)}\n")

    fetcher_map = {
        "gcp":   ("GCP Cloud Interconnect",   GCPFetcher(config)),
        "aws":   ("AWS Direct Connect",        AWSFetcher(config)),
        "azure": ("Azure ExpressRoute",        AzureFetcher(config)),
    }

    total_points = 0
    all_points = []

    for provider in providers:
        label, fetcher = fetcher_map[provider]
        console.print(f"  [cyan]Fetching {label}...[/cyan]")
        try:
            points = fetcher.fetch()
            saved = store.save_prices(run_id, points)
            all_points.extend(points)
            total_points += saved
            console.print(f"  [green]OK[/green] {label}: {saved} price points")
        except Exception as exc:
            console.print(f"  [red]FAIL[/red] {label}: {exc}")

    # Change detection
    console.print(f"\n  [cyan]Detecting changes vs previous run...[/cyan]")
    changes = store.detect_and_save_changes(run_id, prev_run)
    store.complete_run(run_id, total_points)

    if changes:
        console.print(f"  [yellow]ALERT[/yellow] {len(changes)} change(s) detected!\n")
        _print_changes_table(changes[:10])
        if len(changes) > 10:
            console.print(f"  ... and {len(changes)-10} more. Run `python main.py changes` for full list.")
    else:
        console.print("  [green]OK[/green] No price changes detected.")

    console.print(f"\n[bold green]DONE: Fetch complete:[/bold green] {total_points} total price points saved.\n")

    # Notifications
    if notify and changes:
        console.print("  [cyan]Sending notifications...[/cyan]")
        notifier = Notifier(config)
        result = notifier.send(changes)
        unnotified = [c["id"] for c in store.get_unnotified_changes() if c.get("id")]
        if unnotified:
            store.mark_changes_notified(unnotified)
        for channel, sent in result.items():
            console.print(f"    {channel}: {sent} sent")

    # Report
    if report:
        _generate_report(run_id, config, store, output)


@cli.command()
@click.option("--output", "-o", default=None, help="Custom output path for the HTML report")
@click.option("--run-id", default=None, help="Specific run ID to report on (default: latest)")
@click.pass_context
def report(ctx, output, run_id):
    """Generate an HTML exec report from the most recent (or specified) data."""
    config = ctx.obj["config"]
    from storage.store import PriceStore

    store = PriceStore()
    if run_id is None:
        run_id = store.get_latest_run()
        if not run_id:
            console.print("[red]No completed fetch runs found. Run `python main.py fetch` first.[/red]")
            sys.exit(1)

    _generate_report(run_id, config, store, output)


@cli.command()
@click.option("--limit", "-n", default=20, help="Number of runs to show")
@click.pass_context
def runs(ctx, limit):
    """List recent fetch runs."""
    from storage.store import PriceStore
    store = PriceStore()
    run_list = store.list_runs(limit)

    if not run_list:
        console.print("[yellow]No fetch runs found.[/yellow]")
        return

    table = Table(title="Recent Fetch Runs", show_header=True, header_style="bold blue")
    table.add_column("Run ID", style="cyan", no_wrap=True)
    table.add_column("Started", style="white")
    table.add_column("Status", style="white")
    table.add_column("Records", justify="right")
    table.add_column("Providers")

    for r in run_list:
        status_color = {
            "completed": "[green]✓ completed[/green]",
            "running": "[yellow]⟳ running[/yellow]",
            "failed": "[red]✗ failed[/red]",
        }.get(r.get("status", ""), r.get("status", ""))

        import json
        try:
            providers = ", ".join(json.loads(r.get("providers") or "[]")).upper()
        except Exception:
            providers = r.get("providers", "")

        table.add_row(
            r["run_id"][:19],
            r["started_at"][:19],
            status_color,
            str(r.get("record_count", 0)),
            providers,
        )

    console.print(table)


@cli.command()
@click.option("--limit", "-n", default=50, help="Number of changes to show")
@click.option("--provider", default=None, help="Filter by provider")
@click.pass_context
def changes(ctx, limit, provider):
    """Show recent detected price changes."""
    from storage.store import PriceStore
    store = PriceStore()
    change_list = store.get_recent_changes(limit)

    if provider:
        change_list = [c for c in change_list if c.get("provider") == provider]

    if not change_list:
        console.print("[green]No price changes recorded yet.[/green]")
        return

    table = Table(title=f"Recent Price Changes ({len(change_list)})",
                  show_header=True, header_style="bold yellow")
    table.add_column("Detected", style="cyan", no_wrap=True)
    table.add_column("Type", style="white")
    table.add_column("Provider")
    table.add_column("SKU", max_width=50)
    table.add_column("Region")
    table.add_column("Old Price", justify="right")
    table.add_column("New Price", justify="right")
    table.add_column("Change", justify="right")

    for ch in change_list:
        ct = ch.get("change_type", "")
        type_fmt = {
            "price_change": "[yellow]💰 price[/yellow]",
            "new_sku":      "[green]🆕 new[/green]",
            "removed_sku":  "[red]🗑 removed[/red]",
        }.get(ct, ct)

        old_p = f"${ch['old_price_monthly']:,.2f}" if ch.get("old_price_monthly") else "—"
        new_p = f"${ch['new_price_monthly']:,.2f}" if ch.get("new_price_monthly") else "—"
        pct = ch.get("pct_change_monthly")
        pct_str = f"{pct:+.2f}%" if pct is not None else "—"
        pct_fmt = f"[red]{pct_str}[/red]" if (pct and pct > 0) else f"[green]{pct_str}[/green]"

        table.add_row(
            (ch.get("detected_at") or "")[:19],
            type_fmt,
            (ch.get("provider") or "").upper(),
            (ch.get("sku_name") or ch.get("sku_id", ""))[:50],
            ch.get("region_raw", ""),
            old_p, new_p,
            pct_fmt if pct is not None else "—",
        )

    console.print(table)


@cli.command()
def dashboard():
    """Launch the Streamlit interactive dashboard."""
    dash_path = Path(__file__).parent / "dashboard.py"
    console.print("[bold blue]Launching Streamlit dashboard...[/bold blue]")
    console.print("  Open [cyan]http://localhost:8501[/cyan] in your browser.\n")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(dash_path)],
        cwd=str(Path(__file__).parent),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _generate_report(run_id: str, config: dict, store, output: str = None):
    from analysis.compare import PriceComparator
    from reports import HTMLReportGenerator

    console.print(f"\n  [cyan]Generating report for run {run_id[:19]}...[/cyan]")
    records = store.get_prices_for_run(run_id)
    if not records:
        console.print("[red]  No records found for this run.[/red]")
        return

    comp = PriceComparator(records)
    changes = store.get_recent_changes(50)
    gen = HTMLReportGenerator(comp, run_id, changes, config)
    path = gen.generate(output)
    console.print(f"  [green]OK[/green] Report saved: [bold]{path}[/bold]\n")
    console.print("  Open the file in your browser to view the exec report.")


def _print_changes_table(changes: list):
    table = Table(show_header=True, header_style="bold yellow",
                  show_lines=False, box=None)
    table.add_column("Provider", style="cyan")
    table.add_column("Type")
    table.add_column("SKU", max_width=55)
    table.add_column("Change", justify="right")

    for ch in changes:
        ct = ch.get("change_type", "")
        pct = ch.get("pct_change_monthly")
        pct_str = f"{pct:+.2f}%" if pct is not None else ""
        table.add_row(
            (ch.get("provider") or "").upper(),
            ct,
            (ch.get("sku_name") or "")[:55],
            f"[red]{pct_str}[/red]" if (pct and pct > 0) else f"[green]{pct_str}[/green]",
        )
    console.print(table)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
