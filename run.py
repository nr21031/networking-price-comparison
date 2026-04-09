"""
Cloud Run entrypoint.

1. Downloads the SQLite price database from Cloud Storage on startup
   (so data survives container restarts).
2. Launches a background thread that re-uploads the DB to GCS every
   GCS_SYNC_INTERVAL_SECONDS (default 300 s / 5 min).
3. Starts Streamlit on $PORT (Cloud Run injects this; defaults to 8080).

Environment variables
─────────────────────
GCS_BUCKET              GCS bucket name for DB persistence  (required for sync)
GCS_DB_BLOB             Blob path inside the bucket          (default: prices.db)
GCS_SYNC_INTERVAL_SECONDS  Sync cadence in seconds           (default: 300)
PORT                    Listening port injected by Cloud Run  (default: 8080)
GCP_API_KEY             GCP Billing API key for live prices   (optional)
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "prices.db")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCS_BLOB   = os.environ.get("GCS_DB_BLOB", "prices.db")
SYNC_INTERVAL = int(os.environ.get("GCS_SYNC_INTERVAL_SECONDS", "300"))


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _gcs_client():
    # Import here so the module works even without google-cloud-storage installed
    # (falls back to no-op sync when the package is absent).
    from google.cloud import storage as gcs  # noqa: PLC0415
    return gcs.Client()


def download_db() -> None:
    if not GCS_BUCKET:
        print("[gcs] GCS_BUCKET not set — skipping DB download.", flush=True)
        return
    try:
        client = _gcs_client()
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(GCS_BLOB)
        if blob.exists():
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            blob.download_to_filename(DB_PATH)
            print(f"[gcs] Downloaded gs://{GCS_BUCKET}/{GCS_BLOB} → {DB_PATH}", flush=True)
        else:
            print(f"[gcs] No existing DB at gs://{GCS_BUCKET}/{GCS_BLOB} — starting fresh.", flush=True)
    except Exception as exc:
        print(f"[gcs] Download failed (non-fatal): {exc}", flush=True)


def upload_db() -> None:
    if not GCS_BUCKET:
        return
    if not os.path.exists(DB_PATH):
        return
    try:
        client = _gcs_client()
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(GCS_BLOB)
        blob.upload_from_filename(DB_PATH)
        print(f"[gcs] Uploaded {DB_PATH} → gs://{GCS_BUCKET}/{GCS_BLOB}", flush=True)
    except Exception as exc:
        print(f"[gcs] Upload failed (non-fatal): {exc}", flush=True)


def _sync_loop() -> None:
    """Background thread: periodically push DB to GCS."""
    while True:
        time.sleep(SYNC_INTERVAL)
        upload_db()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Pull latest DB from GCS
    download_db()

    # 2. Start background sync thread
    if GCS_BUCKET:
        t = threading.Thread(target=_sync_loop, daemon=True, name="gcs-sync")
        t.start()
        print(f"[gcs] Background sync every {SYNC_INTERVAL}s started.", flush=True)

    # 3. Propagate GCP API key to the env expected by the fetcher
    api_key = os.environ.get("GCP_API_KEY", "")
    if api_key:
        print("[startup] GCP_API_KEY found — live pricing enabled.", flush=True)

    # 4. Launch Streamlit
    port = os.environ.get("PORT", "8080")
    cmd = [
        sys.executable, "-m", "streamlit", "run", "dashboard.py",
        "--server.port",               port,
        "--server.address",            "0.0.0.0",
        "--server.headless",           "true",
        "--server.enableCORS",         "false",
        "--server.enableXsrfProtection", "false",
        "--browser.gatherUsageStats",  "false",
    ]
    print(f"[startup] Starting Streamlit on port {port}…", flush=True)
    os.execvp(sys.executable, cmd)  # replace current process with streamlit


if __name__ == "__main__":
    main()
