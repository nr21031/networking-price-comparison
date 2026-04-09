# GCP Networking Price Intelligence

An executive-grade competitive pricing dashboard for **GCP Cloud Interconnect** vs **AWS Direct Connect** vs **Azure ExpressRoute** — built for GCP's Networking Pricing & Packaging team.

Live prices are pulled directly from each provider's public pricing API on demand. All comparisons are normalized for fair apples-to-apples analysis (e.g. Azure's 2-circuit-per-purchase bundling).

🔗 **Live dashboard:** https://networking-price-dashboard-gnh4usnu7a-uc.a.run.app

---

## Features

| Tab | What it shows |
|-----|---------------|
| **Overview** | Executive KPIs + all-flavour comparison table (Dedicated, Partner, Cross-Cloud, Site-to-Site) with pricing scope and model indicators |
| **Port Fees** | Head-to-head port/circuit fee comparison by region and speed |
| **TCO Scenarios** | All-in monthly cost with utilisation % input; Azure Metered vs Unlimited split; Azure per-circuit normalisation |
| **Regional** | Full price table across all regions × speeds with multiselect filters; GCP global flat-rate correctly propagated to all regions |
| **Changes** | Price change history with configurable email/Slack/webhook alerts |
| **Coverage** | Regional availability map with pricing scope and model type summary |

### Key pricing insights surfaced
- **Azure 2-circuit bundling** — Azure ExpressRoute purchases include a primary *and* secondary circuit per port. Every table shows both the list price and a per-circuit normalised price (÷ 2) for fair comparison.
- **GCP global flat rate** — GCP charges the same price in every region; AWS and Azure use regional/zone pricing. This is highlighted in the comparison tables.
- **Azure Metered vs Unlimited** — TCO tab shows both models side-by-side; at high utilisation Unlimited is often cheaper.
- **All 4 interconnect flavours** — Dedicated/Direct, Partner/Hosted, Cross-Cloud (GCP-exclusive), and Site-to-Site extensions mapped across all three providers.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Streamlit Dashboard                    │
│                    (dashboard.py)                       │
└────────────┬────────────────────────────────┬───────────┘
             │                                │
    ┌────────▼────────┐              ┌────────▼────────┐
    │  Price Fetchers  │              │    Comparator   │
    │  fetchers/       │              │ analysis/       │
    │  ├── gcp.py      │              │  compare.py     │
    │  ├── aws.py      │              └────────┬────────┘
    │  └── azure.py    │                       │
    └────────┬─────────┘              ┌────────▼────────┐
             │                        │   HTML Report   │
    ┌────────▼─────────┐              │ reports/        │
    │   SQLite Store   │              │  html_report.py │
    │   storage/       │              └─────────────────┘
    │   store.py       │
    └────────┬─────────┘
             │  sync every 5 min
    ┌────────▼─────────┐
    │   GCS Bucket     │
    │  prices.db       │
    └──────────────────┘
```

**Price sources (all public APIs, no auth required except GCP):**
| Provider | API | Auth |
|----------|-----|------|
| GCP | [Cloud Billing Catalog API](https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list) | Free API key |
| AWS | [Price List API](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDirectConnect/) | None |
| Azure | [Retail Prices API](https://prices.azure.com/api/retail/prices) | None |

---

## Local development

### Prerequisites
- Python 3.11+
- (Optional) GCP API key for live GCP prices — [get one free](https://console.cloud.google.com/apis/credentials)

### Setup

```bash
git clone https://github.com/nr21031/networking-price-comparison.git
cd networking-price-comparison

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configuration

```bash
# Option A: environment variable (recommended)
export GCP_API_KEY="your-key-here"

# Option B: edit config/settings.yaml
gcp:
  api_key: "your-key-here"
```

### Run

```bash
# Fetch latest prices from all 3 providers
python main.py fetch

# Launch interactive dashboard
streamlit run dashboard.py

# Generate standalone HTML report
python main.py report
```

### CLI reference

```
python main.py fetch            # Fetch prices (all providers)
python main.py fetch --report   # Fetch + generate HTML report
python main.py fetch --notify   # Fetch + send change notifications
python main.py report           # Generate HTML report from latest data
python main.py runs             # List fetch run history
python main.py changes          # Show recent price changes
python main.py dashboard        # Launch Streamlit dashboard
```

---

## Cloud Run deployment

The app is deployed on GCP Cloud Run with:
- **SQLite database** synced to/from Cloud Storage (`gs://testinggcpforwork-networking-prices/prices.db`) every 5 minutes — survives container restarts
- **GCP API key** stored in Secret Manager (`networking-gcp-api-key`)
- **Zero to 3 instances** — scales to zero when idle (~$0 when not in use)

### Re-deploy manually

```bash
gcloud builds submit \
  --config=cloudbuild.yaml \
  --project=testinggcpforwork \
  --substitutions=COMMIT_SHA=$(git rev-parse --short HEAD) \
  .
```

### CI/CD

Every push to `main` triggers an automatic build + deploy via Cloud Build. To enable:
1. Go to [Cloud Build → Triggers](https://console.cloud.google.com/cloud-build/triggers?project=testinggcpforwork)
2. Click **Connect Repository** → select `nr21031/networking-price-comparison`
3. Create trigger from the existing `cloudbuild.yaml`

---

## Notifications

Configure price change alerts via environment variables (keep credentials out of the repo):

| Env var | Purpose |
|---------|---------|
| `SMTP_USER` | Gmail / Workspace address used to send alerts |
| `SMTP_PASSWORD` | Gmail app password ([create one here](https://myaccount.google.com/apppasswords)) |
| `NOTIFICATION_TO_EMAIL` | Comma-separated recipient list — stored in Secret Manager, never committed |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |

Enable the channel in `config/settings.yaml` (no addresses needed there):

```yaml
notifications:
  email:
    enabled: true          # turn on the channel
    to_addresses: []       # leave blank — set NOTIFICATION_TO_EMAIL env var instead
  thresholds:
    price_change_pct: 1.0  # alert on ≥1% price change
    new_sku: true
    removed_sku: true
```

On Cloud Run, recipient addresses are injected from Secret Manager at runtime and never appear in source control.

---

## Project structure

```
├── dashboard.py          # Streamlit app (6 tabs)
├── main.py               # CLI entry point
├── run.py                # Cloud Run entrypoint (GCS sync + Streamlit)
├── requirements.txt
├── Dockerfile
├── cloudbuild.yaml       # CI/CD pipeline
├── config/
│   └── settings.yaml     # Provider config, region mapping, TCO scenarios
├── fetchers/
│   ├── gcp.py            # GCP Billing API fetcher
│   ├── aws.py            # AWS Price List API fetcher
│   └── azure.py          # Azure Retail Prices API fetcher
├── analysis/
│   └── compare.py        # Normalisation, comparison, TCO engine
├── storage/
│   └── store.py          # SQLite persistence + change detection
├── reports/
│   └── html_report.py    # Self-contained HTML report generator
└── notifications/
    └── notifier.py       # Email / Slack / webhook alerts
```

---

## License

Internal use only.
