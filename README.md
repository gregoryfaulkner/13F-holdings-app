# 13F Investment Manager Holdings

A Flask web app for fetching, analyzing, and visualizing SEC 13F/N-PORT filing data from investment managers. Build a combined portfolio from multiple managers, compare allocations against the MSCI ACWI benchmark, and generate PDF/Excel reports.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-3.x-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Features

- **Unified Manager Search** — Search 13F filers and mutual funds (N-PORT) from a single bar, with typeahead powered by SEC EDGAR and EFTS
- **Portfolio Construction** — Add multiple managers with custom weight allocations, save/load presets
- **Financial Enrichment** — Automatically enrich holdings with returns, P/E, EPS, dividends, sector, industry, and country via yfinance
- **ACWI Benchmarking** — Compare portfolio sector, geographic, and top holdings allocations against MSCI ACWI
- **Summary Dashboard** — Animated stat cards, 2x2 comparison chart grid, valuation bubble scatter, and auto-generated key takeaways
- **Heatmap** — Finviz-style grid with stocks sized by weight and colored by quarterly return
- **Spotlight Search (Ctrl+K)** — Instantly search stocks across all managers
- **Stock Drill-Down** — Click any ticker for a slide-out panel with sparkline, stats, and per-manager breakdown
- **History & Compare** — Save runs to SQLite, browse history, instantly compare any two runs quarter-over-quarter
- **Export** — PDF reports (landscape, with comparison charts), Excel workbooks (individual, combined, weighted), PPTX

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Install

```bash
pip install flask yfinance openpyxl python-pptx reportlab edgartools matplotlib
```

### Run

```bash
python 13F_stocks_app.py
```

Open **http://localhost:8080** in your browser.

### Usage

1. Type a manager name (e.g. "Berkshire", "Pershing Square") in the search bar
2. Select from the dropdown and click **+ Add**
3. Repeat for additional managers, adjust weights if desired
4. Click **Fetch Holdings** — progress streams via SSE
5. Explore the dashboard, charts, heatmap, overlap analysis, and more
6. Download PDF/Excel reports from the header buttons

## Architecture

```
EDGAR API → Parse 13F/N-PORT XML → Resolve CUSIPs (OpenFIGI) → Enrich (yfinance)
    ↓                                                                ↓
  Flask API (46 routes, SSE streaming)                    Excel/PPTX/PDF output
    ↓                                                                ↓
  Static frontend (Plotly.js, Tailwind CSS)              SQLite history (db.py)
```

- **Backend:** Pure Flask API with zero server-side templating. Threading for long-running fetches, SSE for real-time progress.
- **Frontend:** Single-page app with vanilla JS, Plotly.js charts, Tailwind CSS utilities, and a custom ~40-token CSS design system with glassmorphism and spring animations.
- **Data flow:** All data loaded client-side via `fetch()` calls to JSON API endpoints.

## Project Structure

```
13F_stocks_app.py    Flask API server (46 routes, SSE streaming)
holdings.py          EDGAR fetching, CUSIP resolution, Excel/PPTX/PDF output
analysis.py          Overlap, sector/geo breakdowns, weighting, QoQ diff
financial_data.py    yfinance enrichment, FMP ESG, ACWI benchmark
db.py                SQLite persistence for run history
static/
  index.html         HTML skeleton (CDN: Tailwind, Plotly, Google Fonts)
  css/app.css        Design system (~40 tokens, dark theme, responsive)
  js/app.js          All frontend logic (charts, search, SSE, state)
```

## Data Pipeline

### Ticker Resolution

1. Hardcoded `CUSIP_TO_TICKER` dictionary (120+ S&P 500 entries)
2. OpenFIGI API batch lookup (50 CUSIPs/request, 2 retries)
3. SEC `company_tickers.json` name-to-ticker fallback
4. Ticker alias mapping (e.g. FB → META)

### Enrichment Per Stock

Returns (filing quarter, prior quarter, QTD), forward P/E, trailing/forward EPS, EPS growth, dividend yield, sector, industry, country, market cap

### External APIs

| API | Auth | Purpose |
|-----|------|---------|
| SEC EDGAR | None | 13F-HR and NPORT-P filings |
| SEC EFTS | None | Full-text fund name search |
| yfinance | None | Stock fundamentals and returns |
| OpenFIGI | None | CUSIP → ticker resolution |
| FMP | API key (optional) | ESG scores |

## Configuration

On first run, `holdings_config.json` is created with defaults:

- **Managers & weights** — portfolio allocation percentages
- **Presets** — 100+ built-in manager presets
- **FMP API key** — optional, for ESG enrichment (250 calls/day free tier)
- **Top N** — number of holdings per manager (default: 20)
- **Reporting quarter** — date filter for filings

## Output Files

Each run generates:

| File | Description |
|------|-------------|
| `{manager}_top20_{period}.xlsx` | Individual manager holdings |
| `all_managers_top20_{date}.xlsx` | Combined, one row per manager-stock |
| `weighted_portfolio_{date}.xlsx` | Deduplicated, weighted by allocation |
| `report_{date}.pptx` | Charts (Portfolio vs ACWI) + tables |
| `report_{date}.pdf` | Full PDF report with comparison charts |

## License

MIT
