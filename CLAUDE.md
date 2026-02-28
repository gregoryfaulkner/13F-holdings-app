# 13F Investment Manager Holdings App

Flask web app for fetching, analyzing, and visualizing SEC 13F filing data from investment managers.

**Run:** `python 13F_stocks_app.py` → http://localhost:8080

## Setup

**Dependencies** (no requirements.txt — install manually):
- Flask, yfinance, openpyxl, python-pptx, reportlab

**Config:** `holdings_config.json` — managers, weights, presets (100+ built-in), FMP API key
**Database:** `holdings_history.db` — SQLite, auto-created on first run

## Key Files

### Backend
| File | Purpose |
|------|---------|
| `13F_stocks_app.py` | Flask API server, 46 routes, serves static frontend, mutual fund search, SSE streaming |
| `holdings.py` | EDGAR 13F/N-PORT fetching, OpenFIGI CUSIP resolution, Excel/PPTX/CSV output, PDF report generation |
| `analysis.py` | Overlap analysis, sector/geo breakdowns, manager weighting, QoQ diff, summary stats |
| `financial_data.py` | yfinance enrichment (returns, P/E, EPS, sector/industry/country), FMP ESG, ACWI benchmark |
| `db.py` | SQLite persistence for historical run tracking (save/load/compare/delete) |

### Frontend (`static/`)
| File | Purpose |
|------|---------|
| `index.html` | HTML skeleton with CDN links (Tailwind, Plotly, Google Fonts) |
| `css/app.css` | ~40-token design system, glassmorphism, animations, responsive breakpoints |
| `js/app.js` | All JS: chart rendering, typeahead (stale-while-revalidate), SSE, state management |

### Shared JS helpers
- `qrColor(qr, fallback)` — quarterly-return HSL color
- `plotTreemap(elId, data, opts)` — shared Plotly treemap config + click→stock panel
- `plotCompBar(elId, cfg)` — grouped horizontal bar (Portfolio vs ACWI)
- `populateQuarterSelect()` — reporting quarter dropdown options

## Architecture

- **Backend:** Pure Flask API (zero templating), threading for long-running fetches, SSE for progress
- **Frontend:** Separate static files served by Flask. Tailwind CSS, Plotly.js, Inter + JetBrains Mono fonts
- **Data flow:** EDGAR API → parse holdings → OpenFIGI resolve CUSIPs → enrich via yfinance + FMP → write files + serve via API
- **All data loaded client-side** via `fetch()` — no server-side templating

## Data Pipeline

### Ticker Resolution Chain
1. XML extraction from filing
2. Hardcoded `CUSIP_TO_TICKER` dict (120+ S&P 500 entries)
3. OpenFIGI API (batch of 50, 2 retries, 20s timeout)
4. SEC `company_tickers.json` name→ticker fallback

### Enrichment Fields Per Stock
returns (filing quarter, prior quarter, QTD), forward P/E, trailing EPS, forward EPS, forward EPS growth, dividend yield, sector, industry, country, market cap, ESG scores (env/social/gov via FMP)

### Search (unified bar, 3-tier lookup)
1. SEC mutual fund tickers (exact + prefix match from 28K+ fund list)
2. SEC EFTS full-text search for NPORT-P filings
3. EDGAR company search fallback

### Caching
- **Server:** `_search_cache` (15min TTL), `_edgar_form_cache` (15min TTL), ACWI benchmark (24hr)
- **Client:** `_taCache` (15min TTL), `_spotData` (invalidated on reset), stale-while-revalidate typeahead

## External APIs

| API | Auth | Notes |
|-----|------|-------|
| SEC EDGAR | None | 13F-HR and NPORT-P filings |
| SEC EFTS | None | Full-text search for fund name lookup |
| yfinance | None | Stock fundamentals, returns, sparklines |
| OpenFIGI | None | CUSIP→ticker, 20 req/min, 50/batch |
| FMP | API key | ESG scores, 250 calls/day free tier |

## API Endpoints (key ones)

- `/api/search-unified?q=` — Combined 13F + N-PORT typeahead (fund name, ticker, or manager)
- `/api/summary-data` — Summary stats, weighted top stocks, quarter labels, avg ESG
- `/api/sector-data` — Sector + industry breakdown with GICS-normalized sectors
- `/api/geo-data` — Country-level portfolio breakdown (raw + iShares-normalized)
- `/api/acwi-benchmark` — MSCI ACWI benchmark weights (sectors, countries, top holdings)
- `/api/category-stocks?type=sector|industry|country&name=...` — Drill-down: top 8 stocks + manager breakdown
- `/api/valuation-scatter` — Top 20 by weight with P/E, EPS growth, dividend yield
- `/api/stock-detail/<ticker>` — Full detail: stats, manager breakdown, 6-month sparkline, weight history
- `/api/treemap-data`, `/api/sector-treemap-data`, `/api/geo-treemap-data` — Treemap data
- `/api/qoq-diff` — Quarter-over-quarter holdings diff
- `/api/history` — List/load/compare/delete stored runs

## Output Files Per Run

- `{manager}_top20_{period}.xlsx` — Individual manager holdings
- `all_managers_top20_{date}.xlsx` — Combined (one row per manager-stock)
- `weighted_portfolio_{date}.xlsx` — Deduplicated, weighted by manager allocations (32 cols + ESG)
- `report_{date}.pptx` — Grouped bar charts (Portfolio vs ACWI) + tables
- `report_{date}.pdf` — PDF with comparison charts and holdings tables

## Design System

- **Philosophy:** Joe Gebbia-inspired — story-driven UX, generous whitespace, craft in transitions
- **Palette:** Warmer dark (`#0c1220` base), accent `#5b8def`, glassmorphism (`backdrop-filter:blur`)
- **Tokens:** ~40 CSS custom properties in `:root` (spacing, colors, typography, radius, shadows, easing)
- **Animations:** `fadeInUp`, `fadeInScale`, `slideInRight`, spring easing, stagger delays, skeleton shimmer
- **Charts:** Dark theme (`#0c1220` bg), blue (`#5b8def`) portfolio vs amber (`#F59E0B`) ACWI

## UI Features

- **Summary Dashboard:** 2x2 comparison grid (sector/geo/holdings/industry) + valuation scatter + key takeaways
- **Hero Insight Card:** Narrative headline with animated metrics (holdings, unique stocks, avg return, EPS beat rate)
- **Heatmap Tab:** Finviz-style CSS grid, stocks sized by weight, colored by quarterly return (HSL interpolation)
- **Spotlight Search (Ctrl+K):** Cross-manager stock search overlay with per-manager weight pills
- **Stock Drill-Down Panel:** Click any ticker → slide-out panel with sparkline, stats, manager table, weight history
- **History Panel:** Browse/load/label/delete past runs, instant QoQ compare between any two runs
- **Category Drill-Down:** Click any bar chart segment → detail card with top stocks + manager pills

## Conventions

- All new data fields must be added across the full pipeline: `financial_data.py` → `holdings.py` (FIELDNAMES + Excel) → `13F_stocks_app.py` (API) → `db.py` (schema + migration) → `app.js` (UI)
- Sector names use GICS standard via `normalize_sector_name()` for ACWI matching
- Country names normalized via `normalize_country_name()` for iShares matching
- `doReset()` in app.js must clear all chart elements, caches, and UI state
- DB schema changes need `ALTER TABLE ADD COLUMN` migration with existence check in `db.py`

## Changelog

Detailed change history is in [CHANGELOG.md](CHANGELOG.md).
