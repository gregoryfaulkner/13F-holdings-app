# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Flask web app for fetching, analyzing, and visualizing SEC 13F/N-PORT filing data from investment managers. Builds combined weighted portfolios with written CIO-style analysis, holdings tables, bar charts, and Excel export.

## Running

**Main app:** `python 13F_stocks_app.py` → http://localhost:8080

**Dependencies** (no requirements.txt — install manually):
```bash
pip install flask yfinance openpyxl edgartools matplotlib numpy
```

**Config:** `holdings_config.json` — managers, weights, presets (100+ built-in), top_n, reporting quarter. Auto-created on first run. Gitignored.

**Database:** `holdings_history.db` — SQLite, auto-created on first run. History UI removed; backend endpoints (`/api/history`) still exist.

**Windows note:** To restart the server, use `taskkill //F //IM python.exe` — `pkill` does NOT work on Windows/Git Bash.

## Key Files

### Backend
| File | Purpose |
|------|---------|
| `13F_stocks_app.py` | Flask API server, serves static frontend, mutual fund search, SSE streaming, enrichment merge |
| `holdings.py` | EDGAR 13F/N-PORT fetching, OpenFIGI CUSIP resolution, simplified Excel output (single file) |
| `analysis.py` | Sector/geo breakdowns (with stocks_detail), manager weighting, summary stats, key highlights, written analysis, portfolio table data |
| `financial_data.py` | yfinance enrichment (returns, P/E, EPS growth, monthly returns, sector/industry/country) |
| `db.py` | SQLite persistence for historical run tracking (save/load/delete) |

### Frontend (`static/`)
| File | Purpose |
|------|---------|
| `index.html` | HTML skeleton with CDN links (Plotly, Google Fonts) |
| `css/app.css` | Dark design system, glassmorphism, animations, responsive breakpoints |
| `js/app.js` | All JS: table rendering, bubble chart, bar charts (with click-to-pin popups), typeahead, SSE, top 10/20 toggle |

## Architecture

- **Backend:** Pure Flask API (zero templating), threading for long-running fetches, SSE for progress
- **Frontend:** Separate static files served by Flask. Plotly.js, Inter + JetBrains Mono fonts. All data loaded client-side via `fetch()`.
- **Data flow:** EDGAR API → parse holdings → OpenFIGI resolve CUSIPs → enrich via yfinance → write Excel + serve via API

### Data Pipeline

**Ticker Resolution Chain:**
1. XML extraction from filing (ticker column if present)
2. Hardcoded `CUSIP_TO_TICKER` dict (120+ S&P 500 entries)
3. OpenFIGI API (batch of 50, 2 retries, 20s timeout)
4. `_TICKER_ALIASES` dict (e.g. FB→META, CLHB→CLH) — applied after all resolution
5. SEC `company_tickers.json` name→ticker fallback (`_resolve_remaining_tickers`)

**Enrichment Fields Per Stock:**
Returns (filing quarter, QTD, monthly within quarter), forward P/E, forward EPS growth, dividend yield, current price, sector, industry, country, market cap, EPS beat data, monthly returns within current quarter

**Data Sources (yfinance):**
- `forward_eps_growth`: `t.growth_estimates` DataFrame, `+1y` row, `stockTrend` column
- `forward_pe`: `info.get("forwardPE")` = currentPrice / forwardEps
- `dividend_yield`: `info.get("dividendYield")` — already in percentage form (0.39 = 0.39%, NOT decimal)
- `monthly_returns`: computed from yfinance historical prices per calendar month within current quarter

**Weighted Aggregation Methodology:**
- P/E: Weighted harmonic mean (excludes values ≤ 0)
- EPS growth: Weighted arithmetic mean, winsorized at ±50% per stock (`_clamp_growth()`)
- QTD return, monthly returns, dividend yield: Weighted arithmetic mean
- All totals computed from ALL holdings, not just displayed top N

**Unified Search (3-tier lookup):**
1. SEC mutual fund tickers (exact + prefix match from 28K+ fund list)
2. SEC EFTS full-text search for NPORT-P filings
3. EDGAR company search fallback

### Caching
- **Server:** `_search_cache` (15min TTL), `_edgar_form_cache` (15min TTL)
- **Client:** `_taCache` (15min TTL), stale-while-revalidate typeahead

## External APIs

| API | Auth | Notes |
|-----|------|-------|
| SEC EDGAR | None | 13F-HR and NPORT-P filings |
| SEC EFTS | None | Full-text search for fund name lookup |
| yfinance | None | Stock fundamentals, returns, growth estimates |
| OpenFIGI | None (optional key for higher limits) | CUSIP→ticker, 20 req/min, 50/batch |

## API Endpoints (key ones)

- `/api/search-unified?q=` — Combined 13F + N-PORT typeahead (fund name, ticker, or manager)
- `/api/summary-data` — Summary stats, weighted top stocks, quarter labels
- `/api/sector-data` — Sector + industry breakdown
- `/api/geo-data` — Country-level portfolio breakdown
- `/api/written-analysis` — Key highlights (5 takeaways) + CIO-style analysis paragraphs (overview, geography, sectors, valuation, earnings, risks)
- `/api/portfolio-table?top_n=10` — Holdings table data with totals (weighted portfolio + per-manager)
- `/api/bubble-data?manager=` — Bubble chart data (Fwd P/E vs EPS Growth), optional manager filter
- `/api/history` — List/load/delete stored runs
- `/api/presets` — Save/load/delete manager presets
- `DELETE /api/presets/<name>` — Delete a preset

## UI Sections

1. **Key Highlights** — 5 institutional-quality takeaways (sector, strategy, valuation, earnings, risk)
2. **Written Analysis** — CIO-oriented paragraphs generated from data (no LLM)
3. **Holdings Tables** — Weighted portfolio + per-manager tables with 12 columns + dynamic monthly returns
4. **Top 10/20 Toggle** — Switch displayed rows; totals always use all holdings
5. **Bubble Chart** — Fwd P/E vs EPS Growth scatter with manager toggle buttons
6. **Bar Charts** — Geographic, Sector, Industry allocation (horizontal bars via Plotly, click-to-pin stock detail popups)
7. **Excel Download** — Single file with weighted portfolio sheet + per-manager sheets

## Holdings Table Columns (12 fixed + dynamic monthly)

| # | Column | Notes |
|---|--------|-------|
| 1 | % of Portfolio | Weight in combined portfolio |
| 2 | Stock Name | Shortened via `shorten_stock_name()` |
| 3 | Ticker | |
| 4 | Sector | |
| 5 | Industry | |
| 6 | Quarter End Price | `filing_price_qtr_end` |
| 7 | Current Price | |
| 8 | QTD Total Return | |
| — | Monthly returns | Dynamic columns (Jan, Feb, Mar MTD, etc.) within current quarter |
| 9 | Forward P/E | |
| 10 | Forward EPS Growth | |
| 11 | Reported EPS | `filing_reported_eps` |
| 12 | EPS Beat | Green ▲ / red ▼ / yellow ► + beat % |

## Output Files

- `portfolio_{date}.xlsx` — Single Excel file with "Weighted Portfolio" sheet + one sheet per manager. All holdings ranked by weight, 12 columns + monthly returns, totals row, methodology footnotes.

## Conventions

- All new data fields must be added across the full pipeline: `financial_data.py` → `13F_stocks_app.py` (enrichment merge in `run_fetch()`) → `analysis.py` (summary stats + table data + written analysis) → `holdings.py` (Excel columns) → `app.js` (UI table)
- `_clamp_growth()` helper in analysis.py winsorizes growth rates at ±50%
- `_generate_key_highlights()` in analysis.py produces 5 institutional-quality takeaways
- `shorten_stock_name()` in analysis.py handles short display names
- `_TICKER_ALIASES` in holdings.py maps wrong/old tickers to correct ones (e.g. CLHB→CLH)
- **Totals use all holdings**, not just the displayed top N — never compute totals from the truncated list
- `doReset()` in app.js must clear all chart elements (including `Plotly.purge('bubbleChart')`), caches, and UI state
- DB schema changes need `ALTER TABLE ADD COLUMN` migration with existence check in `db.py`
- **Enrichment error handling:** yfinance failures default to `None` per field; stocks with missing data are still included in holdings (shown as N/A in UI). Do not skip or filter them out.
- Server restart on Windows: `taskkill //F //IM python.exe` then `python 13F_stocks_app.py`

## Changelog

Detailed change history is in [CHANGELOG.md](CHANGELOG.md).
