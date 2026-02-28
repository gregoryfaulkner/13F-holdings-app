# Changelog

## Feb 2026

### QTD Returns (Quarter-to-Date Performance Tracking)
Calculates total return from day after reporting quarter end to previous trading day's close. Example: if reporting = 4Q25 (2025-12-31), QTD = return from 2026-01-01 to last trading day before today.

- **`financial_data.py`**: Added `qtd_return_pct` and `qtd_price_start` to `_empty_result()`; in `fetch_ticker_data()`, fetches yfinance history from quarter_end+1 to today, uses last available close as end price; guards: `today > quarter_end_date` and `len(hist) >= 2`
- **`holdings.py`**: Added `qtd_return_pct`, `qtd_price_start` to `FIELDNAMES` and `_build_rows()`; "QTD Return %" column in per-manager Excel (col 26) and weighted portfolio Excel (col 28); return columns get green/red color formatting
- **`analysis.py`**: Extended `_calc_weighted_return()` with `qtd_sum`/`qtd_weight_sum` → `qtd_weighted_return`; `compute_summary_stats()` picks it up via existing `weighted_ret` dict
- **`13F_stocks_app.py`**: Added `qtd_return_pct`/`qtd_price_start` to enrichment mapping loop; `qtd_return_pct` in `/api/stock-detail/<ticker>` response; `qtd_return` in treemap data for hover tooltips
- **`db.py`**: Added `qtd_return_pct`, `qtd_price_start` to `_HOLDINGS_COLUMNS`, schema, and migration
- **`app.js` UI**: New stat box "Wtd QTD Return", hero insight subtext, stock drill-down QTD pill, summary findings, treemap hover

### Search & Settings UX Overhaul
- **Search moved above Managers** (`index.html`): Typeahead dropdown opens downward into empty space. Search card has `z-index:var(--z-dropdown)` to render above Managers card
- **Reporting Quarter dropdown**: Replaced date input with `<select>` — `populateQuarterSelect()` generates 8 most recent quarter-end options, defaults to most recent completed quarter
- **Search typeahead speedup**: Client stale-while-revalidate (80ms debounce, 15min cache); Server parallel search via `ThreadPoolExecutor(max_workers=2)`, 15min cache TTL

### Code Review Bug Fixes (11 fixes)
- **`_load_mf_tickers()` fix**: Startup preload called non-existent function name, silently swallowed
- **`_build_rows()` field alignment**: Removed dead trailing P/E fields, added missing EPS fields to match FIELDNAMES
- **Dividend yield x100**: yfinance returns decimal, code stored without converting
- **`qrColor(0)` neutral gray**: Zero return produced red instead of neutral slate
- **Spotlight cache preserved on close**: Cache no longer wiped on every close
- **`doRun()` error resilience**: Added 3-layer error handling (network, status, JSON parse)
- **SSE reconnection tolerance**: Tolerates up to 3 consecutive errors before closing
- **`switchTab()` double-highlight**: Removed stale event global reference
- **Chart background consistency**: Fixed sunburst + sector-by-manager to use `#0c1220`
- **`doReset()` completeness**: Now clears `last_db_run_id`, `pendingNport`/`pendingType`, hides series picker
- **Treemap ticker regex**: Added `\-` to character class for hyphenated tickers (BF-B)

### Frontend Extraction (Monolith → Static Files) + JS Deduplication
- Extracted inline HTML/CSS/JS from `13F_stocks_app.py` into `static/` files (54% reduction)
- JS dedup: `qrColor()`, `plotTreemap()`, `plotCompBar()` shared helpers

### Scroll Animation Fix
- Root cause: IntersectionObserver used default viewport instead of `.flex-1.overflow-y-auto` container
- Fix: Double-rAF pattern, `resetScrollReveal()`, MutationObserver for new elements

### Executive Summary Slide Fix (PPT/PDF)
- `_add_wrapped_text` robustness: `wrap='square'`, `anchor='t'`
- Increased bullet textbox heights, reduced font size
- Fixed `closing_y` calculation and `exec_data` variable scope

### Dashboard Enhancement: Valuation Charts, Key Takeaways, New Metrics
- Valuation Bubble Scatter: Forward P/E vs EPS Growth, sized by weight, OLS regression line
- Overlap Bubble Matrix: Manager x Stock bubble chart
- Key Takeaways: Data-driven insights above each chart
- Summary Findings Paragraph: Auto-generated portfolio narrative
- 4 new stat boxes: Fwd P/E (harmonic), Fwd EPS Growth, Dividend Yield, Expected Return

### Chart Layout Fixes
- Title/legend overlap: Increased `margin.t`, repositioned legend
- Portfolio bar rendered last (top position in bar group)
- Industry stays portfolio-only (ACWI industry weights not publicly available)

### Geographic Comparison Bar + Category Drill-Down
- `normalize_country_name()` for iShares matching
- `/api/geo-data` endpoint, `renderGeoCompBar()` with Portfolio vs ACWI
- `/api/category-stocks` endpoint: On-demand drill-down with stock table + manager pills
- Click handlers on sector/geo/industry bars → detail cards

### Live Dashboard ACWI Comparison Charts
- 2x2 grid: Sector + Geo (ACWI comparison), Top Holdings + Industry (portfolio focus)
- `Promise.allSettled()` parallel fetch with graceful ACWI fallback

### ACWI Benchmark Comparison Charts
- Pie charts replaced with grouped horizontal bar charts (PPTX + PDF)
- `fetch_acwi_benchmark()`: iShares CSV → yfinance → hardcoded fallback (24hr cache)
- `SECTOR_NAME_MAP` + `normalize_sector_name()` for GICS standard mapping
- PPTX: 5 slides with comparison charts; PDF: 4 comparison charts

### Gebbia Design Overhaul (Joe Gebbia-inspired)
- 60 CSS custom properties: spacing scale, warmer dark palette, typography scale, easing curves
- Glassmorphism: backdrop-blur cards, gradient buttons, hover glow, shimmer skeletons
- Animations: `fadeInUp`, `fadeInScale`, spring easing, stagger delays
- Portfolio Intelligence hero card with narrative headline + animated metrics
- Empty states, skeleton loading, status chips

### Trailing P/E Removal + EPS Fields
- Removed `prior_trailing_pe` / `filing_trailing_pe` across all files
- Added `trailing_eps` / `forward_eps` across full pipeline (with DB migration)

### Search Typeahead Performance
- Triple-layer caching: server search cache, EDGAR form cache, client JS cache
- Startup pre-load of SEC mutual fund tickers (28K+ funds)

### GQRIX / Mutual Fund Fetch Fixes
- OpenFIGI batch size: 100 → 50 (avoid HTTP 413)
- Windows encoding fix for SEC tickers print

### Ticker Resolution Overhaul
- CUSIP_TO_TICKER expanded to 120+ entries
- Fixed ALTM→MO (Altria)
- SEC `company_tickers.json` fallback after OpenFIGI
- OpenFIGI retry logic (2 attempts, 5s delay, 20s timeout)

### Mutual Fund Search by Name/Ticker
- `_load_mf_tickers()`: Downloads/caches SEC fund list, indexes by ticker and CIK
- `_search_mutual_funds()`: 3-tier search (ticker → EFTS name → EDGAR company)

### UI/UX Overhaul
- Unified search bar (13F + N-PORT), inline series picker for funds
- Manager weight bar with color coding, Clear All button
- Summary dashboard with donut charts, stock name shortening, quarter labels
- All treemaps maxdepth:3 for stock-level drill-down

### SQLite Historical Tracking (Portfolio Time Machine)
- `db.py`: `runs` + `holdings` tables, auto-save every fetch, auto-load latest on startup
- History panel: browse/load/label/delete past runs
- Instant QoQ compare between any two stored runs

### OpenFIGI CUSIP Resolution
- `openfigi_lookup()` batch function with in-memory cache
- Resolution: filing data → CUSIP dict → OpenFIGI API

### FMP ESG Integration
- 4 fields: `esg_score`, `esg_environmental`, `esg_social`, `esg_governance`
- FMP API key in config (Settings card), avg ESG on summary dashboard

### Stock Drill-Down Panel
- Click any ticker → slide-out panel (440px) with sparkline, stats, manager table, weight history
- 7 click targets: heatmap, treemaps, overlap table, spotlight, top stocks bar

### Stock Spotlight Search (Ctrl+K)
- Cross-manager stock search overlay, grouped by stock, sorted by manager count
- Data cached from `/api/treemap-data`, invalidated on reset

### Finviz-Style Portfolio Heatmap Tab
- CSS grid, stocks sized by weight, colored by quarterly return (HSL interpolation)
- Per-manager sections with colored dot headers

### Glass UI Overhaul + Skeleton Loading
- Full glassmorphism redesign: cards, stat boxes, buttons, tabs, header, toast
- Shimmer skeleton loading, animated stat counters, stagger-fade manager cards

### Standalone Add Button + Weighted Portfolio Excel
- Green "+ Add" button with pulse animation
- `write_weighted_xlsx()`: 32 columns including ESG
