"""
Investment Manager Holdings Fetcher — SEC 13F + N-PORT (edgartools edition)
===========================================================================
Uses the free `edgartools` library (no API key, no rate limits).

Outputs:
  - One Excel per manager: <name>_top20_<asofdate>.xlsx
  - One combined Excel:    all_managers_top20_<rundate>.xlsx
  - Multi-slide PPTX report with treemap

Install:  pip install edgartools openpyxl python-pptx matplotlib
"""

import csv
import io
import json as _json
import os
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

import math
from edgar import set_identity, Company
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from pptx import Presentation as PptxPresentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

# ── Config ────────────────────────────────────────────────────────────────────

set_identity("Investment Manager Holdings Fetcher holdings@example.com")

MAX_DATE = "2025-12-31"
TOP_N    = 20

def _max_filing_date():
    """Compute filing date cutoff: MAX_DATE + 75 days.
    13F/NPORT filings are due ~45 days after quarter end.
    If user sets MAX_DATE=2025-12-31 (meaning Q4 2025),
    we search for filings filed through ~mid-March 2026."""
    from datetime import datetime, timedelta
    try:
        dt = datetime.strptime(MAX_DATE, "%Y-%m-%d")
        return (dt + timedelta(days=75)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return MAX_DATE

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

MANAGERS_13F = {
    "Baupost Group":       "1061768",
    "Elliott Management":  "1791786",
    "Durable Capital":     "1798849",
    "Tiger Global":        "1167483",
    "Aspex Management":    "1768375",
    "D1 Capital":          "1747057",
    "Viking Global":       "1103804",
    "Arrowstreet Capital": "1164508",
    "CC&L Q ACWI":         "1596800",
    "Spyglass Capital":    "1654344",
    "Wellington Mgmt":     "902219",
}

MANAGERS_NPORT = {}

# ── Ticker aliases — maps old/wrong tickers to current ones ──────────────────
_TICKER_ALIASES = {
    "FB": "META", "ANTM": "ELV", "TWTR": "X", "DISCA": "WBD",
    "DISCK": "WBD", "VIAC": "PARA", "VIACA": "PARA", "KSU": "CP",
}

def _apply_ticker_aliases(records):
    """Fix outdated tickers (e.g. FB→META) in a list of holding dicts."""
    for rec in records:
        t = (rec.get("ticker") or "").upper()
        if t in _TICKER_ALIASES:
            rec["ticker"] = _TICKER_ALIASES[t]

# ── CUSIP → Ticker fallback (for SPDR ETFs that omit tickers) ───────────────

CUSIP_TO_TICKER = {
    # ── Mega-cap tech ────────────────────────────────────────────────────────
    "594918104": "MSFT",  "037833100": "AAPL",  "67066G104": "NVDA",
    "02079K305": "GOOGL", "02079K107": "GOOG",  "023135106": "AMZN",
    "30303M102": "META",  "11135F101": "AVGO",  "882508104": "TSLA",
    "594918AD2": "MSFT",  "00507V109": "ADBE",  "46120E602": "INTU",
    "79466L302": "CRM",   "568259108": "MRVL",  "007903107": "AMD",
    "458140100": "INTC",  "457030104": "IBM",   "68389X105": "ORCL",
    "17275R102": "CSCO",  "87612E106": "TMUS",  "20030N101": "CMCSA",
    "92826C839": "V",     "571748102": "MA",    "585055106": "MELI",
    "00724F101": "ADSK",  "74762E102": "QCOM",  "872540109": "TXN",
    "98138H101": "WM",    "04271T100": "ANET",  "29786A106": "EQIX",
    # ── Financials ───────────────────────────────────────────────────────────
    "46625H100": "JPM",   "46625H218": "JPM",   "060505104": "BAC",
    "172967424": "C",     "949746101": "WFC",   "38141G104": "GS",
    "58933Y105": "MS",    "09247X101": "BLK",   "78462F103": "SPGI",
    "55354G100": "MSCI",  "629491105": "NDAQ",  "075887109": "BDX",
    "808513105": "SCHW",  "14913Q104": "CB",    "29364G103": "ENB",
    "053015103": "AXP",   "459200101": "IBM",   "256135203": "DFS",
    "635405101": "NEE",   "29250N105": "ENB",   "742718109": "PG",
    # ── Healthcare ───────────────────────────────────────────────────────────
    "478160104": "JNJ",   "91324P102": "UNH",
    "69608A108": "PFE",   "002824100": "ABBV",  "532457108": "LLY",
    "88160R101": "TMO",   "718172109": "PM",
    "57636Q104": "MRK",   "053332102": "AZN",   "000360206": "ABT",
    "09062X103": "BIIB",  "92532F100": "VRTX",  "125523100": "CI",
    "004489403": "ACE",   "00846U101": "GILD",  "464287457": "ISRG",
    "94974B100": "WELL",  "75886F107": "REGN",  "126650100": "CVS",
    # ── Consumer / Retail ────────────────────────────────────────────────────
    "931142103": "WMT",   "22160K105": "COST",  "693475105": "PEP",
    "172062101": "KO",    "552953101": "MO",    "02209S103": "MO",
    "761152107": "RES",   "500754106": "KR",
    "866743105": "SBUX",  "580135101": "MCD",   "902973304": "TGT",
    "037833108": "AAPL",  "913017109": "UPS",   "35137L105": "FDX",
    # ── Energy ───────────────────────────────────────────────────────────────
    "30231G102": "XOM",   "166764100": "CVX",   "192446102": "COP",
    "81369Y506": "SLB",   "302445101": "EOG",   "718507105": "PSX",
    "59001A102": "MPC",   "91911K102": "VLO",   "670346105": "OXY",
    "35671D857": "FANG",  "845467105": "SPH",
    # ── Industrials ──────────────────────────────────────────────────────────
    "443185106": "HON",   "149123101": "CAT",   "369604301": "GE",
    "20825C104": "COP",   "091166105": "BA",    "032511107": "AMGN",
    "912456100": "RTX",   "26441C204": "DUK",
    "025537101": "AEP",   "842587107": "SO",
    "524651106": "LECO",  "370334104": "GD",    "539830109": "LMT",
    # ── Telecom / Utilities / Insurance ──────────────────────────────────────
    "00206R102": "T",     "92343V104": "VZ",    "843034101": "NEE",
    "026874784": "AIG",   "743315103": "PGR",   "59156R108": "MET",
    "902494103": "TFC",
    "607059104": "MMM",   "88579Y101": "MMC",   "29089Q105": "EMR",
    "404280109": "HSIC",  "11133T103": "BMY",
    # ── Other large-cap ──────────────────────────────────────────────────────
    "025816109": "AMGN",  "624756102": "MUFG",
    "98978V103": "ZTS",   "904764200": "UBER",  "90384S303": "ULTA",
    "89832Q109": "TSCO",  "828806109": "SHW",   "48203R104": "JNPR",
    "68902V107": "OTIS",  "855244109": "SBAC",  "31428X106": "FE",
}

# ── OpenFIGI CUSIP → Ticker resolution ───────────────────────────────────────

_figi_cache = {}
_figi_cache_lock = threading.Lock()


def openfigi_lookup(cusips):
    """
    Batch lookup CUSIPs via OpenFIGI API (free, no key needed).
    Anonymous API: 20 req/min, 100 items per request.
    Returns dict mapping CUSIP -> ticker (or 'N/A' if not found).
    """
    results = {}

    # Check cache first
    uncached = []
    with _figi_cache_lock:
        for cusip in cusips:
            if cusip in _figi_cache:
                results[cusip] = _figi_cache[cusip]
            else:
                uncached.append(cusip)

    if not uncached:
        return results

    # Batch into groups of 50 (API limit is 100, but smaller batches avoid 413 errors)
    for i in range(0, len(uncached), 50):
        batch = uncached[i:i + 50]
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]
        for attempt in range(2):  # try twice
            try:
                req = urllib.request.Request(
                    "https://api.openfigi.com/v3/mapping",
                    data=_json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=20) as resp:
                    data = _json.loads(resp.read().decode("utf-8"))

                for j, item in enumerate(data):
                    cusip = batch[j]
                    ticker = "N/A"
                    if "data" in item and item["data"]:
                        for d in item["data"]:
                            t = d.get("ticker", "")
                            if t and t not in ("", "N/A"):
                                ticker = t
                                break
                    results[cusip] = ticker
                    with _figi_cache_lock:
                        _figi_cache[cusip] = ticker
                break  # success — exit retry loop
            except Exception as e:
                if attempt == 0:
                    print(f"[OpenFIGI] Batch {i//100+1} failed: {e}, retrying in 5s...")
                    time.sleep(5)
                else:
                    print(f"[OpenFIGI] Batch {i//100+1} failed on retry: {e} — marking {len(batch)} CUSIPs as N/A")
                    for cusip in batch:
                        results[cusip] = "N/A"

        # Rate limit: ~20 req/min = 1 every 3 seconds
        if i + 50 < len(uncached):
            time.sleep(3)

    return results


# ── SEC company_tickers.json name→ticker fallback ─────────────────────────────

_sec_tickers_cache = None
_sec_tickers_lock = threading.Lock()


def _load_sec_tickers():
    """Download SEC company_tickers.json and build normalized name→ticker map."""
    global _sec_tickers_cache
    if _sec_tickers_cache is not None:
        return
    try:
        req = urllib.request.Request(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "13F-App/1.0 holdings@example.com"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        name_map = {}
        for item in data.values():
            norm = item["title"].upper().strip()
            tk = item["ticker"].upper().strip()
            if norm and tk:
                name_map[norm] = tk
        _sec_tickers_cache = name_map
        print(f"[SEC Tickers] Loaded {len(name_map):,} company name→ticker mappings")
    except Exception as e:
        try:
            print(f"[SEC Tickers] Failed to load: {e}")
        except (UnicodeEncodeError, OSError):
            print("[SEC Tickers] Failed to load (encoding error in message)")
        _sec_tickers_cache = {}


def _sec_name_to_ticker(name):
    """Lookup ticker by company name using SEC company_tickers.json."""
    with _sec_tickers_lock:
        _load_sec_tickers()
    if not _sec_tickers_cache or not name:
        return None
    norm = name.upper().strip()
    # Exact match
    if norm in _sec_tickers_cache:
        return _sec_tickers_cache[norm]
    # Try stripping common suffixes
    for suffix in [" INC.", " INC", " CORP.", " CORP", " CORPORATION",
                   " CO.", " CO", " LTD.", " LTD", " PLC", " GROUP",
                   " HOLDINGS", " LP", " N.V.", " SA", " AG", " SE",
                   " & CO.", " & CO", " INTERNATIONAL", " INTL"]:
        stripped = norm.replace(suffix, "").strip()
        if stripped and stripped in _sec_tickers_cache:
            return _sec_tickers_cache[stripped]
    # Try removing "THE " prefix
    if norm.startswith("THE "):
        no_the = norm[4:]
        if no_the in _sec_tickers_cache:
            return _sec_tickers_cache[no_the]
        for suffix in [" INC.", " INC", " CORP.", " CORP", " CORPORATION",
                       " CO.", " CO", " GROUP", " HOLDINGS"]:
            stripped = no_the.replace(suffix, "").strip()
            if stripped and stripped in _sec_tickers_cache:
                return _sec_tickers_cache[stripped]
    return None


def _resolve_remaining_tickers(holdings_list, name_key="name"):
    """Final fallback: resolve any remaining N/A tickers via SEC name lookup."""
    count = 0
    for rec in holdings_list:
        if rec.get("ticker", "N/A") == "N/A":
            found = _sec_name_to_ticker(rec.get(name_key, ""))
            if found:
                rec["ticker"] = found
                count += 1
    if count:
        print(f"[SEC Tickers] Resolved {count} tickers via company name lookup")


# ── Fieldnames ───────────────────────────────────────────────────────────────

FIELDNAMES = [
    # Base fields (8)
    "manager", "period_of_report", "filed_at", "rank",
    "name", "ticker", "value_usd", "pct_of_portfolio",
    # Prior quarter (6)
    "prior_price_qtr_end", "prior_quarter_return_pct",
    "prior_reported_eps", "prior_consensus_eps",
    "prior_eps_beat_dollars", "prior_eps_beat_pct",
    # Filing quarter (6)
    "filing_price_qtr_end", "filing_quarter_return_pct",
    "filing_reported_eps", "filing_consensus_eps",
    "filing_eps_beat_dollars", "filing_eps_beat_pct",
    # Current / live (5)
    "forward_pe", "forward_eps_growth", "dividend_yield",
    "trailing_eps", "forward_eps",
    # QTD (2)
    "qtd_return_pct", "qtd_price_start",
    # Static (3)
    "sector", "industry", "country",
]

# Original 8 fields (for backward compatibility checks)
BASE_FIELDNAMES = FIELDNAMES[:8]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_name(manager_name):
    return (manager_name.lower()
            .replace(" ", "_").replace("(", "").replace(")", "")
            .replace("/", "").replace("&", "and"))


def _has_enrichment(rows):
    """Check if rows contain enrichment data (any non-None enrichment field)."""
    for r in rows:
        if any(r.get(f) is not None for f in FIELDNAMES[8:]):
            return True
    return False


def _format_xlsx_sheet(ws, rows):
    """Apply professional formatting to a worksheet with holdings data."""
    enriched = _has_enrichment(rows)

    if enriched:
        HEADERS = [
            "Manager", "Period of Report", "Filed At", "Rank",
            "Name", "Ticker", "Value (USD $000s)", "% of Portfolio",
            # Prior quarter
            "Prior Qtr Price", "Prior Qtr Return %",
            "Prior Reported EPS", "Prior Consensus EPS",
            "Prior EPS Beat ($)", "Prior EPS Beat (%)",
            # Filing quarter
            "Filing Qtr Price", "Filing Qtr Return %",
            "Filing Reported EPS", "Filing Consensus EPS",
            "Filing EPS Beat ($)", "Filing EPS Beat (%)",
            # Current / live
            "Fwd P/E (Curr)", "Fwd EPS Growth", "Div Yield",
            "Trail 4Q EPS", "Fwd 12M EPS",
            # QTD
            "QTD Return %",
            # Static
            "Sector", "Industry", "Country",
        ]
    else:
        HEADERS = [
            "Manager", "Period of Report", "Filed At", "Rank",
            "Name", "Ticker", "Value (USD $000s)", "% of Portfolio",
        ]

    num_cols = len(HEADERS)

    hdr_font = Font(name="Arial", bold=True, size=11, color="FFFFFF")
    hdr_fill = PatternFill("solid", fgColor="2F5496")
    hdr_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    txt_font = Font(name="Arial", size=10)
    num_font = Font(name="Arial", size=10)
    green_font = Font(name="Arial", size=10, color="227722")
    red_font = Font(name="Arial", size=10, color="CC2222")
    thin_border = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"))

    # Headers
    for c, h in enumerate(HEADERS, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = hdr_align
        cell.border = thin_border

    # Column indices (1-based) for enriched mode
    # 1-8: base, 9-14: prior qtr, 15-20: filing qtr, 21-25: live, 26: QTD, 27-29: static
    COL_PRIOR_PRICE, COL_PRIOR_RET = 9, 10
    COL_PRIOR_EPS_R, COL_PRIOR_EPS_C = 11, 12
    COL_PRIOR_BEAT_D, COL_PRIOR_BEAT_P = 13, 14
    COL_FILING_PRICE, COL_FILING_RET = 15, 16
    COL_FILING_EPS_R, COL_FILING_EPS_C = 17, 18
    COL_FILING_BEAT_D, COL_FILING_BEAT_P = 19, 20
    COL_FWD_PE, COL_FWD_GROWTH, COL_DIV_YIELD = 21, 22, 23
    COL_TRAIL_EPS, COL_FWD_EPS = 24, 25
    COL_QTD_RET = 26
    COL_SECTOR, COL_INDUSTRY, COL_COUNTRY = 27, 28, 29

    PRICE_COLS = {COL_PRIOR_PRICE, COL_FILING_PRICE}
    RETURN_COLS = {COL_PRIOR_RET, COL_FILING_RET, COL_QTD_RET}
    PE_COLS = {COL_FWD_PE}
    EPS_COLS = {COL_PRIOR_EPS_R, COL_PRIOR_EPS_C, COL_FILING_EPS_R, COL_FILING_EPS_C,
                COL_TRAIL_EPS, COL_FWD_EPS}
    BEAT_D_COLS = {COL_PRIOR_BEAT_D, COL_FILING_BEAT_D}
    BEAT_P_COLS = {COL_PRIOR_BEAT_P, COL_FILING_BEAT_P}
    PCT_COLS = {COL_FWD_GROWTH, COL_DIV_YIELD}
    TEXT_COLS_ENRICHED = {COL_SECTOR, COL_INDUSTRY, COL_COUNTRY}

    # Data rows
    for r_idx, row in enumerate(rows, 2):
        vals = [
            row["manager"],
            row["period_of_report"],
            row["filed_at"],
            int(row["rank"]),
            row["name"],
            row["ticker"],
            int(round(row["value_usd"] / 1000)),
            row["pct_of_portfolio"] / 100,
        ]
        if enriched:
            # Prior quarter
            prior_ret = row.get("prior_quarter_return_pct")
            vals.extend([
                row.get("prior_price_qtr_end"),
                prior_ret / 100 if prior_ret is not None else None,
                row.get("prior_reported_eps"),
                row.get("prior_consensus_eps"),
                row.get("prior_eps_beat_dollars"),
                row.get("prior_eps_beat_pct"),
            ])
            # Filing quarter
            filing_ret = row.get("filing_quarter_return_pct")
            vals.extend([
                row.get("filing_price_qtr_end"),
                filing_ret / 100 if filing_ret is not None else None,
                row.get("filing_reported_eps"),
                row.get("filing_consensus_eps"),
                row.get("filing_eps_beat_dollars"),
                row.get("filing_eps_beat_pct"),
            ])
            # Current / live
            fwd_growth = row.get("forward_eps_growth")
            div_yield = row.get("dividend_yield")
            vals.extend([
                row.get("forward_pe"),
                fwd_growth / 100 if fwd_growth is not None else None,
                div_yield / 100 if div_yield is not None else None,
                row.get("trailing_eps"),
                row.get("forward_eps"),
            ])
            # QTD
            qtd_ret = row.get("qtd_return_pct")
            vals.append(qtd_ret / 100 if qtd_ret is not None else None)
            # Static
            vals.extend([
                row.get("sector") or "",
                row.get("industry") or "",
                row.get("country") or "",
            ])

        for c_idx, val in enumerate(vals, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            cell.border = thin_border

            # Base text columns (A-C, E-F)
            if c_idx in (1, 2, 3, 5, 6):
                cell.font = txt_font
                cell.number_format = "@"
            # Rank (D)
            elif c_idx == 4:
                cell.font = num_font
                cell.number_format = "0"
                cell.alignment = Alignment(horizontal="center")
            # Value (G)
            elif c_idx == 7:
                cell.font = num_font
                cell.number_format = "#,##0"
            # Pct of portfolio (H)
            elif c_idx == 8:
                cell.font = num_font
                cell.number_format = "0.00%"
            elif not enriched:
                continue
            # Price columns
            elif c_idx in PRICE_COLS:
                cell.font = num_font
                cell.number_format = "$#,##0.00"
            # Return columns — percentage with color
            elif c_idx in RETURN_COLS:
                if val is not None:
                    cell.font = green_font if val >= 0 else red_font
                    cell.number_format = "0.00%"
                else:
                    cell.font = txt_font
            # P/E columns — 0.0x format
            elif c_idx in PE_COLS:
                cell.font = num_font
                cell.number_format = '0.0"x"'
            # EPS columns
            elif c_idx in EPS_COLS:
                cell.font = num_font
                cell.number_format = "0.00"
            # EPS Beat $ columns — green/red
            elif c_idx in BEAT_D_COLS:
                if val is not None:
                    cell.font = green_font if val >= 0 else red_font
                    cell.number_format = '+0.00;-0.00'
                else:
                    cell.font = txt_font
            # EPS Beat % columns — green/red
            elif c_idx in BEAT_P_COLS:
                if val is not None:
                    cell.font = green_font if val >= 0 else red_font
                    cell.number_format = '+0.0%;-0.0%'
                else:
                    cell.font = txt_font
            # Growth / Yield columns
            elif c_idx in PCT_COLS:
                cell.font = num_font
                cell.number_format = "0.0%"
            # Sector, Industry
            elif c_idx in TEXT_COLS_ENRICHED:
                cell.font = txt_font

    # Alternating row fill
    alt_fill = PatternFill("solid", fgColor="F2F7FB")
    for r_idx in range(3, len(rows) + 2, 2):
        for c_idx in range(1, num_cols + 1):
            ws.cell(row=r_idx, column=c_idx).fill = alt_fill

    # Column widths
    if enriched:
        widths = [
            22, 16, 14, 6, 30, 8, 18, 14,      # base (8)
            12, 12, 11, 12, 12, 12, 12,          # prior qtr (7)
            12, 12, 11, 12, 12, 12, 12,          # filing qtr (7)
            11, 12, 10,                           # live (3)
            18, 22, 18,                           # static (3)
        ]
    else:
        widths = [22, 16, 14, 6, 30, 8, 18, 14]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # Freeze top row
    ws.freeze_panes = "A2"

    # Auto-filter
    last_col = get_column_letter(num_cols)
    ws.auto_filter.ref = f"A1:{last_col}{len(rows) + 1}"


def _eps_beat_label(beat_dollars, beat_pct=None):
    """Format EPS beat as '+$0.12 (+8.5%)' or 'N/A'."""
    if beat_dollars is None:
        return "N/A"
    sign = "+" if beat_dollars >= 0 else ""
    dollar_part = f"{sign}${beat_dollars:.2f}"
    if beat_pct is not None:
        pct_part = f"({sign}{beat_pct:.1f}%)"
        return f"{dollar_part} {pct_part}"
    return dollar_part


def write_individual_xlsx(manager_name, period, rows):
    """Write a per-manager Excel file and return the filename."""
    safe = _safe_name(manager_name)
    period_str = period.replace("-", "")
    filename = f"{safe}_top20_{period_str}.xlsx"
    path = os.path.join(OUTPUT_DIR, filename)
    wb = Workbook()
    ws = wb.active
    ws.title = manager_name[:31]
    _format_xlsx_sheet(ws, rows)
    wb.save(path)
    return filename


def write_combined_xlsx(all_rows, run_date):
    """Write combined Excel for all managers and return the filename."""
    filename = f"all_managers_top20_{run_date}.xlsx"
    path = os.path.join(OUTPUT_DIR, filename)
    wb = Workbook()
    ws = wb.active
    ws.title = "All Managers"
    _format_xlsx_sheet(ws, all_rows)
    wb.save(path)
    return filename


def write_weighted_xlsx(all_rows, run_date, manager_weights=None):
    """
    Write a weighted combined portfolio Excel that deduplicates stocks across
    managers and weights them by the user's manager allocations.

    Returns the filename.
    """
    import analysis

    weighted_rows, _ = analysis._apply_manager_weights(all_rows, manager_weights)

    # Aggregate by ticker
    by_ticker = {}
    for r in weighted_rows:
        tk = r.get("ticker", "N/A")
        key = tk if tk != "N/A" else r.get("name", "Unknown")
        if key not in by_ticker:
            by_ticker[key] = {
                "ticker": r.get("ticker", "N/A"),
                "name": r.get("name", "Unknown"),
                "combined_weight": 0.0,
                "total_value": 0,
                "managers": set(),
                # Enrichment fields (per-ticker, take from first non-None)
                "sector": None,
                "industry": None,
                "country": None,
                "prior_quarter_return_pct": None,
                "filing_quarter_return_pct": None,
                "forward_pe": None,
                "forward_eps_growth": None,
                "dividend_yield": None,
                "trailing_eps": None,
                "forward_eps": None,
                "qtd_return_pct": None,
                "prior_price_qtr_end": None,
                "filing_price_qtr_end": None,
                "prior_reported_eps": None,
                "prior_consensus_eps": None,
                "prior_eps_beat_dollars": None,
                "prior_eps_beat_pct": None,
                "filing_reported_eps": None,
                "filing_consensus_eps": None,
                "filing_eps_beat_dollars": None,
                "filing_eps_beat_pct": None,
            }
        d = by_ticker[key]
        d["combined_weight"] += r.get("combined_weight", 0)
        d["total_value"] += r.get("value_usd", 0)
        d["managers"].add(r["manager"])
        # Fill enrichment from first row that has data (all same per ticker)
        for field in [
            "sector", "industry", "country",
            "prior_quarter_return_pct", "filing_quarter_return_pct",
            "forward_pe", "forward_eps_growth", "dividend_yield",
            "trailing_eps", "forward_eps", "qtd_return_pct",
            "prior_price_qtr_end", "filing_price_qtr_end",
            "prior_reported_eps", "prior_consensus_eps",
            "prior_eps_beat_dollars", "prior_eps_beat_pct",
            "filing_reported_eps", "filing_consensus_eps",
            "filing_eps_beat_dollars", "filing_eps_beat_pct",
        ]:
            if d[field] is None and r.get(field) is not None:
                d[field] = r[field]

    # Sort by combined weight descending
    sorted_stocks = sorted(by_ticker.values(), key=lambda x: -x["combined_weight"])

    # Build workbook
    filename = f"weighted_portfolio_{run_date}.xlsx"
    path = os.path.join(OUTPUT_DIR, filename)
    wb = Workbook()
    ws = wb.active
    ws.title = "Weighted Portfolio"

    enriched = any(s["sector"] is not None or s["industry"] is not None for s in sorted_stocks)

    if enriched:
        HEADERS = [
            "Rank", "Ticker", "Name", "Sector", "Industry", "Country",
            "Wtd Port %", "# Managers", "Managers", "Total Value ($000s)",
            "Prior Qtr Price", "Prior Qtr Return %",
            "Prior Reported EPS", "Prior Consensus EPS",
            "Prior EPS Beat ($)", "Prior EPS Beat (%)",
            "Filing Qtr Price", "Filing Qtr Return %",
            "Filing Reported EPS", "Filing Consensus EPS",
            "Filing EPS Beat ($)", "Filing EPS Beat (%)",
            "Fwd P/E", "Fwd EPS Growth %", "Div Yield %",
            "Trail 4Q EPS", "Fwd 12M EPS",
            "QTD Return %",
        ]
    else:
        HEADERS = [
            "Rank", "Ticker", "Name",
            "Wtd Port %", "# Managers", "Managers", "Total Value ($000s)",
        ]

    num_cols = len(HEADERS)

    # Styles
    hdr_font = Font(name="Arial", bold=True, size=11, color="FFFFFF")
    hdr_fill = PatternFill("solid", fgColor="2F5496")
    hdr_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    txt_font = Font(name="Arial", size=10)
    num_font = Font(name="Arial", size=10)
    green_font = Font(name="Arial", size=10, color="227722")
    red_font = Font(name="Arial", size=10, color="CC2222")
    thin_border = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"))

    # Write headers
    for c, h in enumerate(HEADERS, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = hdr_align
        cell.border = thin_border

    # Write data rows
    for r_idx, s in enumerate(sorted_stocks, 2):
        rank = r_idx - 1
        mgr_list = ", ".join(sorted(s["managers"]))
        mgr_count = len(s["managers"])
        wt_pct = s["combined_weight"] / 100  # as decimal for Excel % format

        if enriched:
            prior_ret = s["prior_quarter_return_pct"]
            filing_ret = s["filing_quarter_return_pct"]
            fwd_growth = s["forward_eps_growth"]
            div_yield = s["dividend_yield"]
            qtd_ret = s.get("qtd_return_pct")
            vals = [
                rank, s["ticker"], s["name"],
                s["sector"] or "", s["industry"] or "", s["country"] or "",
                wt_pct, mgr_count, mgr_list,
                int(round(s["total_value"] / 1000)) if s["total_value"] else 0,
                s["prior_price_qtr_end"],
                prior_ret / 100 if prior_ret is not None else None,
                s["prior_reported_eps"], s["prior_consensus_eps"],
                s["prior_eps_beat_dollars"], s["prior_eps_beat_pct"],
                s["filing_price_qtr_end"],
                filing_ret / 100 if filing_ret is not None else None,
                s["filing_reported_eps"], s["filing_consensus_eps"],
                s["filing_eps_beat_dollars"], s["filing_eps_beat_pct"],
                s["forward_pe"],
                fwd_growth / 100 if fwd_growth is not None else None,
                div_yield / 100 if div_yield is not None else None,
                s["trailing_eps"],
                s["forward_eps"],
                qtd_ret / 100 if qtd_ret is not None else None,
            ]
        else:
            vals = [
                rank, s["ticker"], s["name"],
                wt_pct, mgr_count, mgr_list,
                int(round(s["total_value"] / 1000)) if s["total_value"] else 0,
            ]

        for c_idx, val in enumerate(vals, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            cell.border = thin_border

            if enriched:
                # Column mapping for enriched mode:
                # 1=Rank, 2=Ticker, 3=Name, 4=Sector, 5=Industry, 6=Country,
                # 7=Wtd%, 8=#Mgrs, 9=Managers, 10=Value,
                # 11=PriorPrice, 12=PriorRet,
                # 13=PriorEPSR, 14=PriorEPSC, 15=PriorBeat$, 16=PriorBeat%,
                # 17=FilingPrice, 18=FilingRet,
                # 19=FilingEPSR, 20=FilingEPSC, 21=FilingBeat$, 22=FilingBeat%,
                # 23=FwdPE, 24=FwdGrowth, 25=DivYield, 26=TrailEPS, 27=FwdEPS,
                # 28=QTDRet
                if c_idx == 1:  # Rank
                    cell.font = num_font
                    cell.number_format = "0"
                    cell.alignment = Alignment(horizontal="center")
                elif c_idx in (2, 3, 4, 5, 6, 9):  # Text cols
                    cell.font = txt_font
                elif c_idx == 7:  # Wtd Port %
                    cell.font = Font(name="Arial", size=10, bold=True)
                    cell.number_format = "0.00%"
                elif c_idx == 8:  # # Managers
                    cell.font = num_font
                    cell.number_format = "0"
                    cell.alignment = Alignment(horizontal="center")
                elif c_idx == 10:  # Total Value
                    cell.font = num_font
                    cell.number_format = "#,##0"
                elif c_idx in (11, 17):  # Price cols
                    cell.font = num_font
                    cell.number_format = "$#,##0.00"
                elif c_idx in (12, 18, 28):  # Return % cols (prior, filing, QTD)
                    if val is not None:
                        cell.font = green_font if val >= 0 else red_font
                        cell.number_format = "0.00%"
                    else:
                        cell.font = txt_font
                elif c_idx == 23:  # Fwd P/E
                    cell.font = num_font
                    cell.number_format = '0.0"x"'
                elif c_idx in (13, 14, 19, 20, 26, 27):  # EPS cols (incl. Trail/Fwd EPS)
                    cell.font = num_font
                    cell.number_format = "0.00"
                elif c_idx in (15, 21):  # Beat $ cols
                    if val is not None:
                        cell.font = green_font if val >= 0 else red_font
                        cell.number_format = '+0.00;-0.00'
                    else:
                        cell.font = txt_font
                elif c_idx in (16, 22):  # Beat % cols
                    if val is not None:
                        cell.font = green_font if val >= 0 else red_font
                        cell.number_format = '+0.0%;-0.0%'
                    else:
                        cell.font = txt_font
                elif c_idx in (24, 25):  # Growth / Yield
                    cell.font = num_font
                    cell.number_format = "0.0%"
            else:
                # Non-enriched column mapping:
                # 1=Rank, 2=Ticker, 3=Name, 4=Wtd%, 5=#Mgrs, 6=Managers, 7=Value
                if c_idx == 1:
                    cell.font = num_font
                    cell.number_format = "0"
                    cell.alignment = Alignment(horizontal="center")
                elif c_idx in (2, 3, 6):
                    cell.font = txt_font
                elif c_idx == 4:
                    cell.font = Font(name="Arial", size=10, bold=True)
                    cell.number_format = "0.00%"
                elif c_idx == 5:
                    cell.font = num_font
                    cell.number_format = "0"
                    cell.alignment = Alignment(horizontal="center")
                elif c_idx == 7:
                    cell.font = num_font
                    cell.number_format = "#,##0"

    # Alternating row fill
    alt_fill = PatternFill("solid", fgColor="F2F7FB")
    for r_idx in range(3, len(sorted_stocks) + 2, 2):
        for c_idx in range(1, num_cols + 1):
            ws.cell(row=r_idx, column=c_idx).fill = alt_fill

    # Column widths
    if enriched:
        widths = [
            6, 8, 30, 18, 22, 18,                    # Rank..Country
            12, 10, 40, 18,                           # Wtd%..Value
            12, 12, 11, 12, 12, 12, 12,              # Prior qtr (7)
            12, 12, 11, 12, 12, 12, 12,              # Filing qtr (7)
            11, 12, 10,                               # Fwd PE, Growth, Yield
        ]
    else:
        widths = [6, 8, 30, 12, 10, 40, 18]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # Freeze header + auto-filter
    ws.freeze_panes = "A2"
    last_col = get_column_letter(num_cols)
    ws.auto_filter.ref = f"A1:{last_col}{len(sorted_stocks) + 1}"

    wb.save(path)
    return filename


# ── PPTX Report (multi-slide) ───────────────────────────────────────────────

TREEMAP_COLORS = [
    (0x1E, 0x3A, 0x5F), (0x3B, 0x1F, 0x5E), (0x5E, 0x1E, 0x3A),
    (0x5E, 0x4A, 0x1E), (0x1E, 0x5E, 0x3A), (0x5E, 0x1E, 0x1E),
    (0x1E, 0x4A, 0x5E), (0x3A, 0x5E, 0x1E), (0x5E, 0x3A, 0x1E),
    (0x2E, 0x1E, 0x5E), (0x1E, 0x5E, 0x4A), (0x5E, 0x1E, 0x4A),
    (0x1E, 0x5E, 0x5E), (0x4A, 0x5E, 0x1E), (0x42, 0x2C, 0x50),
    (0x2C, 0x50, 0x42), (0x50, 0x2C, 0x36), (0x36, 0x50, 0x2C),
]

BG_COLOR = RGBColor(0x0F, 0x17, 0x2A)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LIGHT_GRAY = RGBColor(0xE2, 0xE8, 0xF0)
MUTED = RGBColor(0x94, 0xA3, 0xB8)


def _set_slide_bg(slide):
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = BG_COLOR


def _add_text(slide, x, y, w, h, text, size=12, bold=False, color=WHITE, align=PP_ALIGN.LEFT):
    txb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = txb.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(size)
    p.font.bold = bold
    p.font.color.rgb = color
    p.alignment = align
    return tf


def _add_wrapped_text(slide, x, y, w, h, text, size=12, bold=False, color=WHITE, align=PP_ALIGN.LEFT, line_spacing=1.15):
    """Add a textbox with word-wrap and auto-fit. Returns the text frame."""
    txb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = txb.text_frame
    tf.word_wrap = True
    # Auto-shrink text to fit box
    bodyPr = tf._txBody.bodyPr
    bodyPr.attrib['wrap'] = 'square'
    # Allow text to overflow bottom rather than clip
    bodyPr.attrib['anchor'] = 't'
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(size)
    p.font.bold = bold
    p.font.color.rgb = color
    p.alignment = align
    from pptx.util import Pt as _Pt
    p.space_after = _Pt(0)
    if line_spacing and line_spacing != 1.0:
        p.line_spacing = line_spacing
    return tf


def _add_multi_paragraph_text(slide, x, y, w, h, paragraphs, default_size=11, default_color=LIGHT_GRAY, align=PP_ALIGN.LEFT, line_spacing=1.2):
    """Add a textbox with multiple paragraphs. Each paragraph is a dict with text, bold, size, color keys (all optional except text)."""
    txb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = txb.text_frame
    tf.word_wrap = True
    for i, para_data in enumerate(paragraphs):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        text = para_data if isinstance(para_data, str) else para_data.get("text", "")
        p.text = text
        p.font.size = Pt(para_data.get("size", default_size) if isinstance(para_data, dict) else default_size)
        p.font.bold = para_data.get("bold", False) if isinstance(para_data, dict) else False
        p.font.color.rgb = para_data.get("color", default_color) if isinstance(para_data, dict) else default_color
        p.alignment = align
        from pptx.util import Pt as _Pt
        p.space_after = _Pt(para_data.get("space_after", 4) if isinstance(para_data, dict) else 4)
        if line_spacing and line_spacing != 1.0:
            p.line_spacing = line_spacing
    return tf


def _build_exec_summary_content(all_rows, acwi_data=None):
    """Build chairman-level executive summary text from portfolio data."""
    import analysis

    stats = analysis.compute_summary_stats(all_rows)
    sector_data = analysis.compute_sector_breakdown(all_rows)
    geo_data = analysis.compute_geo_breakdown(all_rows)
    overlap = analysis.compute_overlap(all_rows)

    mgrs = list({r["manager"] for r in all_rows})
    total_value = stats.get("total_value", 0)
    total_value_str = f"${total_value / 1e9:.1f}B" if total_value >= 1e9 else f"${total_value / 1e6:.0f}M"
    unique = stats.get("unique_stocks", 0)
    n_mgrs = len(mgrs)

    # Period info
    period_str = stats.get("filing_period", "")

    # Top sector
    sectors = sector_data.get("sectors", [])
    top_sector = sectors[0] if sectors else None
    top_sector_name = top_sector["name"] if top_sector else "N/A"
    top_sector_pct = top_sector["pct"] if top_sector else 0

    # Top holding
    top_stocks = stats.get("top_stocks_by_pct", [])
    top_stock = top_stocks[0] if top_stocks else None

    # Geo
    countries = geo_data.get("normalized_countries", [])
    us_pct = next((c["pct"] for c in countries if c["name"] == "United States"), 0)
    intl_pct = round(100 - us_pct, 1) if us_pct else 0

    # Returns
    avg_ret = stats.get("avg_quarter_return")
    weighted_ret = stats.get("weighted_return")
    fwd_pe = stats.get("weighted_forward_pe")
    eps_growth = stats.get("weighted_eps_growth")
    exp_return = stats.get("expected_return")
    eps_beat = stats.get("eps_beat_rate")

    # ACWI comparison
    acwi_sector_overweight = ""
    if acwi_data and top_sector:
        acwi_sectors = acwi_data.get("sectors", {})
        acwi_top_pct = acwi_sectors.get(top_sector_name, 0)
        diff = top_sector_pct - acwi_top_pct
        if abs(diff) >= 1:
            direction = "overweight" if diff > 0 else "underweight"
            acwi_sector_overweight = f" ({diff:+.1f}% vs ACWI — {direction})"

    # Overlap
    overlap_stocks = overlap if isinstance(overlap, list) else []
    n_overlap = len(overlap_stocks)
    overlap_note = ""
    if n_overlap >= 3 and n_mgrs >= 2:
        top_overlap = overlap_stocks[:3]
        tickers = ", ".join(o["ticker"] for o in top_overlap)
        overlap_note = f"{n_overlap} stocks held by multiple managers, led by {tickers}."

    # ── Opening paragraph ──
    opening = (
        f"This report analyzes the combined portfolio of {n_mgrs} institutional manager{'s' if n_mgrs > 1 else ''} "
        f"with aggregate holdings of {total_value_str} across {unique} unique stocks"
        f"{' as of ' + period_str if period_str else ''}. "
        f"The portfolio reflects the collective conviction of leading institutional investors "
        f"and provides insight into sector positioning, geographic exposure, and forward-looking return expectations."
    )

    # ── Key findings (bullet points) ──
    findings = []

    # 1. Concentration & Top Holdings
    if top_stock:
        top5_pct = sum(s["pct"] for s in top_stocks[:5])
        findings.append(
            f"Concentration: Top 5 holdings represent {top5_pct:.1f}% of the portfolio, "
            f"led by {top_stock['ticker']} at {top_stock['pct']:.1f}%."
        )

    # 2. Sector positioning
    if top_sector:
        findings.append(
            f"Sector Positioning: {top_sector_name} dominates at {top_sector_pct:.1f}% of portfolio weight"
            f"{acwi_sector_overweight}."
        )

    # 3. Geographic allocation
    if us_pct > 0:
        findings.append(
            f"Geographic Allocation: {us_pct:.0f}% US / {intl_pct:.0f}% International exposure."
        )

    # 4. Returns
    if weighted_ret is not None:
        ret_str = f"Portfolio Weighted Return: {weighted_ret:+.1f}% over the filing quarter"
        if avg_ret is not None:
            ret_str += f" (equal-weight avg: {avg_ret:+.1f}%)"
        ret_str += "."
        findings.append(ret_str)

    # 5. Valuation & Forward
    if fwd_pe is not None:
        val_str = f"Valuation: Portfolio trades at {fwd_pe:.1f}x forward P/E"
        if eps_growth is not None:
            val_str += f" with {eps_growth:.1f}% consensus EPS growth"
        val_str += "."
        findings.append(val_str)

    # 6. Expected return
    if exp_return is not None:
        findings.append(
            f"Expected Return: {exp_return:.1f}% implied return (EPS growth + dividend yield)."
        )

    # 7. EPS beat rate
    if eps_beat is not None:
        findings.append(
            f"Earnings Quality: {eps_beat:.0f}% of portfolio holdings beat EPS estimates last quarter."
        )

    # 8. Overlap
    if overlap_note:
        findings.append(f"Manager Consensus: {overlap_note}")

    # ── Closing paragraph ──
    closing_parts = []
    if fwd_pe is not None and fwd_pe > 25:
        closing_parts.append("a growth-oriented tilt at premium valuations")
    elif fwd_pe is not None and fwd_pe < 18:
        closing_parts.append("a value-oriented positioning at attractive valuations")
    else:
        closing_parts.append("a balanced valuation profile")

    if us_pct > 75:
        closing_parts.append("a strong US domestic focus")
    elif intl_pct > 40:
        closing_parts.append("meaningful international diversification")

    closing = (
        f"In summary, the combined portfolio reflects {' with '.join(closing_parts)}. "
    )
    if exp_return is not None and exp_return > 0:
        closing += (
            f"The implied expected return of {exp_return:.1f}% suggests a constructive outlook from these institutional managers. "
        )
    closing += "We recommend monitoring sector concentration and overlap positions for risk management."

    return {
        "opening": opening,
        "findings": findings,
        "closing": closing,
        "stats": stats,
    }


def write_report_pptx(all_rows, run_date, client_name="", report_name=""):
    """Create a multi-slide PPTX report. Returns filename or None."""
    by_mgr = defaultdict(list)
    for r in all_rows:
        by_mgr[r["manager"]].append(r)
    mgrs = list(by_mgr.keys())
    if not mgrs:
        return None

    enriched = _has_enrichment(all_rows)

    # Fetch ACWI benchmark data for comparison charts
    acwi_data = None
    if enriched:
        try:
            from financial_data import fetch_acwi_benchmark
            acwi_data = fetch_acwi_benchmark()
        except Exception:
            pass

    prs = PptxPresentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    # ── Slide 1: Title ──
    slide1 = prs.slides.add_slide(prs.slide_layouts[6])
    _set_slide_bg(slide1)

    title_parts = [p for p in [client_name, report_name] if p]
    title_text = " — ".join(title_parts) if title_parts else "Portfolio Holdings Report"

    _add_text(slide1, 1, 2.2, 11.333, 1.2, title_text, size=36, bold=True, align=PP_ALIGN.CENTER)
    _add_text(slide1, 1, 3.5, 11.333, 0.5,
              f"Generated: {run_date[:4]}-{run_date[4:6]}-{run_date[6:]}",
              size=16, color=MUTED, align=PP_ALIGN.CENTER)

    total_value = sum(r.get("value_usd", 0) for r in all_rows)
    unique_tickers = len({r["ticker"] for r in all_rows if r.get("ticker") != "N/A"})
    _add_text(slide1, 1, 4.2, 11.333, 0.5,
              f"{len(mgrs)} Managers  |  {len(all_rows)} Holdings  |  {unique_tickers} Unique Stocks  |  ${total_value/1e9:.1f}B Total Value",
              size=14, color=LIGHT_GRAY, align=PP_ALIGN.CENTER)

    # ── Slide 2: Executive Summary ──
    exec_data = None
    try:
        print("[PPTX] Building executive summary slide...")
        exec_data = _build_exec_summary_content(all_rows, acwi_data)
        slide_exec = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide_exec)

        # Title
        _add_text(slide_exec, 0.5, 0.25, 12.333, 0.7, "Executive Summary",
                  size=28, bold=True, align=PP_ALIGN.CENTER)

        # Accent line under title
        line = slide_exec.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                                           Inches(5.5), Inches(0.85), Inches(2.333), Inches(0.04))
        line.fill.solid()
        line.fill.fore_color.rgb = RGBColor(0x3B, 0x82, 0xF6)
        line.line.fill.background()

        # Opening paragraph
        _add_wrapped_text(slide_exec, 0.8, 1.1, 11.733, 1.0,
                          exec_data["opening"],
                          size=12, color=LIGHT_GRAY, align=PP_ALIGN.LEFT, line_spacing=1.3)

        # Key Findings header
        _add_text(slide_exec, 0.8, 2.0, 5, 0.4, "Key Findings",
                  size=16, bold=True, color=RGBColor(0x3B, 0x82, 0xF6))

        # Bullet findings — split into 2 columns
        findings = exec_data["findings"]
        mid = (len(findings) + 1) // 2
        col1 = findings[:mid]
        col2 = findings[mid:]

        # Left column
        y_pos = 2.45
        for f in col1:
            parts = f.split(": ", 1)
            if len(parts) == 2:
                tf = _add_wrapped_text(slide_exec, 1.0, y_pos, 5.5, 0.6,
                                       "", size=10, color=LIGHT_GRAY, line_spacing=1.2)
                p = tf.paragraphs[0]
                run_bold = p.add_run()
                run_bold.text = "• " + parts[0] + ": "
                run_bold.font.size = Pt(10)
                run_bold.font.bold = True
                run_bold.font.color.rgb = WHITE
                run_rest = p.add_run()
                run_rest.text = parts[1]
                run_rest.font.size = Pt(10)
                run_rest.font.color.rgb = LIGHT_GRAY
            else:
                _add_wrapped_text(slide_exec, 1.0, y_pos, 5.5, 0.6,
                                  f"• {f}", size=10, color=LIGHT_GRAY, line_spacing=1.2)
            y_pos += 0.55

        # Right column
        y_pos_r = 2.45
        for f in col2:
            parts = f.split(": ", 1)
            if len(parts) == 2:
                tf = _add_wrapped_text(slide_exec, 6.8, y_pos_r, 5.5, 0.6,
                                       "", size=10, color=LIGHT_GRAY, line_spacing=1.2)
                p = tf.paragraphs[0]
                run_bold = p.add_run()
                run_bold.text = "• " + parts[0] + ": "
                run_bold.font.size = Pt(10)
                run_bold.font.bold = True
                run_bold.font.color.rgb = WHITE
                run_rest = p.add_run()
                run_rest.text = parts[1]
                run_rest.font.size = Pt(10)
                run_rest.font.color.rgb = LIGHT_GRAY
            else:
                _add_wrapped_text(slide_exec, 6.8, y_pos_r, 5.5, 0.6,
                                  f"• {f}", size=10, color=LIGHT_GRAY, line_spacing=1.2)
            y_pos_r += 0.55

        # Closing paragraph
        closing_y = max(max(y_pos, y_pos_r) + 0.2, 5.5)
        closing_y = min(closing_y, 6.2)
        _add_wrapped_text(slide_exec, 0.8, closing_y, 11.733, 1.0,
                          exec_data["closing"],
                          size=12, color=LIGHT_GRAY, align=PP_ALIGN.LEFT, line_spacing=1.3)
        print("[PPTX] Executive summary slide complete.")
    except Exception as e:
        print(f"[PPTX] Executive summary FAILED: {e}")
        import traceback
        traceback.print_exc()

    # Build insights for chart slides (reuse exec_data from above or re-compute)
    try:
        if exec_data is None:
            exec_data = _build_exec_summary_content(all_rows, acwi_data)
    except Exception:
        exec_data = None
    findings = exec_data["findings"] if exec_data else []

    def _find_insight(keyword):
        """Find a finding bullet that matches a keyword."""
        for f in findings:
            if keyword.lower() in f.lower():
                return f
        return ""

    # ── Slide 3: Portfolio Summary (Sector + Top Holdings vs ACWI) ──
    if enriched:
        slide2 = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide2)
        _add_text(slide2, 0.5, 0.25, 12.333, 0.7, "Portfolio Summary",
                  size=24, bold=True, align=PP_ALIGN.CENTER)

        # Left: Sector comparison bar chart
        try:
            sector_img = _generate_sector_comparison(all_rows, acwi_data)
            if sector_img:
                slide2.shapes.add_picture(sector_img, Inches(0.3), Inches(1.3),
                                          Inches(6.2), Inches(5.6))
        except Exception:
            pass

        # Right: Top Holdings comparison bar chart
        try:
            top_img = _generate_top_holdings_comparison(all_rows, acwi_data)
            if top_img:
                slide2.shapes.add_picture(top_img, Inches(6.8), Inches(1.3),
                                          Inches(6.2), Inches(5.6))
        except Exception:
            pass

        # Key takeaway
        insight_sector = _find_insight("sector")
        insight_conc = _find_insight("concentration")
        takeaway = " | ".join(filter(None, [insight_sector, insight_conc]))
        if takeaway:
            _add_wrapped_text(slide2, 0.5, 0.9, 12.333, 0.35, takeaway,
                              size=10, color=MUTED, align=PP_ALIGN.CENTER, line_spacing=1.1)

    # ── Slide 4: Sector & Geographic Exposure vs ACWI ──
    if enriched:
        slide_sg = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide_sg)
        _add_text(slide_sg, 0.5, 0.25, 12.333, 0.7,
                  "Sector & Geographic Exposure", size=24, bold=True, align=PP_ALIGN.CENTER)

        # Left: Sector comparison
        try:
            sector_img2 = _generate_sector_comparison(all_rows, acwi_data)
            if sector_img2:
                slide_sg.shapes.add_picture(sector_img2, Inches(0.3), Inches(1.3),
                                            Inches(6.2), Inches(5.6))
        except Exception:
            pass

        # Right: Geographic comparison
        try:
            geo_img = _generate_geo_comparison(all_rows, acwi_data)
            if geo_img:
                slide_sg.shapes.add_picture(geo_img, Inches(6.8), Inches(1.3),
                                            Inches(6.2), Inches(5.6))
        except Exception:
            pass

        # Key takeaway
        insight_sec2 = _find_insight("sector")
        insight_geo = _find_insight("geographic")
        takeaway_sg = " | ".join(filter(None, [insight_sec2, insight_geo]))
        if takeaway_sg:
            _add_wrapped_text(slide_sg, 0.5, 0.9, 12.333, 0.35, takeaway_sg,
                              size=10, color=MUTED, align=PP_ALIGN.CENTER, line_spacing=1.1)

    # ── Slide 5: Industry Breakdown (Portfolio only) ──
    if enriched:
        slide_ind = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide_ind)
        _add_text(slide_ind, 0.5, 0.25, 12.333, 0.7,
                  "Industry Breakdown", size=24, bold=True, align=PP_ALIGN.CENTER)

        try:
            ind_img = _generate_industry_bar(all_rows)
            if ind_img:
                slide_ind.shapes.add_picture(ind_img, Inches(1), Inches(1.2),
                                             Inches(11.333), Inches(5.6))
        except Exception:
            pass

        # Industry insight
        import analysis as _an
        try:
            sd = _an.compute_sector_breakdown(all_rows)
            industries = sd.get("industries", [])
            if industries:
                top_ind = industries[0]
                n_ind = len(industries)
                _add_wrapped_text(slide_ind, 0.5, 0.9, 12.333, 0.35,
                                  f"{top_ind['name']} leads at {top_ind['pct']:.1f}% portfolio weight  |  {n_ind} industries represented across portfolio",
                                  size=10, color=MUTED, align=PP_ALIGN.CENTER, line_spacing=1.1)
        except Exception:
            pass

    # ── Slide 6: Top Holdings vs ACWI (full-width) ──
    if enriched and acwi_data:
        slide_top = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide_top)
        _add_text(slide_top, 0.5, 0.25, 12.333, 0.7,
                  "Top Holdings vs ACWI", size=24, bold=True, align=PP_ALIGN.CENTER)

        # Takeaway
        insight_conc = _find_insight("concentration")
        if insight_conc:
            _add_wrapped_text(slide_top, 0.5, 0.9, 12.333, 0.35, insight_conc,
                              size=10, color=MUTED, align=PP_ALIGN.CENTER, line_spacing=1.1)

        try:
            top_full_img = _generate_top_holdings_comparison(all_rows, acwi_data)
            if top_full_img:
                slide_top.shapes.add_picture(top_full_img, Inches(1), Inches(1.2),
                                              Inches(11.333), Inches(5.6))
        except Exception:
            pass

    # ── Slides: Per-manager slides ──
    for idx, mgr_name in enumerate(mgrs):
        stocks = by_mgr[mgr_name]
        slide_m = prs.slides.add_slide(prs.slide_layouts[6])
        _set_slide_bg(slide_m)

        r_c, g_c, b_c = TREEMAP_COLORS[idx % len(TREEMAP_COLORS)]
        mgr_color = RGBColor(r_c, g_c, b_c)

        # Colored bar at top
        bar = slide_m.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                                       Inches(0), Inches(0), Inches(13.333), Inches(0.08))
        bar.fill.solid()
        bar.fill.fore_color.rgb = mgr_color
        bar.line.fill.background()

        _add_text(slide_m, 0.5, 0.2, 12.333, 0.5, mgr_name, size=22, bold=True)

        period = stocks[0].get("period_of_report", "") if stocks else ""
        _add_text(slide_m, 0.5, 0.7, 6, 0.3,
                  f"Period: {period}  |  Top {len(stocks)} Holdings", size=11, color=MUTED)

        # Holdings table
        if enriched:
            headers = ["#", "Ticker", "Name", "Value ($000s)", "% Port", "Prior Ret", "Filing Ret", "Fwd P/E", "Div Yld", "EPS Beat", "Sector"]
            col_widths = [0.35, 0.7, 2.5, 1.2, 0.7, 0.8, 0.8, 0.7, 0.7, 1.1, 1.6]
        else:
            headers = ["#", "Ticker", "Name", "Value ($000s)", "% Port"]
            col_widths = [0.5, 1.0, 4.0, 2.0, 1.2]

        table_x = 0.5
        table_y = 1.2
        table_w = sum(col_widths)
        n_rows = min(len(stocks), 20) + 1
        table_h = n_rows * 0.28

        tbl = slide_m.shapes.add_table(n_rows, len(headers),
                                        Inches(table_x), Inches(table_y),
                                        Inches(table_w), Inches(table_h)).table

        # Style header
        for ci, hdr in enumerate(headers):
            cell = tbl.cell(0, ci)
            cell.text = hdr
            for paragraph in cell.text_frame.paragraphs:
                paragraph.font.size = Pt(9)
                paragraph.font.bold = True
                paragraph.font.color.rgb = WHITE
            cell.fill.solid()
            cell.fill.fore_color.rgb = RGBColor(0x2F, 0x54, 0x96)

        # Data rows
        for ri, s in enumerate(stocks[:20], 1):
            if enriched:
                prior_ret = s.get("prior_quarter_return_pct")
                prior_ret_str = f"{prior_ret:+.1f}%" if prior_ret is not None else "—"
                filing_ret = s.get("filing_quarter_return_pct")
                filing_ret_str = f"{filing_ret:+.1f}%" if filing_ret is not None else "—"
                fwd_pe = s.get("forward_pe")
                fwd_pe_str = f"{fwd_pe:.1f}x" if fwd_pe is not None else "—"
                div_yld = s.get("dividend_yield")
                div_yld_str = f"{div_yld:.1f}%" if div_yld is not None else "—"
                beat = _eps_beat_label(s.get("filing_eps_beat_dollars"), s.get("filing_eps_beat_pct"))
                sector = (s.get("sector") or "—")[:20]
                vals = [
                    str(s["rank"]), s["ticker"], s["name"][:30],
                    f"{int(round(s['value_usd']/1000)):,}", f"{s['pct_of_portfolio']:.1f}%",
                    prior_ret_str, filing_ret_str, fwd_pe_str, div_yld_str, beat, sector,
                ]
            else:
                vals = [
                    str(s["rank"]), s["ticker"], s["name"][:40],
                    f"{int(round(s['value_usd']/1000)):,}", f"{s['pct_of_portfolio']:.1f}%",
                ]

            for ci, val in enumerate(vals):
                cell = tbl.cell(ri, ci)
                cell.text = val
                for paragraph in cell.text_frame.paragraphs:
                    paragraph.font.size = Pt(8)
                    paragraph.font.color.rgb = LIGHT_GRAY
                # Alternating row
                if ri % 2 == 0:
                    cell.fill.solid()
                    cell.fill.fore_color.rgb = RGBColor(0x16, 0x1E, 0x30)
                else:
                    cell.fill.solid()
                    cell.fill.fore_color.rgb = BG_COLOR

        # Set column widths
        for ci, w in enumerate(col_widths):
            tbl.columns[ci].width = Inches(w)

    # ── Last slide: Treemap ──
    _add_treemap_slide(prs, all_rows, by_mgr, mgrs, client_name, report_name)

    filename = f"report_{run_date}.pptx"
    prs.save(os.path.join(OUTPUT_DIR, filename))
    return filename


# Keep backward-compatible name
def write_treemap_pptx(all_rows, run_date, client_name="", report_name=""):
    return write_report_pptx(all_rows, run_date, client_name, report_name)


def _add_treemap_slide(prs, all_rows, by_mgr, mgrs, client_name, report_name):
    """Add the treemap overview slide to the presentation."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_slide_bg(slide)

    title_parts = [p for p in [client_name, report_name] if p]
    title_text = " — ".join(title_parts) if title_parts else "Portfolio Treemap"
    _add_text(slide, 0.5, 0.25, 12.333, 0.7, title_text, size=20, bold=True, align=PP_ALIGN.CENTER)

    n = len(mgrs)
    if n <= 6:
        cols = n
        rows_n = 1
    elif n <= 12:
        cols = math.ceil(n / 2)
        rows_n = 2
    else:
        cols = math.ceil(math.sqrt(n))
        rows_n = math.ceil(n / cols)
    margin_x, start_y = 0.4, 1.1
    total_w, total_h = 12.533, 6.1
    gap = 0.06
    cell_w = (total_w - (cols - 1) * gap) / cols
    cell_h = (total_h - (rows_n - 1) * gap) / rows_n

    for idx, mgr_name in enumerate(mgrs):
        ci, ri = idx % cols, idx // cols
        x = margin_x + ci * (cell_w + gap)
        y = start_y + ri * (cell_h + gap)
        r, g, b = TREEMAP_COLORS[idx % len(TREEMAP_COLORS)]

        shp = slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE,
            Inches(x), Inches(y), Inches(cell_w), Inches(cell_h))
        shp.fill.solid()
        shp.fill.fore_color.rgb = RGBColor(r, g, b)
        shp.line.fill.background()

        tb1 = slide.shapes.add_textbox(
            Inches(x + 0.08), Inches(y + 0.04),
            Inches(cell_w - 0.16), Inches(0.25))
        tf1 = tb1.text_frame
        tf1.word_wrap = True
        p1 = tf1.paragraphs[0]
        p1.text = mgr_name
        p1.font.size = Pt(9)
        p1.font.bold = True
        p1.font.color.rgb = WHITE

        stocks = by_mgr[mgr_name]
        tb2 = slide.shapes.add_textbox(
            Inches(x + 0.08), Inches(y + 0.28),
            Inches(cell_w - 0.16), Inches(cell_h - 0.34))
        tf2 = tb2.text_frame
        tf2.word_wrap = True
        for si, s in enumerate(stocks):
            tk_str = f" ({s['ticker']})" if s.get('ticker', 'N/A') != 'N/A' else ''
            line = f"{s['name']}{tk_str} ({s['pct_of_portfolio']}%)"
            if si == 0:
                p2 = tf2.paragraphs[0]
            else:
                p2 = tf2.add_paragraph()
            p2.text = line
            p2.font.size = Pt(7)
            p2.font.color.rgb = LIGHT_GRAY
            p2.space_after = Pt(1)


def _generate_grouped_bar(portfolio_data, benchmark_data, title,
                          xlabel="Weight (%)", figsize=(10, 6), max_items=12):
    """
    Generate a grouped horizontal bar chart comparing portfolio vs ACWI.

    Args:
        portfolio_data: list of (label, pct) sorted descending by portfolio weight
        benchmark_data: dict {label: pct} or None for portfolio-only
        title: chart title
        xlabel: x-axis label
        figsize: matplotlib figure size tuple
        max_items: max number of categories to show

    Returns: BytesIO PNG image or None
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return None

    if not portfolio_data:
        return None

    portfolio_data = portfolio_data[:max_items]
    # Reverse for bottom-to-top (highest at top)
    portfolio_data = list(reversed(portfolio_data))

    labels = [p[0] for p in portfolio_data]
    port_vals = [p[1] for p in portfolio_data]

    has_benchmark = benchmark_data is not None and len(benchmark_data) > 0
    bench_vals = [benchmark_data.get(lab, 0) for lab in labels] if has_benchmark else None

    y = np.arange(len(labels))
    height = 0.35 if has_benchmark else 0.6

    fig, ax = plt.subplots(figsize=figsize, facecolor='#0F172A')
    ax.set_facecolor('#0F172A')

    if has_benchmark:
        bars_p = ax.barh(y + height / 2, port_vals, height, label='Portfolio', color='#3B82F6')
        bars_b = ax.barh(y - height / 2, bench_vals, height, label='ACWI', color='#F59E0B')
    else:
        bars_p = ax.barh(y, port_vals, height, color='#3B82F6')
        bars_b = None

    ax.set_yticks(y)
    ax.set_yticklabels(labels, color='#E2E8F0', fontsize=9)
    ax.set_xlabel(xlabel, color='#E2E8F0', fontsize=10)
    ax.set_title(title, color='white', fontsize=14, fontweight='bold', pad=15)
    ax.tick_params(colors='#94A3B8', labelsize=8)
    for spine in ax.spines.values():
        spine.set_color('#334155')
    ax.xaxis.grid(True, color='#1E293B', linewidth=0.5, alpha=0.5)

    # Annotate bar values
    max_val = max(max(port_vals), max(bench_vals) if bench_vals else 0) or 1
    offset = max_val * 0.02

    for bar, val in zip(bars_p, port_vals):
        if val > 0:
            ax.text(bar.get_width() + offset, bar.get_y() + bar.get_height() / 2,
                    f'{val:.1f}%', va='center', color='#93C5FD', fontsize=7, fontweight='bold')

    if bars_b is not None:
        for bar, val in zip(bars_b, bench_vals):
            if val > 0:
                ax.text(bar.get_width() + offset, bar.get_y() + bar.get_height() / 2,
                        f'{val:.1f}%', va='center', color='#FCD34D', fontsize=7, fontweight='bold')

        # Over/underweight annotations on the far right
        right_x = max_val * 1.18
        for i, (pv, bv) in enumerate(zip(port_vals, bench_vals)):
            diff = pv - bv
            if abs(diff) >= 0.05:
                sign = "+" if diff > 0 else ""
                clr = '#4ADE80' if diff > 0 else '#F87171'
                ax.text(right_x, y[i], f'{sign}{diff:.1f}%', va='center',
                        color=clr, fontsize=7, fontweight='bold')

    if has_benchmark:
        ax.legend(loc='lower right', facecolor='#1E293B', edgecolor='#334155',
                  labelcolor='#E2E8F0', fontsize=9)

    ax.set_xlim(0, max_val * 1.32 if has_benchmark else max_val * 1.15)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#0F172A',
                edgecolor='none', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


def _generate_sector_comparison(all_rows, acwi_data=None):
    """Generate sector comparison grouped bar chart: Portfolio vs ACWI."""
    from financial_data import normalize_sector_name

    sectors = defaultdict(float)
    total_val = sum(r.get("value_usd", 0) for r in all_rows)
    if total_val == 0:
        return None
    for r in all_rows:
        s = normalize_sector_name(r.get("sector") or "Unknown")
        sectors[s] += r.get("value_usd", 0)

    # Convert to percentages, sort by portfolio weight desc
    port_data = [(s, round(v / total_val * 100, 2)) for s, v in sectors.items() if s != "Unknown"]
    port_data.sort(key=lambda x: -x[1])

    # Top 10 + Other
    if len(port_data) > 10:
        other_pct = sum(p[1] for p in port_data[10:])
        port_data = port_data[:10]
        if other_pct > 0.1:
            port_data.append(("Other", round(other_pct, 2)))

    benchmark = acwi_data.get("sectors") if acwi_data else None
    title = "Sector Allocation: Portfolio vs ACWI" if benchmark else "Sector Allocation"
    return _generate_grouped_bar(port_data, benchmark, title)


def _generate_geo_comparison(all_rows, acwi_data=None):
    """Generate geographic comparison grouped bar chart: Portfolio vs ACWI."""
    countries = defaultdict(float)
    total_val = sum(r.get("value_usd", 0) for r in all_rows)
    if total_val == 0:
        return None
    for r in all_rows:
        c = r.get("country") or "Unknown"
        countries[c] += r.get("value_usd", 0)

    port_data = [(c, round(v / total_val * 100, 2)) for c, v in countries.items() if c != "Unknown"]
    port_data.sort(key=lambda x: -x[1])

    if len(port_data) > 10:
        other_pct = sum(p[1] for p in port_data[10:])
        port_data = port_data[:10]
        if other_pct > 0.1:
            port_data.append(("Other", round(other_pct, 2)))

    benchmark = acwi_data.get("countries") if acwi_data else None
    title = "Geographic Exposure: Portfolio vs ACWI" if benchmark else "Geographic Exposure"
    return _generate_grouped_bar(port_data, benchmark, title)


def _generate_industry_bar(all_rows):
    """Generate a top-15 industry horizontal bar chart (portfolio only)."""
    industries = defaultdict(float)
    total_val = sum(r.get("value_usd", 0) for r in all_rows)
    if total_val == 0:
        return None
    for r in all_rows:
        ind = r.get("industry") or "Unknown"
        industries[ind] += r.get("value_usd", 0)

    if not industries or len(industries) <= 1:
        return None

    port_data = [(ind[:28], round(v / total_val * 100, 2))
                 for ind, v in industries.items() if ind != "Unknown"]
    port_data.sort(key=lambda x: -x[1])
    port_data = port_data[:15]

    return _generate_grouped_bar(port_data, None, "Top Industries by Weight")


def _generate_top_holdings_comparison(all_rows, acwi_data=None, manager_weights=None):
    """Generate top holdings comparison grouped bar chart: Portfolio vs ACWI."""
    try:
        import analysis
        weighted_rows, _ = analysis._apply_manager_weights(all_rows, manager_weights)
    except (ImportError, Exception):
        weighted_rows = all_rows

    # Aggregate combined_weight by ticker
    by_ticker = defaultdict(lambda: {"weight": 0, "name": "", "ticker": ""})
    for r in weighted_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        by_ticker[tk]["weight"] += r.get("combined_weight", r.get("pct_of_portfolio", 0))
        by_ticker[tk]["name"] = r.get("name", "")
        by_ticker[tk]["ticker"] = tk

    top10 = sorted(by_ticker.values(), key=lambda x: -x["weight"])[:10]
    if not top10:
        return None

    # Build ACWI lookup by ticker
    acwi_by_ticker = {}
    if acwi_data and acwi_data.get("top_holdings"):
        for h in acwi_data["top_holdings"]:
            acwi_by_ticker[h["ticker"]] = h["weight"]

    try:
        from analysis import shorten_stock_name
    except ImportError:
        def shorten_stock_name(n):
            return n[:20]

    port_data = []
    benchmark = {}
    for s in top10:
        label = f"{s['ticker']} ({shorten_stock_name(s['name'])})"
        port_data.append((label, round(s["weight"], 2)))
        bw = acwi_by_ticker.get(s["ticker"], 0)
        if bw > 0:
            benchmark[label] = round(bw, 2)

    title = "Top 10 Holdings: Portfolio vs ACWI" if benchmark else "Top 10 Holdings by Weight"
    return _generate_grouped_bar(port_data, benchmark if benchmark else None, title)


# Backward-compat aliases
_generate_sector_pie = _generate_sector_comparison
_generate_geo_pie = _generate_geo_comparison


# ── Legacy CSV support ───────────────────────────────────────────────────────

def write_individual_csv(manager_name, period, rows):
    """Write a per-manager CSV (legacy). Prefer write_individual_xlsx."""
    safe = _safe_name(manager_name)
    period_str = period.replace("-", "")
    filename = f"{safe}_top20_{period_str}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return filename


# ── Row builder ──────────────────────────────────────────────────────────────

def _build_rows(manager_name, period, filed_at, holdings_list, value_key="value", multiplier=1):
    """
    Convert a list of (name, ticker, value) dicts into ranked rows.
    holdings_list: list of dicts with keys 'name', 'ticker', value_key
    """
    total_value = sum(h.get(value_key, 0) or 0 for h in holdings_list) * multiplier
    if total_value == 0:
        total_value = 1

    rows = []
    for rank, h in enumerate(holdings_list[:TOP_N], start=1):
        val = (h.get(value_key, 0) or 0) * multiplier
        pct = round(val / total_value * 100, 2)
        rows.append({
            "manager": manager_name,
            "period_of_report": period,
            "filed_at": filed_at,
            "rank": rank,
            "name": h.get("name", "Unknown"),
            "ticker": h.get("ticker", "N/A"),
            "value_usd": round(val),
            "pct_of_portfolio": pct,
            # Prior quarter enrichment (6)
            "prior_price_qtr_end": None,
            "prior_quarter_return_pct": None,
            "prior_reported_eps": None,
            "prior_consensus_eps": None,
            "prior_eps_beat_dollars": None,
            "prior_eps_beat_pct": None,
            # Filing quarter enrichment (6)
            "filing_price_qtr_end": None,
            "filing_quarter_return_pct": None,
            "filing_reported_eps": None,
            "filing_consensus_eps": None,
            "filing_eps_beat_dollars": None,
            "filing_eps_beat_pct": None,
            # Current / live (5)
            "forward_pe": None,
            "forward_eps_growth": None,
            "dividend_yield": None,
            "trailing_eps": None,
            "forward_eps": None,
            # QTD (2)
            "qtd_return_pct": None,
            "qtd_price_start": None,
            # Static (3)
            "sector": None,
            "industry": None,
            "country": None,
        })
    return rows


# ── 13F Fetcher ──────────────────────────────────────────────────────────────

def fetch_13f(manager_name, cik):
    """
    Fetch latest 13F-HR filing for a given CIK.
    Returns (period, filed_at, n_total, rows).
    """
    company = Company(cik)
    filings = company.get_filings(form="13F-HR")
    if not filings or len(filings) == 0:
        raise ValueError(f"No 13F-HR filings found for CIK {cik}")

    filing = None
    obj = None
    cutoff = _max_filing_date()
    for f in filings:
        if str(f.filing_date) > cutoff:
            continue
        _obj = f.obj()
        _por = str(_obj.period_of_report) if hasattr(_obj, "period_of_report") else str(f.filing_date)
        if _por > MAX_DATE:
            continue  # report period is after our target quarter
        filing = f
        obj = _obj
        break

    if filing is None:
        raise ValueError(f"No 13F-HR filing found before {cutoff} with period <= {MAX_DATE}")

    period = str(filing.filing_date)
    filed_at = str(filing.filing_date)

    holdings_df = None
    if hasattr(obj, "infotable"):
        holdings_df = obj.infotable
    elif hasattr(obj, "holdings"):
        holdings_df = obj.holdings

    if holdings_df is None or len(holdings_df) == 0:
        raise ValueError("Could not parse 13F holdings table")

    if hasattr(obj, "period_of_report"):
        period = str(obj.period_of_report)

    cols = {c.lower().replace(" ", "").replace("_", ""): c for c in holdings_df.columns}

    def _find_col(*candidates):
        for cand in candidates:
            key = cand.lower().replace(" ", "").replace("_", "")
            if key in cols:
                return cols[key]
        return None

    col_name  = _find_col("nameOfIssuer", "Name of Issuer", "Issuer", "name", "companyName")
    col_value = _find_col("value", "Value", "value(x$1000)", "marketValue", "mktVal")
    col_cusip = _find_col("cusip", "CUSIP")
    col_tick  = _find_col("ticker", "Ticker", "symbol", "Symbol")

    records = []
    n_total = len(holdings_df)

    for _, row in holdings_df.iterrows():
        name  = str(row[col_name]) if col_name and col_name in row.index else "Unknown"
        cusip = str(row[col_cusip]).strip() if col_cusip and col_cusip in row.index else ""
        value = float(row[col_value] or 0) if col_value and col_value in row.index else 0.0

        ticker = "N/A"
        if col_tick and col_tick in row.index:
            tv = str(row[col_tick]).strip()
            if tv and tv not in ("N/A", "nan", "None", ""):
                ticker = tv
        if ticker == "N/A" and cusip:
            ticker = CUSIP_TO_TICKER.get(cusip, "N/A")

        records.append({"name": name, "ticker": ticker, "value": value, "_cusip": cusip})

    # Batch resolve unresolved CUSIPs via OpenFIGI
    unresolved = {}
    for idx, rec in enumerate(records):
        if rec["ticker"] == "N/A" and rec.get("_cusip"):
            unresolved.setdefault(rec["_cusip"], []).append(idx)
    if unresolved:
        try:
            figi = openfigi_lookup(list(unresolved.keys()))
            for cusip, ticker in figi.items():
                if ticker != "N/A" and cusip in unresolved:
                    for idx in unresolved[cusip]:
                        records[idx]["ticker"] = ticker
        except Exception:
            pass

    _apply_ticker_aliases(records)
    _resolve_remaining_tickers(records)
    records.sort(key=lambda x: x["value"], reverse=True)
    rows = _build_rows(manager_name, period, filed_at, records, value_key="value", multiplier=1)
    return period, filed_at, n_total, rows


# ── N-PORT Fetcher ───────────────────────────────────────────────────────────

def fetch_nport(manager_name, cik, series_keyword):
    """
    Fetch latest NPORT-P filing for a given CIK, matching a series by keyword.
    Returns (period, filed_at, n_total, rows).
    """
    company = Company(cik)
    filings = company.get_filings(form="NPORT-P")
    if not filings or len(filings) == 0:
        raise ValueError(f"No NPORT-P filings found for CIK {cik}")

    ns = {"nport": "http://www.sec.gov/edgar/nport"}

    cutoff = _max_filing_date()
    for f in filings:
        if str(f.filing_date) > cutoff:
            continue

        xml_content = None
        for att in f.attachments:
            if hasattr(att, "is_xml") and att.is_xml:
                xml_content = att.download()
                break
            elif hasattr(att, "document") and att.document.endswith(".xml"):
                xml_content = att.download()
                break

        if not xml_content:
            continue

        if isinstance(xml_content, bytes):
            xml_content = xml_content.decode("utf-8", errors="replace")

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            continue

        series_el = root.find(".//nport:seriesName", ns)
        if series_el is None:
            series_el = root.find(".//{http://www.sec.gov/edgar/nport}seriesName")
        if series_el is None:
            series_el = root.find(".//seriesName")

        series_name = ""
        if series_el is not None and series_el.text:
            series_name = series_el.text.strip()

        if series_keyword.lower() not in series_name.lower():
            continue

        period = str(f.filing_date)
        filed_at = str(f.filing_date)

        rep_date_el = root.find(".//nport:repPdDate", ns)
        if rep_date_el is not None and rep_date_el.text:
            period = rep_date_el.text.strip()

        # Skip filings whose reporting period is after MAX_DATE
        if period > MAX_DATE:
            continue

        inv_elements = root.findall(".//nport:invstOrSec", ns)
        if not inv_elements:
            inv_elements = root.findall(".//{http://www.sec.gov/edgar/nport}invstOrSec")

        holdings_list = []
        for inv in inv_elements:
            name_el = inv.find("nport:name", ns)
            if name_el is None:
                name_el = inv.find("{http://www.sec.gov/edgar/nport}name")
            name = name_el.text.strip() if name_el is not None and name_el.text else "Unknown"

            val_el = inv.find("nport:valUSD", ns)
            if val_el is None:
                val_el = inv.find("{http://www.sec.gov/edgar/nport}valUSD")
            value = float(val_el.text) if val_el is not None and val_el.text else 0

            cusip_el = inv.find("nport:cusip", ns)
            if cusip_el is None:
                cusip_el = inv.find("{http://www.sec.gov/edgar/nport}cusip")
            cusip = cusip_el.text.strip() if cusip_el is not None and cusip_el.text else ""

            ticker = "N/A"

            ident = inv.find(".//nport:identifiers", ns)
            if ident is None:
                ident = inv.find(".//{http://www.sec.gov/edgar/nport}identifiers")
            if ident is not None:
                ticker_el = ident.find("nport:ticker", ns)
                if ticker_el is None:
                    ticker_el = ident.find("{http://www.sec.gov/edgar/nport}ticker")
                if ticker_el is not None:
                    tv = ticker_el.get("value", "").strip()
                    if tv and tv != "N/A":
                        ticker = tv

            if ticker == "N/A":
                ticker_el2 = inv.find("nport:ticker", ns)
                if ticker_el2 is None:
                    ticker_el2 = inv.find("{http://www.sec.gov/edgar/nport}ticker")
                if ticker_el2 is not None and ticker_el2.text:
                    tv2 = ticker_el2.text.strip()
                    if tv2 and tv2 != "N/A":
                        ticker = tv2

            if ticker == "N/A" and cusip:
                ticker = CUSIP_TO_TICKER.get(cusip, "N/A")

            holdings_list.append({"name": name, "ticker": ticker, "value": value, "_cusip": cusip})

        n_total = len(holdings_list)
        if n_total == 0:
            continue

        # Batch resolve unresolved CUSIPs via OpenFIGI
        unresolved = {}
        for idx, rec in enumerate(holdings_list):
            if rec["ticker"] == "N/A" and rec.get("_cusip"):
                unresolved.setdefault(rec["_cusip"], []).append(idx)
        if unresolved:
            try:
                figi = openfigi_lookup(list(unresolved.keys()))
                for cusip_k, ticker_v in figi.items():
                    if ticker_v != "N/A" and cusip_k in unresolved:
                        for idx in unresolved[cusip_k]:
                            holdings_list[idx]["ticker"] = ticker_v
            except Exception:
                pass

        _apply_ticker_aliases(holdings_list)
        _resolve_remaining_tickers(holdings_list)
        holdings_list.sort(key=lambda x: x["value"], reverse=True)

        rows = _build_rows(manager_name, period, filed_at, holdings_list,
                           value_key="value", multiplier=1)
        return period, filed_at, n_total, rows

    raise ValueError(f"No NPORT-P filing matched series keyword '{series_keyword}' before {cutoff} (max_date={MAX_DATE})")


# ── Main (standalone) ────────────────────────────────────────────────────────

def main():
    combined_rows = []
    run_date = datetime.today().strftime("%Y%m%d")

    print("=" * 60)
    print("Investment Manager Holdings Fetcher — 13F + N-PORT (edgartools)")
    print(f"Max period: {MAX_DATE}  |  Run: {run_date}")
    print("=" * 60)

    for manager_name, cik in MANAGERS_13F.items():
        print(f"\n[13F] {manager_name}  (CIK: {cik})")
        try:
            period, filed_at, n_total, rows = fetch_13f(manager_name, cik)
            print(f"      Period: {period}  |  Filed: {filed_at}  |  Total positions: {n_total}")
            filename = write_individual_csv(manager_name, period, rows)
            combined_rows.extend(rows)
            print(f"      OK {filename}")
        except Exception as e:
            print(f"      Error: {e}")

    for manager_name, info in MANAGERS_NPORT.items():
        print(f"\n[N-PORT] {manager_name}  (CIK: {info['cik']})")
        try:
            period, filed_at, n_total, rows = fetch_nport(
                manager_name, info["cik"], info["series_keyword"]
            )
            print(f"         Period: {period}  |  Filed: {filed_at}  |  Total holdings: {n_total}")
            filename = write_individual_csv(manager_name, period, rows)
            combined_rows.extend(rows)
            print(f"         OK {filename}")
        except Exception as e:
            print(f"         Error: {e}")

    if combined_rows:
        combined_name = f"all_managers_top20_{run_date}.csv"
        combined_path = os.path.join(OUTPUT_DIR, combined_name)
        with open(combined_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(combined_rows)

        print(f"\n{'=' * 60}")
        print(f"Combined CSV: {combined_name}")
        n_managers = len(MANAGERS_13F) + len(MANAGERS_NPORT)
        print(f"   {n_managers} managers, {len(combined_rows)} rows")
        print("=" * 60)
    else:
        print("\nNo data collected.")


# ── PDF Report Generation ─────────────────────────────────────────────────────
# Merged from pdf_report.py — requires reportlab (pip install reportlab)

def generate_pdf(all_rows, run_date, cfg):
    """
    Generate a PDF report from holdings data.

    Args:
        all_rows: list of enriched holdings dicts
        run_date: string like "20250215"
        cfg: config dict with client_name, report_name, etc.

    Returns:
        BytesIO buffer containing the PDF
    """
    from reportlab.lib import colors as rl_colors
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, KeepTogether, Image,
    )

    # ── Color Palette ──
    DARK_BG = rl_colors.HexColor("#0F172A")
    CARD_BG = rl_colors.HexColor("#1E293B")
    HEADER_BG = rl_colors.HexColor("#2F5496")
    LIGHT_TEXT = rl_colors.HexColor("#E2E8F0")
    MUTED_TEXT = rl_colors.HexColor("#94A3B8")
    GREEN = rl_colors.HexColor("#22C55E")
    RED = rl_colors.HexColor("#EF4444")
    WHITE = rl_colors.white
    BLACK = rl_colors.black
    ALT_ROW = rl_colors.HexColor("#F2F7FB")
    BORDER_COLOR = rl_colors.HexColor("#D9D9D9")

    def _pe_fmt(val):
        if val is None:
            return "—"
        return f"{val:.1f}x"

    def _pct_fmt(val, plus=False):
        if val is None:
            return "—"
        sign = "+" if plus and val > 0 else ""
        return f"{sign}{val:.1f}%"

    def _dollar_fmt(val):
        if val is None:
            return "—"
        return f"${val:,.0f}"

    def _eps_beat_fmt(beat_d, beat_p):
        if beat_d is None:
            return "N/A"
        sign = "+" if beat_d >= 0 else ""
        s = f"{sign}${beat_d:.2f}"
        if beat_p is not None:
            s += f" ({sign}{beat_p:.1f}%)"
        return s

    def _pdf_styles():
        """Create custom paragraph styles for the report."""
        ss = getSampleStyleSheet()
        ss.add(ParagraphStyle(
            "CoverTitle", parent=ss["Title"],
            fontSize=28, textColor=HEADER_BG, spaceAfter=12,
            alignment=1,
        ))
        ss.add(ParagraphStyle(
            "CoverSub", parent=ss["Normal"],
            fontSize=14, textColor=MUTED_TEXT, alignment=1,
            spaceAfter=6,
        ))
        ss.add(ParagraphStyle(
            "SectionTitle", parent=ss["Heading1"],
            fontSize=16, textColor=HEADER_BG, spaceBefore=18,
            spaceAfter=8,
        ))
        ss.add(ParagraphStyle(
            "MgrTitle", parent=ss["Heading2"],
            fontSize=13, textColor=HEADER_BG, spaceBefore=12,
            spaceAfter=6,
        ))
        ss.add(ParagraphStyle(
            "SmallText", parent=ss["Normal"],
            fontSize=8, textColor=rl_colors.HexColor("#666666"),
        ))
        ss.add(ParagraphStyle(
            "CellText", parent=ss["Normal"],
            fontSize=7, leading=9,
        ))
        ss.add(ParagraphStyle(
            "ExecBody", parent=ss["Normal"],
            fontSize=10, textColor=rl_colors.HexColor("#334155"),
            spaceAfter=8, leading=14,
        ))
        ss.add(ParagraphStyle(
            "ExecBullet", parent=ss["Normal"],
            fontSize=9, textColor=rl_colors.HexColor("#334155"),
            spaceAfter=5, leading=12, leftIndent=18, bulletIndent=6,
        ))
        ss.add(ParagraphStyle(
            "KeyTakeaway", parent=ss["Normal"],
            fontSize=8, textColor=rl_colors.HexColor("#475569"),
            spaceAfter=4, leading=10, leftIndent=6,
            borderWidth=0, borderPadding=4,
        ))
        return ss

    def _header_footer(canvas, doc, title=""):
        """Add header/footer to each page."""
        canvas.saveState()
        canvas.setStrokeColor(HEADER_BG)
        canvas.setLineWidth(1)
        canvas.line(36, letter[1] - 36 if doc.pagesize == letter else landscape(letter)[1] - 36,
                    doc.pagesize[0] - 36,
                    doc.pagesize[1] - 36)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTED_TEXT)
        canvas.drawString(36, 20, title)
        canvas.drawRightString(doc.pagesize[0] - 36, 20, f"Page {doc.page}")
        canvas.restoreState()

    def _make_table(headers, data_rows, col_widths=None):
        """Create a styled table."""
        all_data = [headers] + data_rows
        tbl = Table(all_data, colWidths=col_widths, repeatRows=1)
        style_cmds = [
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
            ("TOPPADDING", (0, 0), (-1, 0), 5),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
            ("TOPPADDING", (0, 1), (-1, -1), 3),
            ("GRID", (0, 0), (-1, -1), 0.5, BORDER_COLOR),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ]
        for i in range(1, len(all_data)):
            if i % 2 == 0:
                style_cmds.append(("BACKGROUND", (0, i), (-1, i), ALT_ROW))
        tbl.setStyle(TableStyle(style_cmds))
        return tbl

    # ── Build the PDF ──
    buf = io.BytesIO()
    ss = _pdf_styles()

    client_name = cfg.get("client_name", "")
    report_name = cfg.get("report_name", "")
    title_parts = [p for p in [client_name, report_name] if p]
    title = " — ".join(title_parts) if title_parts else "Portfolio Holdings Report"

    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        topMargin=0.6 * inch,
        bottomMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        title=title,
    )

    elements = []

    # ── Cover Page ──
    elements.append(Spacer(1, 2 * inch))
    elements.append(Paragraph(title, ss["CoverTitle"]))
    date_str = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}" if len(run_date) == 8 else run_date
    elements.append(Paragraph(f"Generated: {date_str}", ss["CoverSub"]))

    by_mgr = defaultdict(list)
    for r in all_rows:
        by_mgr[r["manager"]].append(r)

    total_value = sum(r.get("value_usd", 0) for r in all_rows)
    unique_tickers = len({r["ticker"] for r in all_rows if r.get("ticker") != "N/A"})
    n_mgrs = len(by_mgr)

    elements.append(Spacer(1, 0.5 * inch))
    elements.append(Paragraph(
        f"{n_mgrs} Managers  &bull;  {len(all_rows)} Holdings  &bull;  "
        f"{unique_tickers} Unique Stocks  &bull;  ${total_value/1e9:.1f}B Total Value",
        ss["CoverSub"]
    ))
    elements.append(PageBreak())

    # ── Executive Summary ──
    exec_data = None
    try:
        from financial_data import fetch_acwi_benchmark
        print("[PDF] Building executive summary...")
        acwi_bench = None
        try:
            acwi_bench = fetch_acwi_benchmark()
        except Exception:
            pass
        exec_data = _build_exec_summary_content(all_rows, acwi_bench)

        elements.append(Paragraph("Executive Summary", ss["SectionTitle"]))
        elements.append(Spacer(1, 0.1 * inch))
        elements.append(Paragraph(exec_data["opening"], ss["ExecBody"]))
        elements.append(Spacer(1, 0.15 * inch))
        elements.append(Paragraph(
            '<font color="#2F5496"><b>Key Findings</b></font>', ss["ExecBody"]
        ))
        for f in exec_data["findings"]:
            parts = f.split(": ", 1)
            if len(parts) == 2:
                bullet_text = f'<bullet>&bull;</bullet><b>{parts[0]}:</b> {parts[1]}'
            else:
                bullet_text = f'<bullet>&bull;</bullet>{f}'
            elements.append(Paragraph(bullet_text, ss["ExecBullet"]))
        elements.append(Spacer(1, 0.15 * inch))
        elements.append(Paragraph(exec_data["closing"], ss["ExecBody"]))
        print("[PDF] Executive summary complete.")
    except Exception as e:
        print(f"[PDF] Executive summary FAILED: {e}")
        import traceback
        traceback.print_exc()

    elements.append(PageBreak())

    # ── Portfolio Summary ──
    elements.append(Paragraph("Portfolio Summary", ss["SectionTitle"]))
    try:
        import analysis
        stats = analysis.compute_summary_stats(all_rows)
        if stats:
            wr = stats.get("weighted_return", {})
            summary_data = [
                ["Total Holdings", str(stats.get("total_holdings", 0))],
                ["Unique Stocks", str(stats.get("unique_stocks", 0))],
                ["Managers", str(stats.get("unique_managers", 0))],
                ["Total Value", f"${stats.get('total_value', 0)/1e9:.2f}B"],
                ["Avg Filing Qtr Return", _pct_fmt(stats.get("avg_quarter_return"), plus=True)],
                ["Wtd Filing Qtr Return", _pct_fmt(wr.get("filing_qtr_weighted_return"), plus=True)],
                ["Wtd Prior Qtr Return", _pct_fmt(wr.get("prior_qtr_weighted_return"), plus=True)],
                ["EPS Beat Rate", _pct_fmt(stats.get("eps_beat_rate"))],
            ]
            most = stats.get("most_common_stock")
            if most:
                summary_data.append(["Most Held Stock",
                                      f"{most['name']} ({most['ticker']}) — {most['manager_count']} managers"])
            sum_tbl = _make_table(["Metric", "Value"], summary_data,
                                   col_widths=[2.5 * inch, 4 * inch])
            elements.append(sum_tbl)
    except ImportError:
        pass

    elements.append(Spacer(1, 0.3 * inch))

    # ── Top 10 Holdings ──
    elements.append(Paragraph("Top 10 Holdings by Aggregate Value", ss["SectionTitle"]))
    value_by_ticker = defaultdict(lambda: {"value": 0, "name": "", "ticker": ""})
    for r in all_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        value_by_ticker[tk]["value"] += r.get("value_usd", 0)
        value_by_ticker[tk]["name"] = r.get("name", "")
        value_by_ticker[tk]["ticker"] = tk

    top10 = sorted(value_by_ticker.values(), key=lambda x: x["value"], reverse=True)[:10]
    if top10:
        top_rows = []
        for i, s in enumerate(top10, 1):
            val_str = f"${s['value']/1e6:.0f}M" if s['value'] >= 1e6 else f"${s['value']/1e3:.0f}K"
            top_rows.append([str(i), f"{s['name']} ({s['ticker']})", val_str])
        top_tbl = _make_table(["#", "Stock", "Aggregate Value"], top_rows,
                               col_widths=[0.4 * inch, 5 * inch, 1.5 * inch])
        elements.append(top_tbl)

    elements.append(PageBreak())

    # ── Per-Manager Holdings ──
    elements.append(Paragraph("Manager Holdings Detail", ss["SectionTitle"]))
    has_enrichment = any(
        r.get("filing_quarter_return_pct") is not None or r.get("forward_pe") is not None
        for r in all_rows
    )

    for mgr_name in sorted(by_mgr.keys()):
        stocks = by_mgr[mgr_name]
        period = stocks[0].get("period_of_report", "") if stocks else ""
        mgr_total = sum(s.get("value_usd", 0) for s in stocks)

        elements.append(Paragraph(
            f"{mgr_name} &mdash; Period: {period} &bull; "
            f"${mgr_total/1e6:.0f}M &bull; {len(stocks)} holdings",
            ss["MgrTitle"]
        ))

        if has_enrichment:
            headers = ["#", "Stock", "Value ($K)", "% Port",
                        "Prior Ret", "Filing Ret",
                        "Fwd P/E", "Trail EPS", "Fwd EPS",
                        "Div Yld", "EPS Beat (Filing)", "Sector"]
            col_w = [0.3, 2.2, 0.8, 0.6, 0.7, 0.7, 0.6, 0.6, 0.6, 0.5, 1.2, 1.2]
            col_widths = [w * inch for w in col_w]

            data_rows = []
            for s in stocks[:20]:
                tk = s.get("ticker", "N/A")
                display = f"{s['name'][:22]} ({tk})" if tk != "N/A" else s["name"][:28]
                data_rows.append([
                    str(s["rank"]),
                    display,
                    f"{int(round(s['value_usd']/1000)):,}",
                    f"{s['pct_of_portfolio']:.1f}%",
                    _pct_fmt(s.get("prior_quarter_return_pct"), plus=True),
                    _pct_fmt(s.get("filing_quarter_return_pct"), plus=True),
                    _pe_fmt(s.get("forward_pe")),
                    f"${s['trailing_eps']:.2f}" if s.get("trailing_eps") is not None else "—",
                    f"${s['forward_eps']:.2f}" if s.get("forward_eps") is not None else "—",
                    _pct_fmt(s.get("dividend_yield")),
                    _eps_beat_fmt(s.get("filing_eps_beat_dollars"), s.get("filing_eps_beat_pct")),
                    (s.get("sector") or "—")[:18],
                ])
        else:
            headers = ["#", "Stock", "Value ($K)", "% of Portfolio"]
            col_widths = [0.4 * inch, 4 * inch, 1.2 * inch, 1.2 * inch]
            data_rows = []
            for s in stocks[:20]:
                tk = s.get("ticker", "N/A")
                display = f"{s['name'][:30]} ({tk})" if tk != "N/A" else s["name"][:35]
                data_rows.append([
                    str(s["rank"]),
                    display,
                    f"{int(round(s['value_usd']/1000)):,}",
                    f"{s['pct_of_portfolio']:.1f}%",
                ])

        tbl = _make_table(headers, data_rows, col_widths=col_widths)
        elements.append(KeepTogether([tbl, Spacer(1, 0.2 * inch)]))

    elements.append(PageBreak())

    # ── Overlap Analysis ──
    try:
        import analysis
        overlap = analysis.compute_overlap(all_rows)
        if overlap:
            elements.append(Paragraph("Stock Overlap Analysis", ss["SectionTitle"]))
            elements.append(Paragraph(
                "Stocks held by 2 or more managers, sorted by overlap count.",
                ss["SmallText"]
            ))
            elements.append(Spacer(1, 0.15 * inch))
            ov_headers = ["Stock", "# Mgrs", "Avg %", "Sector", "Held By"]
            ov_rows = []
            for o in overlap[:30]:
                lbl = o.get("display_label", f"{o['name']} ({o['ticker']})")
                mgr_list = ", ".join(o["managers"][:5])
                if len(o["managers"]) > 5:
                    mgr_list += f" +{len(o['managers'])-5} more"
                ov_rows.append([
                    lbl[:30],
                    str(o["manager_count"]),
                    f"{o['avg_pct']:.1f}%",
                    (o.get("sector") or "—")[:18],
                    mgr_list,
                ])
            ov_widths = [2 * inch, 0.6 * inch, 0.6 * inch, 1.3 * inch, 5 * inch]
            ov_tbl = _make_table(ov_headers, ov_rows, col_widths=ov_widths)
            elements.append(ov_tbl)
    except ImportError:
        pass

    elements.append(PageBreak())

    # ── Sector Breakdown ──
    try:
        import analysis
        sector_data = analysis.compute_sector_breakdown(all_rows)
        sectors = sector_data.get("sectors", [])
        if sectors:
            elements.append(Paragraph("Sector Breakdown", ss["SectionTitle"]))
            sec_headers = ["Sector", "Total Value", "% of Portfolio", "# Holdings"]
            sec_rows = []
            for s in sectors[:15]:
                sec_rows.append([
                    s["name"],
                    f"${s['total_value']/1e6:.0f}M",
                    f"{s['pct']:.1f}%",
                    str(s["count"]),
                ])
            sec_widths = [2.5 * inch, 1.5 * inch, 1.2 * inch, 1 * inch]
            sec_tbl = _make_table(sec_headers, sec_rows, col_widths=sec_widths)
            elements.append(sec_tbl)

        industries = sector_data.get("industries", [])
        if industries:
            elements.append(Spacer(1, 0.3 * inch))
            elements.append(Paragraph("Industry Breakdown", ss["SectionTitle"]))
            ind_headers = ["Industry", "Sector", "Total Value", "% of Portfolio", "# Holdings"]
            ind_rows = []
            for ind in industries[:25]:
                ind_rows.append([
                    ind["name"][:30],
                    ind.get("sector", "—")[:18],
                    f"${ind['total_value']/1e6:.0f}M",
                    f"{ind['pct']:.1f}%",
                    str(ind["count"]),
                ])
            ind_widths = [2.5 * inch, 1.8 * inch, 1.2 * inch, 1.0 * inch, 0.8 * inch]
            ind_tbl = _make_table(ind_headers, ind_rows, col_widths=ind_widths)
            elements.append(ind_tbl)
    except ImportError:
        pass

    elements.append(PageBreak())

    # ── Geographic Exposure ──
    try:
        import analysis
        geo_data = analysis.compute_geo_treemap(all_rows)
        countries = geo_data.get("countries", [])
        if countries and len(countries) > 1:
            elements.append(Paragraph("Geographic Exposure", ss["SectionTitle"]))
            geo_headers = ["Country", "% of Portfolio", "# Holdings"]
            geo_rows = []
            for co in countries[:20]:
                geo_rows.append([
                    co["name"],
                    f"{co['pct']:.1f}%",
                    str(co["count"]),
                ])
            geo_widths = [3 * inch, 1.5 * inch, 1 * inch]
            geo_tbl = _make_table(geo_headers, geo_rows, col_widths=geo_widths)
            elements.append(geo_tbl)
    except ImportError:
        pass

    # ── Comparison Chart Images (Portfolio vs ACWI) ──
    try:
        acwi_data = None
        try:
            from financial_data import fetch_acwi_benchmark
            acwi_data = fetch_acwi_benchmark()
        except Exception:
            pass

        chart_elements = []

        # Build key takeaways for charts
        chart_insights = {}
        try:
            if exec_data and exec_data.get("findings"):
                for f in exec_data["findings"]:
                    fl = f.lower()
                    if "sector" in fl:
                        chart_insights["sector"] = f
                    if "geographic" in fl:
                        chart_insights["geo"] = f
                    if "concentration" in fl or "top 5" in fl:
                        chart_insights["holdings"] = f
                    if "valuation" in fl or "forward p/e" in fl:
                        chart_insights["valuation"] = f
                    if "expected return" in fl:
                        chart_insights["return"] = f
        except Exception:
            pass

        sector_img = _generate_sector_comparison(all_rows, acwi_data)
        if sector_img:
            chart_elements.append(Paragraph("Sector Allocation: Portfolio vs ACWI", ss["SectionTitle"]))
            if "sector" in chart_insights:
                chart_elements.append(Paragraph(
                    f'<font color="#475569"><i>{chart_insights["sector"]}</i></font>',
                    ss["KeyTakeaway"]))
            chart_elements.append(Image(sector_img, width=7.5 * inch, height=4.5 * inch))

        geo_img = _generate_geo_comparison(all_rows, acwi_data)
        if geo_img:
            chart_elements.append(Paragraph("Geographic Exposure: Portfolio vs ACWI", ss["SectionTitle"]))
            if "geo" in chart_insights:
                chart_elements.append(Paragraph(
                    f'<font color="#475569"><i>{chart_insights["geo"]}</i></font>',
                    ss["KeyTakeaway"]))
            chart_elements.append(Image(geo_img, width=7.5 * inch, height=4.5 * inch))

        ind_img = _generate_industry_bar(all_rows)
        if ind_img:
            chart_elements.append(Paragraph("Industry Breakdown", ss["SectionTitle"]))
            try:
                import analysis as _a
                _sd = _a.compute_sector_breakdown(all_rows)
                _inds = _sd.get("industries", [])
                if _inds:
                    _ti = _inds[0]
                    chart_elements.append(Paragraph(
                        f'<font color="#475569"><i>{_ti["name"]} leads at {_ti["pct"]:.1f}% weight — {len(_inds)} industries represented</i></font>',
                        ss["KeyTakeaway"]))
            except Exception:
                pass
            chart_elements.append(Image(ind_img, width=7.5 * inch, height=4.5 * inch))

        top_img = _generate_top_holdings_comparison(all_rows, acwi_data)
        if top_img:
            chart_elements.append(Paragraph("Top Holdings: Portfolio vs ACWI", ss["SectionTitle"]))
            if "holdings" in chart_insights:
                chart_elements.append(Paragraph(
                    f'<font color="#475569"><i>{chart_insights["holdings"]}</i></font>',
                    ss["KeyTakeaway"]))
            chart_elements.append(Image(top_img, width=7.5 * inch, height=4.5 * inch))

        if chart_elements:
            elements.append(PageBreak())
            elements.extend(chart_elements)
    except (ImportError, Exception):
        pass

    # ── Footer note ──
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(Paragraph(
        f"Report generated on {date_str}. Data sourced from SEC EDGAR 13F filings. "
        "Financial data enriched via yfinance. Past performance is not indicative of future results.",
        ss["SmallText"]
    ))

    # Build
    report_title = title
    doc.build(
        elements,
        onFirstPage=lambda c, d: _header_footer(c, d, report_title),
        onLaterPages=lambda c, d: _header_footer(c, d, report_title),
    )
    buf.seek(0)
    return buf


if __name__ == "__main__":
    main()
