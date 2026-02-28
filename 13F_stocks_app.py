"""
Investment Manager Holdings — Web UI (Enhanced)
=================================================
Flask app wrapping holdings.py with financial enrichment, interactive treemap,
overlap analysis, sector breakdown, presets, and QoQ diff.

    pip install flask edgartools yfinance matplotlib python-dateutil
    python 13F_stocks_app.py
    → http://localhost:8080
"""

import io
import json
import os
import queue
import re
import sys
import threading
import time
import zipfile
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
from datetime import datetime
from flask import Flask, jsonify, request, Response, send_from_directory, send_file

# ── Setup ─────────────────────────────────────────────────────────────────────

APP_DIR     = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, "holdings_config.json")

app = Flask(__name__, static_folder=os.path.join(APP_DIR, 'static'), static_url_path='/static')
progress_queues = {}
run_lock        = threading.Lock()
abort_flag      = threading.Event()
last_results    = {}
diff_results    = {}

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
import holdings  # noqa: E402
import db  # noqa: E402

db.init(os.path.join(APP_DIR, "holdings_history.db"))

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "client_name": "",
    "report_name": "",
    "enrich_financial": True,
    "managers_13f": {
        "Berkshire Hathaway":          "1067983",
        "BlackRock":                   "1364742",
        "Vanguard Group":              "102909",
        "State Street":                "93751",
        "Fidelity (FMR)":             "315066",
        "JPMorgan Chase":              "19617",
        "Goldman Sachs":               "886982",
        "Morgan Stanley":              "895421",
        "Capital Research & Mgmt":     "44204",
        "Wellington Management":       "902219",
        "T. Rowe Price":               "1428175",
        "Invesco":                     "914208",
        "Franklin Templeton":          "38777",
        "Northern Trust":              "73124",
        "Dimensional Fund Advisors":   "354204",
        "Geode Capital Management":    "1214717",
        "MFS Investment Management":   "63296",
        "AllianceBernstein":           "825313",
        "Nuveen":                      "1308557",
        "American Century":            "808250",
        "Dodge & Cox":                 "315693",
        "PIMCO":                       "811678",
        "Parametric Portfolio":        "1014143",
        "Janus Henderson":             "812295",
        "Charles Schwab":              "884546",
        "BNY Mellon":                  "1111565",
        "Amundi":                      "1424505",
        "Norges Bank (Norway)":        "1582202",
        "Canada Pension Plan":         "1444118",
        "GQG Partners":                "1822486",
        "Bridgewater Associates":      "1350694",
        "Millennium Management":       "1273087",
        "Citadel Advisors":            "1423053",
        "D.E. Shaw":                   "1009207",
        "Two Sigma Investments":       "1179392",
        "AQR Capital Management":      "1167557",
        "Renaissance Technologies":    "1037389",
        "Point72 Asset Management":    "1603466",
        "Baupost Group":               "1061768",
        "Viking Global":               "1103804",
        "Tiger Global":                "1167483",
        "Elliott Investment Mgmt":     "1791786",
        "Pershing Square Capital":     "1336528",
        "Third Point":                 "1040273",
        "Soros Fund Management":       "1029160",
        "Lone Pine Capital":           "1061165",
        "Coatue Management":           "1535392",
        "Marshall Wace":               "1325091",
        "Balyasny Asset Management":   "1218710",
        "Farallon Capital":            "1022455",
        "Appaloosa Management":        "1006438",
        "Man Group":                   "1513075",
        "Tudor Investment Corp":       "1067739",
        "Duquesne Family Office":      "1536411",
        "Greenlight Capital":          "1079114",
        "Paulson & Co":                "1035674",
        "Carl Icahn":                  "921669",
        "Starboard Value":             "1517137",
        "ValueAct Capital":            "1351069",
        "Jana Partners":               "1159159",
        "Sculptor Capital (Och-Ziff)": "1403256",
        "Oaktree Capital":             "949509",
        "Samlyn Capital":              "1421097",
        "Maverick Capital":            "934639",
        "Glenview Capital":            "1138995",
        "D1 Capital Partners":         "1747057",
        "Durable Capital Partners":    "1798849",
        "Darsana Capital Partners":    "1609098",
        "Altimeter Capital":           "1541617",
        "Whale Rock Capital":          "1632361",
        "Light Street Capital":        "1563880",
        "Dragoneer Investment":        "1683627",
        "Alkeon Capital":              "1094401",
        "Matrix Capital Management":   "1061219",
        "Kensico Capital":             "1141913",
        "Soroban Capital Partners":    "1551409",
        "Senator Investment Group":    "1458522",
        "Eminence Capital":            "1320622",
        "Suvretta Capital":            "1569777",
        "Hound Partners":              "1322988",
        "Adage Capital Management":    "1099281",
        "Cantillon Capital":           "1279107",
        "Select Equity Group":         "1034524",
        "Akre Capital Management":     "1112520",
        "Discovery Capital Mgmt":      "1372846",
        "Sachem Head Capital":         "1559965",
        "Saba Capital Management":     "1407600",
        "Inclusive Capital":           "1812720",
        "Abdiel Capital":              "1649931",
        "Aspex Management":            "1768375",
        "Spyglass Capital":            "1654344",
        "CC&L Q ACWI":                 "1596800",
        "Arrowstreet Capital":         "1164508",
        "Hillhouse Capital":           "1510455",
        "Caxton Associates":           "1121825",
        "Highbridge Capital Mgmt":     "1040280",
        "King Street Capital":         "1124917",
        "Lansdowne Partners":          "1284751",
        "Magnetar Capital":            "1369834",
        "Davidson Kempner":            "1224961",
        "Avenue Capital Group":        "1075651",
        "Centerbridge Partners":       "1408930",
        "York Capital Management":     "1054034",
        "PAR Capital Management":      "1127106",
        "Pzena Investment Mgmt":       "1399067",
        "Silver Point Capital":        "1379479",
        "MSD Partners":                "1582541",
        "Fortress Investment":         "1380393",
        "Cerberus Capital":            "1371622",
        "Anchorage Capital Group":     "1357775",
        "Winton Group":                "1457414",
        "GMO (Grantham Mayo)":         "846222",
        "Egerton Capital":             "1388978",
        "Brandes Investment":          "879448",
        "Harris Associates":           "759529",
        "Southeastern Asset Mgmt":     "807985",
        "First Pacific Advisors":      "1389582",
        "Tweedy Browne":               "889548",
        "Third Avenue Management":     "773218",
        "Loews Corp":                  "60714",
        "Markel Corp":                 "700923",
        "Fairfax Financial":           "915191",
        "WorldQuant":                  "1362952",
        "PDT Partners":                "1584529",
        "Schonfeld Strategic":         "1579880",
        "ExodusPoint Capital":         "1768504",
        "Hudson Bay Capital":          "1544969",
        "Graham Capital Mgmt":         "1273823",
        "Capula Investment Mgmt":      "1654782",
        "Rokos Capital Management":    "1730547",
        "Verition Fund Management":    "1571823",
        "Diameter Capital":            "1727489",
        "Steadfast Capital":           "1259305",
        "Brookside Capital":           "1161078",
        "Luxor Capital Group":         "1280011",
        "Hitchwood Capital":           "1598886",
        "Rock Springs Capital":        "1461027",
        "Parnassus Investments":       "929651",
        "ClearBridge Investments":     "1098889",
        "Artisan Partners":            "1142942",
        "Loomis Sayles":               "1364606",
        "Neuberger Berman":            "1114852",
        "Lazard Asset Management":     "1137480",
        "Epoch Investment Partners":   "1309077",
        "First Eagle Investment":      "1119389",
        "Harding Loevner":             "1074902",
        "Jennison Associates":         "1017170",
        "Polen Capital":               "1671038",
        "Brown Advisory":              "1472552",
        "Ruane Cunniff & Goldfarb":    "767218",
        "Winslow Capital Mgmt":        "1061614",
        "Vulcan Value Partners":       "1519455",
        "Giverny Capital":             "1596416",
        "Trian Fund Management":       "1345495",
        "Corvex Management":           "1575572",
        "Cevian Capital":              "1515099",
        "TCI Fund Management":         "1647251",
        "Engaged Capital":             "1621581",
        "Marcato Capital Mgmt":        "1545927",
        "Sarissa Capital":             "1583483",
        "Angelo Gordon":               "1547903",
        "Apollo Management":           "1411494",
        "Ares Management":             "1555280",
        "Bain Capital Public Equity":  "1543160",
        "Carlyle Group":               "1527166",
        "KKR & Co":                    "1404912",
        "TPG Capital":                 "1645498",
        "Vista Equity Partners":       "1498070",
        "General Atlantic":            "1735006",
        "Tiger Management (legacy)":   "1031389",
        "Permira":                     "1631024",
        "Warburg Pincus":              "1373467",
        "Silver Lake":                 "1649363",
        "Thoma Bravo":                 "1821704",
        "Hellman & Friedman":          "1336478",
    },
    "managers_nport": {},
    "top_n": 20,
    "max_date": "2025-12-31",
    "identity": "Investment Manager Holdings holdings@example.com",
    "fmp_api_key": "",  # deprecated, kept for config compat
    "presets": {},
}

# Save built-in managers as a preset, then start with empty config
_BUILTIN_MANAGERS = dict(DEFAULT_CONFIG["managers_13f"])
DEFAULT_CONFIG["managers_13f"] = {}
DEFAULT_CONFIG["manager_weights"] = {}
DEFAULT_CONFIG["presets"] = {
    "All Major Managers": {"managers_13f": _BUILTIN_MANAGERS, "manager_weights": {}}
}

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        # Ensure built-in presets are always available
        for pk, pv in DEFAULT_CONFIG.get("presets", {}).items():
            cfg["presets"].setdefault(pk, pv)
        return cfg
    return json.loads(json.dumps(DEFAULT_CONFIG))

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

# ── Fetch engine ──────────────────────────────────────────────────────────────

def run_fetch(cfg, q):
    import importlib
    importlib.reload(holdings)
    from edgar import set_identity
    set_identity(cfg.get("identity", DEFAULT_CONFIG["identity"]))
    holdings.MAX_DATE   = cfg.get("max_date", "2025-12-31")
    holdings.TOP_N      = cfg.get("top_n", 20)
    holdings.OUTPUT_DIR = APP_DIR

    combined, run_date = [], datetime.today().strftime("%Y%m%d")
    res = {"managers": [], "files": [], "errors": [], "all_rows": [], "run_date": run_date}
    def log(msg):
        q.put({"type": "log", "message": msg})

    managers = cfg.get("managers_13f", {})
    total = len(managers) + len(cfg.get("managers_nport", {}))
    done = 0

    log(f"Fetching {total} managers — MAX_DATE: {holdings.MAX_DATE} | TOP_N: {holdings.TOP_N}")

    for name, cik in managers.items():
        if abort_flag.is_set():
            log("Aborted by user")
            break
        q.put({"type": "manager_start", "name": name})
        log(f"[13F] {name} (CIK {cik})...")
        try:
            period, filed, n, rows = holdings.fetch_13f(name, cik)
            log(f"  OK {n} positions | Period: {period}")
            combined.extend(rows)
            res["managers"].append({"name": name, "status": "success",
                                    "period": period, "filed_at": filed,
                                    "positions": n})
            q.put({"type": "manager_done", "name": name, "status": "success", "positions": n})
        except Exception as e:
            log(f"  Error: {e}")
            res["managers"].append({"name": name, "status": "error", "error": str(e)})
            res["errors"].append(f"{name}: {e}")
            q.put({"type": "manager_done", "name": name, "status": "error", "error": str(e)})
        done += 1
        q.put({"type": "progress", "done": done, "total": total})

    for name, info in cfg.get("managers_nport", {}).items():
        if abort_flag.is_set():
            log("Aborted by user")
            break
        q.put({"type": "manager_start", "name": name})
        log(f"[N-PORT] {name} (CIK {info['cik']})...")
        try:
            period, filed, n, rows = holdings.fetch_nport(name, info["cik"], info["series_keyword"])
            log(f"  OK {n} holdings | Period: {period}")
            combined.extend(rows)
            res["managers"].append({"name": name, "status": "success",
                                    "period": period, "filed_at": filed,
                                    "positions": n})
            q.put({"type": "manager_done", "name": name, "status": "success", "positions": n})
        except Exception as e:
            log(f"  Error: {e}")
            res["managers"].append({"name": name, "status": "error", "error": str(e)})
            res["errors"].append(f"{name}: {e}")
            q.put({"type": "manager_done", "name": name, "status": "error", "error": str(e)})
        done += 1
        q.put({"type": "progress", "done": done, "total": total})

    # ── Financial enrichment ──
    if combined and cfg.get("enrich_financial", True):
        try:
            import financial_data
            unique_tickers = list({r["ticker"] for r in combined if r["ticker"] != "N/A"})
            log(f"Enriching {len(unique_tickers)} unique tickers with financial data...")
            q.put({"type": "enrich_start", "total": len(unique_tickers)})

            periods = [r["period_of_report"] for r in combined if r.get("period_of_report")]
            quarter_end = Counter(periods).most_common(1)[0][0] if periods else "2025-09-30"

            enrichment = financial_data.batch_fetch_financial_data(
                unique_tickers, quarter_end, max_workers=10,
                progress_callback=lambda d, t: (
                    q.put({"type": "enrich_progress", "done": d, "total": t}),
                    log(f"  Enriched {d}/{t} tickers") if d % 20 == 0 or d == t else None,
                )
            )

            for row in combined:
                data = enrichment.get(row["ticker"], {})
                # Prior quarter
                row["prior_price_qtr_end"] = data.get("prior_price_qtr_end")
                row["prior_quarter_return_pct"] = data.get("prior_quarter_return_pct")
                row["prior_reported_eps"] = data.get("prior_reported_eps")
                row["prior_consensus_eps"] = data.get("prior_consensus_eps")
                row["prior_eps_beat_dollars"] = data.get("prior_eps_beat_dollars")
                row["prior_eps_beat_pct"] = data.get("prior_eps_beat_pct")
                # Filing quarter
                row["filing_price_qtr_end"] = data.get("filing_price_qtr_end")
                row["filing_quarter_return_pct"] = data.get("filing_quarter_return_pct")
                row["filing_reported_eps"] = data.get("filing_reported_eps")
                row["filing_consensus_eps"] = data.get("filing_consensus_eps")
                row["filing_eps_beat_dollars"] = data.get("filing_eps_beat_dollars")
                row["filing_eps_beat_pct"] = data.get("filing_eps_beat_pct")
                # Current / live
                row["forward_pe"] = data.get("forward_pe")
                row["forward_eps_growth"] = data.get("forward_eps_growth")
                row["dividend_yield"] = data.get("dividend_yield")
                row["trailing_eps"] = data.get("trailing_eps")
                row["forward_eps"] = data.get("forward_eps")
                # QTD
                row["qtd_return_pct"] = data.get("qtd_return_pct")
                row["qtd_price_start"] = data.get("qtd_price_start")
                # Static
                row["sector"] = data.get("sector")
                row["industry"] = data.get("industry")
                row["country"] = data.get("country")

            # Apply name-based sector fallback for rows still missing sector
            # (especially N/A-ticker rows from NPORT that never got enriched)
            from financial_data import lookup_sector_fallback
            fallback_count = 0
            for row in combined:
                if not row.get("sector"):
                    fb_s, fb_i, fb_c = lookup_sector_fallback(
                        row.get("ticker", ""), row.get("name", ""))
                    if fb_s:
                        row["sector"] = fb_s
                        row["industry"] = row.get("industry") or fb_i
                        row["country"] = row.get("country") or fb_c
                        fallback_count += 1
            if fallback_count:
                log(f"  Applied sector fallback for {fallback_count} holdings")

            log(f"Financial enrichment complete for {len(unique_tickers)} tickers")
        except ImportError:
            log("yfinance not installed — skipping financial enrichment")
        except Exception as e:
            log(f"Enrichment error: {e}")

    # ── Generate output files ──
    if combined:
        # Write individual XLSX per manager
        by_mgr = defaultdict(list)
        for r in combined:
            by_mgr[r["manager"]].append(r)
        for mgr_name, mgr_rows in by_mgr.items():
            try:
                period = mgr_rows[0]["period_of_report"]
                fname = holdings.write_individual_xlsx(mgr_name, period, mgr_rows)
                res["files"].append(fname)
                # Update manager result with file
                for m in res["managers"]:
                    if m["name"] == mgr_name and m["status"] == "success":
                        m["file"] = fname
            except Exception as e:
                log(f"  XLSX error for {mgr_name}: {e}")

        cname = holdings.write_combined_xlsx(combined, run_date)
        res["files"].append(cname)
        log(f"Combined Excel: {cname} ({len(combined)} rows)")

        # Write weighted portfolio XLSX
        try:
            weights = cfg.get("manager_weights", {})
            wname = holdings.write_weighted_xlsx(combined, run_date, weights)
            res["files"].append(wname)
            log(f"Weighted Portfolio: {wname}")
        except Exception as e:
            log(f"  Weighted XLSX error: {e}")

        log("Generating report PowerPoint...")
        try:
            pptx_name = holdings.write_report_pptx(
                combined, run_date,
                client_name=cfg.get("client_name", ""),
                report_name=cfg.get("report_name", ""))
            if pptx_name:
                res["files"].append(pptx_name)
                log(f"Report PPTX: {pptx_name}")
        except Exception as e:
            log(f"  PPTX error: {e}")

    res["all_rows"] = combined
    aborted = abort_flag.is_set()
    log("Stopped by user." if aborted else "Done!")
    q.put({"type": "complete", "aborted": aborted, "results": {k: v for k, v in res.items() if k != "all_rows"}})
    return res

# ── EDGAR company search ──────────────────────────────────────────────────────

# Build curated lookup from built-in managers for instant search
KNOWN_FUNDS = {}
for _name, _cik in _BUILTIN_MANAGERS.items():
    KNOWN_FUNDS[_name.lower()] = {"name": _name, "cik": _cik}


_edgar_form_cache = {}  # (query_lower, form_type) -> {"results": [...], "ts": time}

def _search_edgar_by_form(query, form_type="13F-HR"):
    """Search EDGAR ATOM feed for companies, optionally filtered by form type."""
    cache_key = (query.lower(), form_type)
    now = time.time()
    if cache_key in _edgar_form_cache and (now - _edgar_form_cache[cache_key]["ts"]) < 900:
        return _edgar_form_cache[cache_key]["results"]
    ua = "Investment Manager Holdings holdings@example.com"
    type_param = f"&type={form_type}" if form_type else ""
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar?"
        f"company={urllib.parse.quote(query)}&CIK={type_param}&dateb="
        "&owner=include&count=10&search_text=&action=getcompany&output=atom"
    )
    req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        xml_data = resp.read().decode("utf-8", errors="replace")

    root = ET.fromstring(xml_data)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    matches, seen = [], set()

    for entry in root.findall("a:entry", ns):
        title_el   = entry.find("a:title", ns)
        summary_el = entry.find("a:summary", ns)
        link_el    = entry.find("a:link", ns)
        if title_el is None:
            continue
        raw = title_el.text or ""
        clean = re.sub(r"<[^>]+>", "", raw).strip()
        cik = ""
        m = re.search(r"\((\d{7,10})\)", clean)
        if m:
            cik = str(int(m.group(1)))
        if not cik and summary_el is not None and summary_el.text:
            m2 = re.search(r"CIK[=:\s]+(\d{7,10})", re.sub(r"<[^>]+>", "", summary_el.text))
            if m2:
                cik = str(int(m2.group(1)))
        if not cik and link_el is not None:
            m3 = re.search(r"CIK=(\d+)", link_el.get("href", ""))
            if m3:
                cik = str(int(m3.group(1)))
        name = re.sub(r"\s*\(\d{7,10}\)\s*(\(CIK\))?\s*$", "", clean).strip()
        if not name:
            name = clean
        if cik and cik not in seen:
            seen.add(cik)
            matches.append({"name": name, "cik": cik})

    _edgar_form_cache[cache_key] = {"results": matches, "ts": now}
    return matches


def _search_edgar_13f(query):
    """
    Multi-source search: curated list + EDGAR 13F-HR + broad EDGAR + direct CIK.
    """
    results = []
    seen_ciks = set()

    # 1) Search curated list (instant, always works)
    q_lower = query.lower().strip()
    for key, info in KNOWN_FUNDS.items():
        if q_lower in key or q_lower in info["name"].lower():
            if info["cik"] not in seen_ciks:
                seen_ciks.add(info["cik"])
                results.append({"name": info["name"], "cik": info["cik"]})

    # 2) EDGAR search with form type 13F-HR
    try:
        for r in _search_edgar_by_form(query, "13F-HR"):
            if r["cik"] not in seen_ciks:
                seen_ciks.add(r["cik"])
                results.append(r)
    except Exception:
        pass

    # 3) If few results, broader EDGAR search (all form types)
    if len(results) < 5:
        try:
            for r in _search_edgar_by_form(query, ""):
                if r["cik"] not in seen_ciks:
                    seen_ciks.add(r["cik"])
                    results.append(r)
        except Exception:
            pass

    # 4) Direct CIK lookup if query is numeric
    if query.strip().isdigit() and query.strip() not in seen_ciks:
        try:
            from edgar import Company
            c = Company(query.strip())
            results.append({"name": c.name, "cik": query.strip()})
        except Exception:
            pass

    return results[:15]

# ── N-PORT series discovery ───────────────────────────────────────────────────

def _search_edgar_nport(query):
    """Search EDGAR for companies with NPORT-P filings."""
    try:
        return _search_edgar_by_form(query, "NPORT-P")
    except Exception:
        return []


def _fetch_nport_series(cik):
    """Fetch available fund series names from recent NPORT-P filings for a CIK."""
    from edgar import Company
    company = Company(cik)
    filings = company.get_filings(form="NPORT-P")
    if not filings or len(filings) == 0:
        return []

    series_names = set()
    ns = {"nport": "http://www.sec.gov/edgar/nport"}

    count = 0
    for f in filings:
        if count >= 5:
            break
        try:
            xml_content = None
            for att in f.attachments:
                if hasattr(att, "is_xml") and att.is_xml:
                    xml_content = att.download()
                    break
                elif hasattr(att, "document") and att.document.endswith(".xml"):
                    xml_content = att.download()
                    break
            if not xml_content:
                count += 1
                continue
            if isinstance(xml_content, bytes):
                xml_content = xml_content.decode("utf-8", errors="replace")
            root = ET.fromstring(xml_content)

            series_el = root.find(".//nport:seriesName", ns)
            if series_el is None:
                series_el = root.find(".//{http://www.sec.gov/edgar/nport}seriesName")
            if series_el is None:
                series_el = root.find(".//seriesName")
            if series_el is not None and series_el.text:
                series_names.add(series_el.text.strip())
        except Exception:
            pass
        count += 1

    return sorted(series_names)


# ── Mutual Fund Ticker Search (SEC company_tickers_mf.json) ─────────────────

_mf_data = None           # {"by_ticker": {SYM: [{cik, series_id, class_id}]}, "by_cik": {cik: [...]}}
_mf_data_lock = threading.Lock()
_mf_load_time = 0

_series_name_cache = {}   # cik -> {series_id: {"name": ..., "classes": [{"id","ticker","name"}]}}
_series_name_lock = threading.Lock()


def _load_mf_tickers():
    """Download and cache SEC mutual fund ticker data (28K+ entries)."""
    global _mf_data, _mf_load_time
    with _mf_data_lock:
        # Re-use cache for 1 hour
        if _mf_data is not None and (time.time() - _mf_load_time) < 3600:
            return _mf_data
    try:
        ua = "Investment Manager Holdings holdings@example.com"
        req = urllib.request.Request(
            "https://www.sec.gov/files/company_tickers_mf.json",
            headers={"User-Agent": ua})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        # Format: {"fields": ["cik","seriesId","classId","symbol"], "data": [[cik, sid, cid, sym], ...]}
        fields = raw.get("fields", [])
        rows = raw.get("data", [])
        idx_cik = fields.index("cik") if "cik" in fields else 0
        idx_sid = fields.index("seriesId") if "seriesId" in fields else 1
        idx_cid = fields.index("classId") if "classId" in fields else 2
        idx_sym = fields.index("symbol") if "symbol" in fields else 3

        by_ticker = {}  # "GQRIX" -> [{cik, series_id, class_id}]
        by_cik = {}     # "1593547" -> [{series_id, class_id, symbol}]
        for row in rows:
            cik = str(row[idx_cik])
            sid = row[idx_sid] or ""
            cid = row[idx_cid] or ""
            sym = (row[idx_sym] or "").strip().upper()
            if not sym:
                continue
            entry_t = {"cik": cik, "series_id": sid, "class_id": cid}
            entry_c = {"series_id": sid, "class_id": cid, "symbol": sym}
            by_ticker.setdefault(sym, []).append(entry_t)
            by_cik.setdefault(cik, []).append(entry_c)

        result = {"by_ticker": by_ticker, "by_cik": by_cik}
        with _mf_data_lock:
            _mf_data = result
            _mf_load_time = time.time()
        return result
    except Exception:
        return {"by_ticker": {}, "by_cik": {}}


def _fetch_series_names(cik):
    """Fetch series/class names and tickers for a CIK from EDGAR browse-edgar?scd=series.
    Returns {series_id: {"name": ..., "classes": [{"id","ticker"}], "company": ...}}
    """
    with _series_name_lock:
        if cik in _series_name_cache:
            return _series_name_cache[cik]
    try:
        padded = str(cik).zfill(10)
        ua = "Investment Manager Holdings holdings@example.com"
        url = (f"https://www.sec.gov/cgi-bin/browse-edgar?"
               f"action=getcompany&CIK={padded}&scd=series&owner=include&output=atom")
        req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_data = resp.read().decode("utf-8", errors="replace")

        # The XML structure is:
        # <feed><entry><content><company-info><name>...<sids><sid id="S...">
        #   <cids><cid id="C..."><class-name>...<ticker>...</cid></cids>
        #   <series-name>...</series-name>
        # </sid></sids></company-info></content></entry></feed>

        series_map = {}
        company_name = ""

        # Parse with regex (more reliable than ET for this mixed-namespace doc)
        # Get company name
        name_m = re.search(r'<company-info>\s*<cik>[^<]*</cik>\s*<name>([^<]*)</name>', xml_data)
        if name_m:
            company_name = name_m.group(1).strip()

        # Find all <sid> blocks
        sid_pattern = re.compile(
            r'<sid\s+id="(S\d+)">(.*?)</sid>',
            re.DOTALL)
        for sid_m in sid_pattern.finditer(xml_data):
            sid = sid_m.group(1)
            block = sid_m.group(2)

            # Get series name
            sname_m = re.search(r'<series-name>([^<]*)</series-name>', block)
            sname = sname_m.group(1).strip() if sname_m else ""
            # Unescape HTML entities
            sname = sname.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

            # Get classes with tickers
            classes = []
            cid_pattern = re.compile(
                r'<cid\s+id="(C\d+)">(.*?)</cid>',
                re.DOTALL)
            for cid_m in cid_pattern.finditer(block):
                cid = cid_m.group(1)
                cblock = cid_m.group(2)
                ticker_m = re.search(r'<ticker>([^<]*)</ticker>', cblock)
                ticker = ticker_m.group(1).strip().upper() if ticker_m else ""
                if ticker:
                    classes.append({"id": cid, "ticker": ticker})

            series_map[sid] = {"name": sname, "classes": classes, "company": company_name}

        with _series_name_lock:
            _series_name_cache[cik] = series_map
        return series_map
    except Exception:
        return {}


def _search_efts_nport(query):
    """Search SEC EFTS for NPORT-P filings matching a query. Returns list of {cik, name}."""
    try:
        ua = "Investment Manager Holdings holdings@example.com"
        encoded_q = urllib.parse.quote(query)
        url = (f"https://efts.sec.gov/LATEST/search-index?"
               f"q={encoded_q}&forms=NPORT-P&dateRange=custom"
               f"&startdt=2024-01-01&enddt=2026-12-31")
        req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        hits = data.get("hits", {}).get("hits", [])
        seen = set()
        results = []
        for hit in hits:
            src = hit.get("_source", {})
            cik_str = ""
            for key in ("ciks", "entity_cik"):
                v = src.get(key)
                if v:
                    if isinstance(v, list) and v:
                        cik_str = str(v[0])
                    else:
                        cik_str = str(v)
                    break
            name = ""
            for key in ("display_names", "entity_name"):
                v = src.get(key)
                if v:
                    name = v[0] if isinstance(v, list) else str(v)
                    break
            if cik_str and cik_str not in seen:
                seen.add(cik_str)
                results.append({"cik": cik_str, "name": name})
        return results[:10]
    except Exception:
        return []


def _search_mutual_funds(query):
    """
    Search for mutual funds by ticker or name.
    Returns list of {name, cik, type, ticker, series_id, series_keyword}.
    """
    query = query.strip()
    if not query:
        return []

    results = []
    seen = set()  # (cik, series_id) dedup

    q_upper = query.upper()
    q_lower = query.lower()

    # 1) Ticker lookup in SEC mutual fund tickers
    mf = _load_mf_tickers()
    ticker_hits = []

    # Exact match
    if q_upper in mf["by_ticker"]:
        ticker_hits.extend(mf["by_ticker"][q_upper])

    # Prefix match (if query is 2+ chars and looks like a ticker)
    if len(query) >= 2 and query.replace(" ", "").isalpha():
        for sym, entries in mf["by_ticker"].items():
            if sym.startswith(q_upper) and sym != q_upper:
                ticker_hits.extend(entries)
                if len(ticker_hits) > 20:
                    break

    # Resolve ticker hits to named results
    for hit in ticker_hits:
        cik = hit["cik"]
        sid = hit["series_id"]
        key = (cik, sid)
        if key in seen:
            continue
        seen.add(key)

        # Get series name and tickers
        series_info = _fetch_series_names(cik)
        s = series_info.get(sid, {})
        series_name = s.get("name", "")
        company_name = s.get("company", "")
        tickers = [c["ticker"] for c in s.get("classes", []) if c.get("ticker")]
        display_ticker = tickers[0] if tickers else ""

        # Also find the specific ticker that matched
        for t in tickers:
            if t == q_upper or t.startswith(q_upper):
                display_ticker = t
                break

        fund_name = series_name or company_name or f"Fund CIK {cik}"
        results.append({
            "name": fund_name,
            "cik": cik,
            "type": "NPORT",
            "ticker": display_ticker,
            "series_id": sid,
            "series_keyword": series_name,  # for matching in NPORT XML
        })

    # 2) EFTS name search (for non-ticker queries)
    if len(results) < 5 and len(query) >= 3:
        efts_hits = _search_efts_nport(query)
        for eh in efts_hits:
            cik = eh["cik"]
            # Get series info for this CIK
            series_info = _fetch_series_names(cik)
            for sid, s in series_info.items():
                key = (cik, sid)
                if key in seen:
                    continue
                sname = s.get("name", "")
                # Match: query words must appear in series name
                if sname and all(w in sname.lower() for w in q_lower.split()):
                    seen.add(key)
                    tickers = [c["ticker"] for c in s.get("classes", []) if c.get("ticker")]
                    results.append({
                        "name": sname,
                        "cik": cik,
                        "type": "NPORT",
                        "ticker": tickers[0] if tickers else "",
                        "series_id": sid,
                        "series_keyword": sname,
                    })
            if len(results) >= 15:
                break

    # 3) Fallback: existing EDGAR company search for NPORT-P filers
    if len(results) < 3:
        try:
            for r in _search_edgar_by_form(query, "NPORT-P"):
                cik = r["cik"]
                if not any(cik == x["cik"] for x in results):
                    results.append({
                        "name": r["name"],
                        "cik": cik,
                        "type": "NPORT",
                        "ticker": "",
                        "series_id": "",
                        "series_keyword": "",
                    })
        except Exception:
            pass

    return results[:15]


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(load_config())

@app.route("/api/config", methods=["POST"])
def api_set_config():
    cfg = load_config()
    cfg.update(request.json)
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/managers", methods=["POST"])
def api_add_manager():
    cfg  = load_config()
    data = request.json
    name = data.get("name", "").strip()
    cik  = data.get("cik", "").strip()
    if not name or not cik:
        return jsonify({"error": "Name and CIK required"}), 400
    cfg["managers_13f"][name] = cik
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/managers", methods=["DELETE"])
def api_del_manager():
    cfg  = load_config()
    name = request.json.get("name", "")
    cfg["managers_13f"].pop(name, None)
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/managers-nport", methods=["POST"])
def api_add_nport_manager():
    cfg  = load_config()
    data = request.json
    name = data.get("name", "").strip()
    cik  = data.get("cik", "").strip()
    series_keyword = data.get("series_keyword", "").strip()
    if not name or not cik or not series_keyword:
        return jsonify({"error": "Name, CIK, and series keyword required"}), 400
    cfg["managers_nport"][name] = {"cik": cik, "series_keyword": series_keyword}
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/managers-nport", methods=["DELETE"])
def api_del_nport_manager():
    cfg  = load_config()
    name = request.json.get("name", "")
    cfg["managers_nport"].pop(name, None)
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/search-nport-company")
def api_search_nport():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    try:
        return jsonify(_search_edgar_nport(q))
    except Exception:
        return jsonify([])

@app.route("/api/nport-series/<cik>")
def api_nport_series(cik):
    try:
        series = _fetch_nport_series(cik)
        return jsonify({"series": series})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/run", methods=["POST"])
def api_run():
    if not run_lock.acquire(blocking=False):
        return jsonify({"error": "Already running"}), 409
    abort_flag.clear()
    cfg    = load_config()
    run_id = str(int(time.time()))
    q      = queue.Queue()
    progress_queues[run_id] = q

    def _go():
        global last_results
        try:
            last_results = run_fetch(cfg, q)
            # Persist to SQLite for historical tracking
            if last_results.get("all_rows"):
                try:
                    rid = db.save_run(last_results, cfg)
                    if rid:
                        last_results["db_run_id"] = rid
                except Exception as e:
                    q.put({"type": "log", "message": f"DB save warning: {e}"})
        finally:
            run_lock.release()

    threading.Thread(target=_go, daemon=True).start()
    return jsonify({"run_id": run_id})

@app.route("/api/stream/<run_id>")
def api_stream(run_id):
    q = progress_queues.get(run_id)
    if not q:
        return jsonify({"error": "Unknown run"}), 404
    def gen():
        while True:
            try:
                msg = q.get(timeout=120)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") == "complete":
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type':'heartbeat'})}\n\n"
    return Response(gen(), mimetype="text/event-stream")

@app.route("/api/stop", methods=["POST"])
def api_stop():
    abort_flag.set()
    return jsonify({"ok": True})

@app.route("/api/results")
def api_results():
    return jsonify({k: v for k, v in last_results.items() if k != "all_rows"})

@app.route("/api/treemap-data")
def api_treemap():
    cfg = load_config()
    weights = cfg.get("manager_weights", {})
    by_mgr = defaultdict(list)
    for r in last_results.get("all_rows", []):
        tk = r.get("ticker", "N/A")
        dlabel = f"{r['name']} ({tk})" if tk != "N/A" else r["name"]
        by_mgr[r["manager"]].append({
            "name": r["name"], "ticker": tk,
            "display_label": dlabel,
            "pct": r["pct_of_portfolio"], "value": r.get("value_usd", 0),
            "filing_quarter_return": r.get("filing_quarter_return_pct"),
            "prior_quarter_return": r.get("prior_quarter_return_pct"),
            "qtd_return": r.get("qtd_return_pct"),
            "forward_pe": r.get("forward_pe"),
            "sector": r.get("sector"),
            "country": r.get("country"),
        })
    managers = []
    for m, s in by_mgr.items():
        managers.append({"manager": m, "weight": weights.get(m, 0), "stocks": s})
    return jsonify({"managers": managers})

@app.route("/api/summary-data")
def api_summary():
    try:
        import analysis
        cfg = load_config()
        weights = cfg.get("manager_weights", {})
        stats = analysis.compute_summary_stats(
            last_results.get("all_rows", []),
            manager_weights=weights if any(v > 0 for v in weights.values()) else None
        )
        return jsonify(stats)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/valuation-scatter")
def api_valuation_scatter():
    try:
        import analysis
        cfg = load_config()
        weights = cfg.get("manager_weights", {})
        mw = weights if any(v > 0 for v in weights.values()) else None
        data = analysis.compute_top_stocks_valuation(
            last_results.get("all_rows", []), manager_weights=mw
        )
        return jsonify(data)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/overlap-data")
def api_overlap():
    try:
        import analysis
        overlap = analysis.compute_overlap(last_results.get("all_rows", []))
        return jsonify({"overlap": overlap})
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/sector-data")
def api_sector():
    try:
        import analysis
        sectors = analysis.compute_sector_breakdown(last_results.get("all_rows", []))
        # Normalize industry parent-sector names so JS can match ACWI sector keys
        try:
            from financial_data import normalize_sector_name
            for ind in sectors.get("industries", []):
                ind["sector"] = normalize_sector_name(ind.get("sector", ""))
        except ImportError:
            pass
        return jsonify(sectors)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/geo-data")
def api_geo():
    try:
        import analysis
        geo = analysis.compute_geo_breakdown(last_results.get("all_rows", []))
        return jsonify(geo)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/category-stocks")
def api_category_stocks():
    """Get top stocks in a sector/industry/country for drill-down."""
    cat_type = request.args.get("type")
    cat_name = request.args.get("name")
    if not cat_type or not cat_name:
        return jsonify({"error": "type and name required"}), 400
    if cat_type not in ("sector", "industry", "country"):
        return jsonify({"error": "type must be sector, industry, or country"}), 400
    try:
        import analysis
        normalize_fn = None
        if cat_type == "sector":
            from financial_data import normalize_sector_name
            normalize_fn = normalize_sector_name
        elif cat_type == "country":
            from financial_data import normalize_country_name
            normalize_fn = normalize_country_name
        cfg = load_config()
        weights = cfg.get("manager_weights", {})
        data = analysis.compute_category_stocks(
            last_results.get("all_rows", []),
            cat_type, cat_name,
            manager_weights=weights if any(v > 0 for v in weights.values()) else None,
            normalize_fn=normalize_fn,
        )
        return jsonify(data)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/acwi-benchmark")
def api_acwi_benchmark():
    try:
        from financial_data import fetch_acwi_benchmark, normalize_sector_name, normalize_country_name
        data = fetch_acwi_benchmark()
        if data is None:
            return jsonify({"error": "ACWI data unavailable"}), 503
        # Normalize sector keys to GICS standard
        if "sectors" in data:
            norm = {}
            for k, v in data["sectors"].items():
                gics = normalize_sector_name(k)
                norm[gics] = norm.get(gics, 0) + v
            data["sectors"] = norm
        # Normalize country keys to iShares standard
        if "countries" in data:
            norm_c = {}
            for k, v in data["countries"].items():
                nk = normalize_country_name(k)
                norm_c[nk] = norm_c.get(nk, 0) + v
            data["countries"] = norm_c
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sector-treemap-data")
def api_sector_treemap():
    try:
        import analysis
        cfg = load_config()
        weights = cfg.get("manager_weights", {})
        data = analysis.compute_sector_treemap(
            last_results.get("all_rows", []),
            manager_weights=weights if any(v > 0 for v in weights.values()) else None
        )
        return jsonify(data)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

@app.route("/api/geo-treemap-data")
def api_geo_treemap():
    try:
        import analysis
        cfg = load_config()
        weights = cfg.get("manager_weights", {})
        data = analysis.compute_geo_treemap(
            last_results.get("all_rows", []),
            manager_weights=weights if any(v > 0 for v in weights.values()) else None
        )
        return jsonify(data)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

# ── Presets ──

@app.route("/api/presets", methods=["GET"])
def api_get_presets():
    cfg = load_config()
    return jsonify({"presets": list(cfg.get("presets", {}).keys())})

@app.route("/api/presets", methods=["POST"])
def api_save_preset():
    cfg = load_config()
    data = request.json
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Preset name required"}), 400
    cfg.setdefault("presets", {})[name] = {
        "managers_13f": dict(cfg.get("managers_13f", {})),
        "managers_nport": dict(cfg.get("managers_nport", {})),
        "manager_weights": dict(cfg.get("manager_weights", {})),
    }
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/presets/<name>", methods=["DELETE"])
def api_del_preset(name):
    cfg = load_config()
    cfg.get("presets", {}).pop(name, None)
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/presets/<name>/load", methods=["POST"])
def api_load_preset(name):
    cfg = load_config()
    preset = cfg.get("presets", {}).get(name)
    if not preset:
        return jsonify({"error": "Preset not found"}), 404
    cfg["managers_13f"] = dict(preset.get("managers_13f", {}))
    cfg["managers_nport"] = dict(preset.get("managers_nport", {}))
    cfg["manager_weights"] = dict(preset.get("manager_weights", {}))
    save_config(cfg)
    return jsonify({"ok": True})

# ── Manager Weights ──

@app.route("/api/manager-weights", methods=["GET"])
def api_get_weights():
    cfg = load_config()
    return jsonify({"weights": cfg.get("manager_weights", {})})

@app.route("/api/manager-weights", methods=["POST"])
def api_set_weights():
    cfg = load_config()
    data = request.json
    cfg["manager_weights"] = data.get("weights", {})
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/weighted-return")
def api_weighted_return():
    try:
        import analysis
        wr = analysis.compute_portfolio_weighted_return(last_results.get("all_rows", []))
        return jsonify(wr)
    except ImportError:
        return jsonify({"error": "analysis module not found"}), 500

# ── QoQ Diff ──

@app.route("/api/run-diff", methods=["POST"])
def api_run_diff():
    if not run_lock.acquire(blocking=False):
        return jsonify({"error": "Already running"}), 409
    data = request.json
    date1 = data.get("date1", "")
    date2 = data.get("date2", "")
    if not date1 or not date2:
        run_lock.release()
        return jsonify({"error": "Two dates required"}), 400

    cfg = load_config()
    run_id = str(int(time.time()))
    q = queue.Queue()
    progress_queues[run_id] = q

    def _go():
        global diff_results
        try:
            import importlib
            importlib.reload(holdings)
            from edgar import set_identity
            set_identity(cfg.get("identity", DEFAULT_CONFIG["identity"]))
            holdings.TOP_N = cfg.get("top_n", 20)
            holdings.OUTPUT_DIR = APP_DIR
            def log(msg):
                q.put({"type": "log", "message": msg})

            managers_13f = cfg.get("managers_13f", {})
            managers_nport = cfg.get("managers_nport", {})
            total = (len(managers_13f) + len(managers_nport)) * 2
            done = 0

            log(f"QoQ Diff: fetching {len(managers_13f)} 13F + {len(managers_nport)} N-PORT managers for dates {date1} and {date2}")

            rows1, rows2 = [], []

            for name, cik in managers_13f.items():
                log(f"[Period 1 · 13F] {name}...")
                holdings.MAX_DATE = date1
                try:
                    _, _, _, r = holdings.fetch_13f(name, cik)
                    rows1.extend(r)
                except Exception as e:
                    log(f"  Error: {e}")
                done += 1
                q.put({"type": "progress", "done": done, "total": total})

                log(f"[Period 2 · 13F] {name}...")
                holdings.MAX_DATE = date2
                try:
                    _, _, _, r = holdings.fetch_13f(name, cik)
                    rows2.extend(r)
                except Exception as e:
                    log(f"  Error: {e}")
                done += 1
                q.put({"type": "progress", "done": done, "total": total})

            for name, info in managers_nport.items():
                log(f"[Period 1 · N-PORT] {name}...")
                holdings.MAX_DATE = date1
                try:
                    _, _, _, r = holdings.fetch_nport(name, info["cik"], info["series_keyword"])
                    rows1.extend(r)
                except Exception as e:
                    log(f"  Error: {e}")
                done += 1
                q.put({"type": "progress", "done": done, "total": total})

                log(f"[Period 2 · N-PORT] {name}...")
                holdings.MAX_DATE = date2
                try:
                    _, _, _, r = holdings.fetch_nport(name, info["cik"], info["series_keyword"])
                    rows2.extend(r)
                except Exception as e:
                    log(f"  Error: {e}")
                done += 1
                q.put({"type": "progress", "done": done, "total": total})

            import analysis
            diff_results = analysis.compute_qoq_diff(rows2, rows1)
            log("QoQ diff complete!")
            q.put({"type": "complete", "results": {"diff": diff_results}})
        finally:
            run_lock.release()

    threading.Thread(target=_go, daemon=True).start()
    return jsonify({"run_id": run_id})

@app.route("/api/diff-data")
def api_diff_data():
    return jsonify({"diff": diff_results})

# ── History (SQLite) ──

@app.route("/api/history")
def api_history():
    """List stored runs, newest first."""
    limit = request.args.get("limit", 50, type=int)
    return jsonify({"runs": db.list_runs(limit)})

@app.route("/api/history/<int:run_id>/load", methods=["POST"])
def api_history_load(run_id):
    """Load a stored run back into memory so all charts/endpoints work."""
    global last_results
    data = db.load_run(run_id)
    if not data:
        return jsonify({"error": "Run not found"}), 404
    last_results = data
    last_results["db_run_id"] = run_id
    return jsonify({"ok": True, "run_date": data["run_date"],
                     "holdings": len(data["all_rows"]),
                     "managers": len(data.get("managers", []))})

@app.route("/api/history/<int:run_id>", methods=["DELETE"])
def api_history_delete(run_id):
    """Delete a stored run."""
    db.delete_run(run_id)
    return jsonify({"ok": True})

@app.route("/api/history/<int:run_id>/label", methods=["POST"])
def api_history_label(run_id):
    """Set or update a run's label."""
    label = request.json.get("label", "").strip()
    db.label_run(run_id, label)
    return jsonify({"ok": True})

@app.route("/api/history/compare", methods=["POST"])
def api_history_compare():
    """Instant QoQ diff between two stored runs (no EDGAR fetch needed)."""
    global diff_results
    data = request.json
    run_id_1 = data.get("run_id_1")  # earlier
    run_id_2 = data.get("run_id_2")  # later
    if not run_id_1 or not run_id_2:
        return jsonify({"error": "Two run IDs required"}), 400
    rows1 = db.load_run_rows(run_id_1)
    rows2 = db.load_run_rows(run_id_2)
    if not rows1 or not rows2:
        return jsonify({"error": "One or both runs not found"}), 404
    import analysis
    diff_results = analysis.compute_qoq_diff(rows2, rows1)
    return jsonify({"diff": diff_results})

@app.route("/api/history/ticker/<ticker>")
def api_ticker_history(ticker):
    """Get a ticker's weight/value across stored runs."""
    limit = request.args.get("limit", 20, type=int)
    return jsonify({"history": db.ticker_history(ticker.upper(), limit)})

@app.route("/api/stock-detail/<ticker>")
def api_stock_detail(ticker):
    """Full detail for a single stock: stats, managers, sparkline, weight history."""
    ticker = ticker.upper()
    all_rows = last_results.get("all_rows", [])
    matches = [r for r in all_rows if (r.get("ticker") or "").upper() == ticker]
    if not matches:
        return jsonify({"error": "Ticker not found in current holdings"}), 404
    ref = matches[0]
    managers = sorted(
        [{"manager": r["manager"], "pct": r.get("pct_of_portfolio", 0),
          "value": r.get("value_usd", 0), "rank": r.get("rank")} for r in matches],
        key=lambda m: m["pct"], reverse=True)
    # 6-month sparkline + market cap via yfinance
    sparkline, sparkline_dates, market_cap = [], [], None
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        hist = t.history(period="6mo")
        if hist is not None and not hist.empty:
            sparkline = [round(float(p), 2) for p in hist["Close"].tolist()]
            sparkline_dates = [d.strftime("%Y-%m-%d") for d in hist.index]
        info = t.info or {}
        market_cap = info.get("marketCap")
    except Exception:
        pass
    history = db.ticker_history(ticker, limit=20)
    return jsonify({
        "ticker": ticker, "name": ref.get("name", ""),
        "sector": ref.get("sector"), "industry": ref.get("industry"),
        "country": ref.get("country"),
        "forward_pe": ref.get("forward_pe"),
        "trailing_eps": ref.get("trailing_eps"),
        "forward_eps": ref.get("forward_eps"),
        "filing_reported_eps": ref.get("filing_reported_eps"),
        "filing_consensus_eps": ref.get("filing_consensus_eps"),
        "filing_eps_beat_pct": ref.get("filing_eps_beat_pct"),
        "forward_eps_growth": ref.get("forward_eps_growth"),
        "dividend_yield": ref.get("dividend_yield"),
        "filing_quarter_return_pct": ref.get("filing_quarter_return_pct"),
        "prior_quarter_return_pct": ref.get("prior_quarter_return_pct"),
        "qtd_return_pct": ref.get("qtd_return_pct"),
        "filing_price_qtr_end": ref.get("filing_price_qtr_end"),
        "market_cap": market_cap, "managers": managers,
        "sparkline": sparkline, "sparkline_dates": sparkline_dates,
        "history": history})

@app.route("/api/reset", methods=["POST"])
def api_reset():
    global last_results, diff_results
    last_results = {}
    diff_results = {}
    try:
        import financial_data
        financial_data.clear_cache()
    except Exception:
        pass
    return jsonify({"ok": True})

@app.route("/api/search-company")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    try:
        return jsonify(_search_edgar_13f(q))
    except Exception:
        return jsonify([])

# ── Search cache (avoid repeated SEC HTTP calls) ──────────────────────────────
_search_cache = {}       # query_lower -> {"results": [...], "ts": time}
_search_cache_lock = threading.Lock()
_SEARCH_CACHE_TTL = 900  # 15 minutes


@app.route("/api/search-unified")
def api_search_unified():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    q_lower = q.lower()
    now = time.time()

    # Check cache first (huge speedup — avoids SEC HTTP calls)
    with _search_cache_lock:
        cached = _search_cache.get(q_lower)
        if cached and (now - cached["ts"]) < _SEARCH_CACHE_TTL:
            return jsonify(cached["results"])

    # Parallelize mutual fund + 13F searches via ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor, as_completed
    mf_results = []
    f13_results = []

    def _do_mf():
        try:
            return list(_search_mutual_funds(q))
        except Exception:
            return []

    def _do_13f():
        try:
            return list(_search_edgar_13f(q))
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_mf = ex.submit(_do_mf)
        fut_13f = ex.submit(_do_13f)
        mf_results = fut_mf.result()
        f13_results = fut_13f.result()

    results = []
    seen_keys = set()  # (cik, series_id) for NPORT, cik for 13F

    # 1) Mutual fund results first
    for r in mf_results:
        key = (r["cik"], r.get("series_id", ""))
        if key not in seen_keys:
            seen_keys.add(key)
            results.append(r)

    # 2) 13F results
    for r in f13_results:
        key = (r["cik"], "")
        if key not in seen_keys:
            r["type"] = "13F"
            seen_keys.add(key)
            results.append(r)

    final = results[:20]
    # Cache results for future typeahead calls
    with _search_cache_lock:
        _search_cache[q_lower] = {"results": final, "ts": now}
        # Evict old entries to prevent memory growth
        if len(_search_cache) > 500:
            oldest = sorted(_search_cache.items(), key=lambda x: x[1]["ts"])[:250]
            for k, _ in oldest:
                del _search_cache[k]
    return jsonify(final)

@app.route("/api/managers/clear", methods=["POST"])
def api_clear_managers():
    cfg = load_config()
    cfg["managers_13f"] = {}
    cfg["managers_nport"] = {}
    cfg["manager_weights"] = {}
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/download-all")
def api_download():
    files = last_results.get("files", [])
    if not files:
        return jsonify({"error": "No files yet"}), 404
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fn in files:
            fp = os.path.join(APP_DIR, fn)
            if os.path.exists(fp):
                zf.write(fp, fn)
    buf.seek(0)
    dt = last_results.get("run_date", datetime.today().strftime("%Y%m%d"))
    return send_file(buf, mimetype="application/zip", as_attachment=True,
                     download_name=f"manager_holdings_{dt}.zip")

@app.route("/api/download-pdf")
def api_download_pdf():
    if not last_results.get("all_rows"):
        return jsonify({"error": "No data yet"}), 404
    try:
        from holdings import generate_pdf
        cfg = load_config()
        buf = generate_pdf(
            last_results.get("all_rows", []),
            last_results.get("run_date", datetime.today().strftime("%Y%m%d")),
            cfg
        )
        return send_file(buf, mimetype="application/pdf", as_attachment=True,
                         download_name=f"portfolio_report_{last_results.get('run_date', '')}.pdf")
    except ImportError:
        return jsonify({"error": "reportlab not installed — pip install reportlab"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(APP_DIR, filename)

# ── HTML ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(os.path.join(APP_DIR, 'static'), 'index.html')

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
    # Auto-load most recent run from SQLite so charts are available immediately
    try:
        recent = db.list_runs(1)
        if recent:
            loaded = db.load_run(recent[0]["id"])
            if loaded and loaded.get("all_rows"):
                last_results = loaded
                last_results["db_run_id"] = recent[0]["id"]
                print(f"  Loaded previous run #{recent[0]['id']} "
                      f"({len(loaded['all_rows'])} holdings from {recent[0]['run_date']})")
    except Exception as e:
        print(f"  Could not auto-load previous run: {e}")
    # Pre-load mutual fund data in background for faster first search
    def _preload():
        try:
            _load_mf_tickers()
            print("  Mutual fund ticker data pre-loaded")
        except Exception as e:
            print(f"  Warning: mutual fund pre-load failed: {e}")
    threading.Thread(target=_preload, daemon=True).start()

    print("\n" + "=" * 50)
    print("  Investment Manager Holdings (Enhanced)")
    print("  http://localhost:8080")
    print("=" * 50 + "\n")
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=True)
