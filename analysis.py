"""
Portfolio Analysis Functions
============================
Overlap analysis, sector breakdown, quarter-over-quarter diff,
weighted portfolio return, and summary statistics for SEC 13F holdings data.
"""

from collections import defaultdict, Counter

# ── Winsorize helper ─────────────────────────────────────────────────────────

_GROWTH_CAP = 50.0  # cap growth rates at ±50% for weighted averages

def _clamp_growth(val):
    """Clamp growth rate to [-50, +50]% to prevent outliers from dominating weighted averages."""
    if val is None:
        return None
    return max(-_GROWTH_CAP, min(_GROWTH_CAP, val))


# ── Quarter helpers ──────────────────────────────────────────────────────────

def quarter_label(date_str):
    """Convert '2025-09-30' to '3Q25'."""
    if not date_str:
        return ""
    try:
        parts = date_str.split("-")
        year = parts[0][-2:]
        month = int(parts[1])
        q = (month - 1) // 3 + 1
        return f"{q}Q{year}"
    except (IndexError, ValueError):
        return ""


def prior_quarter_end(date_str):
    """Given '2025-09-30', return '2025-06-30'."""
    if not date_str:
        return ""
    try:
        parts = date_str.split("-")
        year = int(parts[0])
        month = int(parts[1])
        q = (month - 1) // 3 + 1
        if q == 1:
            return f"{year-1}-12-31"
        elif q == 2:
            return f"{year}-03-31"
        elif q == 3:
            return f"{year}-06-30"
        else:
            return f"{year}-09-30"
    except (IndexError, ValueError):
        return ""


def quarter_end_from_max_date(max_date):
    """Given a max filing date like '2025-12-31', figure out the most likely
    quarter end the filings cover (typically the prior quarter)."""
    if not max_date:
        return "", ""
    try:
        parts = max_date.split("-")
        year = int(parts[0])
        month = int(parts[1])
        q = (month - 1) // 3 + 1
        # The filing quarter end
        qe_map = {1: f"{year}-03-31", 2: f"{year}-06-30",
                   3: f"{year}-09-30", 4: f"{year}-12-31"}
        filing_qe = qe_map[q]
        prior_qe = prior_quarter_end(filing_qe)
        return filing_qe, prior_qe
    except (IndexError, ValueError):
        return "", ""


def shorten_stock_name(name):
    """Shorten stock names by removing common corporate suffixes.
    Keep meaningful multi-word names like 'Taiwan Semiconductor'."""
    if not name:
        return name
    suffixes = {
        'corporation', 'corp', 'corp.', 'inc', 'inc.', 'ltd', 'ltd.',
        'limited', 'platforms', 'holdings', 'group', 'incorporated',
        'co', 'co.', 'plc', 'lp', 'l.p.', 'nv', 'sa', 'ag', 'se',
        'international', 'intl', 'technologies', 'technology',
        'enterprises', 'enterprise', 'solutions', 'services',
        'industries', 'financial', 'bancorp', 'therapeutics',
        'pharmaceuticals', 'semiconductor', 'class', 'cl',
    }
    # Don't strip 'semiconductor' from 'Taiwan Semiconductor' etc.
    # Only strip if we'd have at least one word remaining
    parts = name.replace(',', '').split()
    if len(parts) <= 1:
        return name
    # Remove class designations like "Class A", "Cl A" at the end
    while len(parts) > 1 and parts[-1].upper() in ('A', 'B', 'C'):
        # Only remove if preceded by "Class" or "Cl"
        if len(parts) >= 2 and parts[-2].lower() in ('class', 'cl'):
            parts.pop()
            parts.pop()
        else:
            break
    # Remove trailing suffixes
    while len(parts) > 1 and parts[-1].lower().rstrip('.,') in suffixes:
        parts.pop()
    result = ' '.join(parts).strip().rstrip(',')
    return result if result else name


def display_label(row_or_name, ticker=None):
    """
    Format a stock label as 'Stock Name (TICKER)'.
    Accepts either a row dict or separate name/ticker args.
    """
    if isinstance(row_or_name, dict):
        name = row_or_name.get("name", "Unknown")
        ticker = row_or_name.get("ticker", "N/A")
    else:
        name = row_or_name
    if ticker and ticker != "N/A":
        return f"{name} ({ticker})"
    return name


def _calc_weighted_return(rows):
    """Calculate weighted return for a list of holdings rows."""
    filing_sum = 0.0
    filing_weight_sum = 0.0
    prior_sum = 0.0
    prior_weight_sum = 0.0
    qtd_sum = 0.0
    qtd_weight_sum = 0.0

    for r in rows:
        pct = r.get("pct_of_portfolio", 0) or 0

        filing_ret = r.get("filing_quarter_return_pct")
        if filing_ret is not None:
            filing_sum += filing_ret * pct
            filing_weight_sum += pct

        prior_ret = r.get("prior_quarter_return_pct")
        if prior_ret is not None:
            prior_sum += prior_ret * pct
            prior_weight_sum += pct

        qtd_ret = r.get("qtd_return_pct")
        if qtd_ret is not None:
            qtd_sum += qtd_ret * pct
            qtd_weight_sum += pct

    return {
        "filing_qtr_weighted_return": round(filing_sum / filing_weight_sum, 2) if filing_weight_sum > 0 else None,
        "prior_qtr_weighted_return": round(prior_sum / prior_weight_sum, 2) if prior_weight_sum > 0 else None,
        "qtd_weighted_return": round(qtd_sum / qtd_weight_sum, 2) if qtd_weight_sum > 0 else None,
        "stocks_with_return": sum(1 for r in rows if r.get("filing_quarter_return_pct") is not None),
        "stocks_without_return": sum(1 for r in rows if r.get("filing_quarter_return_pct") is None),
    }


def compute_portfolio_weighted_return(all_rows, by_manager=False):
    """
    Calculate weighted total return for the portfolio based on stock weights.

    Args:
        all_rows: list of enriched holdings rows
        by_manager: if True, returns per-manager dict instead of overall

    Returns:
        dict with filing_qtr_weighted_return, prior_qtr_weighted_return,
        stocks_with_return, stocks_without_return.
        If by_manager=True, returns {manager_name: return_dict}.
    """
    if not all_rows:
        return {} if by_manager else _calc_weighted_return([])

    if by_manager:
        by_mgr = defaultdict(list)
        for r in all_rows:
            by_mgr[r["manager"]].append(r)
        return {mgr: _calc_weighted_return(rows) for mgr, rows in by_mgr.items()}

    return _calc_weighted_return(all_rows)


def compute_summary_stats(all_rows, manager_weights=None):
    """
    Compute high-level summary statistics from enriched holdings rows.

    Returns dict with:
        total_holdings, unique_stocks, unique_managers,
        most_common_stock, avg_quarter_return, eps_beat_rate,
        total_value, top_stocks_by_value, top_stocks_by_pct,
        weighted_return, filing_quarter, prior_quarter
    """
    if not all_rows:
        return {}

    tickers = [r["ticker"] for r in all_rows if r.get("ticker") != "N/A"]
    managers = {r["manager"] for r in all_rows}

    # Determine quarters from period_of_report
    periods = [r["period_of_report"] for r in all_rows if r.get("period_of_report")]
    main_period = Counter(periods).most_common(1)[0][0] if periods else ""
    filing_qtr = quarter_label(main_period)
    prior_qtr_end = prior_quarter_end(main_period)
    prior_qtr = quarter_label(prior_qtr_end)

    # Most common stock (held by most managers)
    ticker_manager_map = defaultdict(set)
    for r in all_rows:
        if r.get("ticker") != "N/A":
            ticker_manager_map[r["ticker"]].add(r["manager"])

    most_common = None
    most_common_count = 0
    for tk, mgrs in ticker_manager_map.items():
        if len(mgrs) > most_common_count:
            most_common_count = len(mgrs)
            name = tk
            for r in all_rows:
                if r["ticker"] == tk:
                    name = r["name"]
                    break
            most_common = {"ticker": tk, "name": name, "manager_count": len(mgrs)}

    # Average filing quarter return
    returns = [r["filing_quarter_return_pct"] for r in all_rows
               if r.get("filing_quarter_return_pct") is not None]
    avg_return = round(sum(returns) / len(returns), 2) if returns else None

    # EPS beat rate (filing quarter)
    beats = [r for r in all_rows if r.get("filing_eps_beat_dollars") is not None]
    eps_beat_count = sum(1 for r in beats if r["filing_eps_beat_dollars"] > 0)
    eps_beat_rate = round(eps_beat_count / len(beats) * 100, 1) if beats else None

    # Total value
    total_value = sum(r.get("value_usd", 0) for r in all_rows)

    # Top 10 stocks by aggregate value across all managers
    value_by_ticker = defaultdict(lambda: {"value": 0, "name": "", "ticker": "", "count": 0})
    for r in all_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        value_by_ticker[tk]["value"] += r.get("value_usd", 0)
        value_by_ticker[tk]["name"] = r.get("name", "")
        value_by_ticker[tk]["ticker"] = tk
        value_by_ticker[tk]["count"] += 1

    top_stocks = sorted(value_by_ticker.values(), key=lambda x: x["value"], reverse=True)[:10]

    # Top 10 stocks by weighted portfolio % (deduplicated)
    weighted_rows, _ = _apply_manager_weights(all_rows, manager_weights)
    pct_by_ticker = defaultdict(lambda: {
        "combined_weight": 0, "name": "", "ticker": "",
        "managers": set(), "sector": None,
        "forward_pe": None, "forward_eps_growth": None, "dividend_yield": None,
        "forward_revenue_growth": None, "forward_ps": None,
    })
    for r in weighted_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        d = pct_by_ticker[tk]
        d["combined_weight"] += r.get("combined_weight", 0)
        d["name"] = r.get("name", "")
        d["ticker"] = tk
        d["managers"].add(r["manager"])
        if r.get("sector"):
            d["sector"] = r["sector"]
        # Keep first non-None forward metric per ticker
        if d["forward_pe"] is None and r.get("forward_pe") is not None:
            d["forward_pe"] = r["forward_pe"]
        if d["forward_eps_growth"] is None and r.get("forward_eps_growth") is not None:
            d["forward_eps_growth"] = r["forward_eps_growth"]
        if d["dividend_yield"] is None and r.get("dividend_yield") is not None:
            d["dividend_yield"] = r["dividend_yield"]
        if d["forward_revenue_growth"] is None and r.get("forward_revenue_growth") is not None:
            d["forward_revenue_growth"] = r["forward_revenue_growth"]
        if d["forward_ps"] is None and r.get("forward_ps") is not None:
            d["forward_ps"] = r["forward_ps"]

    # Compute weighted forward metrics (deduplicated by ticker)
    wtd_inv_pe_sum = 0.0   # sum of (weight / forward_pe) for harmonic mean
    wtd_inv_pe_wt = 0.0
    wtd_eps_growth = 0.0
    wtd_eps_wt = 0.0
    wtd_div_yield = 0.0
    wtd_div_wt = 0.0
    wtd_rev_growth = 0.0
    wtd_rev_wt = 0.0
    wtd_inv_ps_sum = 0.0
    wtd_inv_ps_wt = 0.0
    for tk_data in pct_by_ticker.values():
        w = tk_data["combined_weight"]
        if w <= 0:
            continue
        fpe = tk_data.get("forward_pe")
        if fpe is not None and fpe > 0:
            wtd_inv_pe_sum += w / fpe
            wtd_inv_pe_wt += w
        feg = _clamp_growth(tk_data.get("forward_eps_growth"))
        if feg is not None:
            wtd_eps_growth += w * feg
            wtd_eps_wt += w
        dy = tk_data.get("dividend_yield")
        if dy is not None:
            wtd_div_yield += w * dy
            wtd_div_wt += w
        frg = _clamp_growth(tk_data.get("forward_revenue_growth"))
        if frg is not None:
            wtd_rev_growth += w * frg
            wtd_rev_wt += w
        fps = tk_data.get("forward_ps")
        if fps is not None and fps > 0:
            wtd_inv_ps_sum += w / fps
            wtd_inv_ps_wt += w

    weighted_forward_pe = round(wtd_inv_pe_wt / wtd_inv_pe_sum, 2) if wtd_inv_pe_sum > 0 else None
    weighted_eps_growth = round(wtd_eps_growth / wtd_eps_wt, 2) if wtd_eps_wt > 0 else None
    weighted_div_yield = round(wtd_div_yield / wtd_div_wt, 2) if wtd_div_wt > 0 else None
    weighted_rev_growth = round(wtd_rev_growth / wtd_rev_wt, 2) if wtd_rev_wt > 0 else None
    weighted_forward_ps = round(wtd_inv_ps_wt / wtd_inv_ps_sum, 2) if wtd_inv_ps_sum > 0 else None
    expected_return = None
    if weighted_eps_growth is not None and weighted_div_yield is not None:
        expected_return = round(weighted_eps_growth + weighted_div_yield, 2)

    top_by_pct = sorted(pct_by_ticker.values(), key=lambda x: -x["combined_weight"])[:10]
    top_stocks_pct = []
    for s in top_by_pct:
        top_stocks_pct.append({
            "ticker": s["ticker"],
            "name": s["name"],
            "short_name": shorten_stock_name(s["name"]),
            "pct": round(s["combined_weight"], 2),
            "manager_count": len(s["managers"]),
            "sector": s["sector"],
        })

    # Weighted portfolio return
    weighted_ret = compute_portfolio_weighted_return(all_rows)

    return {
        "total_holdings": len(all_rows),
        "unique_stocks": len(set(tickers)),
        "unique_managers": len(managers),
        "most_common_stock": most_common,
        "avg_quarter_return": avg_return,
        "eps_beat_rate": eps_beat_rate,
        "eps_beat_count": eps_beat_count if beats else 0,
        "eps_total_count": len(beats),
        "total_value": total_value,
        "top_stocks_by_value": top_stocks,
        "top_stocks_by_pct": top_stocks_pct,
        "weighted_return": weighted_ret,
        "filing_quarter": filing_qtr,
        "prior_quarter": prior_qtr,
        "filing_period": main_period,
        "weighted_forward_pe": weighted_forward_pe,
        "weighted_eps_growth": weighted_eps_growth,
        "weighted_div_yield": weighted_div_yield,
        "weighted_rev_growth": weighted_rev_growth,
        "weighted_forward_ps": weighted_forward_ps,
        "expected_return": expected_return,
    }


def compute_top_stocks_valuation(all_rows, manager_weights=None, top_n=20):
    """
    Top N stocks by portfolio weight with forward P/E and EPS growth for scatter chart.

    Returns dict with:
        stocks: [{ticker, name, short_name, pct, forward_pe, forward_eps_growth,
                  dividend_yield, sector}]
        portfolio_avg: {forward_pe, eps_growth, div_yield, expected_return}
    """
    if not all_rows:
        return {"stocks": [], "portfolio_avg": {}}

    weighted_rows, _ = _apply_manager_weights(all_rows, manager_weights)

    # Deduplicate by ticker
    by_ticker = defaultdict(lambda: {
        "combined_weight": 0, "name": "", "ticker": "",
        "forward_pe": None, "forward_eps_growth": None,
        "dividend_yield": None, "sector": None,
        "forward_revenue_growth": None, "forward_ps": None,
    })
    for r in weighted_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        d = by_ticker[tk]
        d["combined_weight"] += r.get("combined_weight", 0)
        d["name"] = r.get("name", "")
        d["ticker"] = tk
        if d["forward_pe"] is None and r.get("forward_pe") is not None:
            d["forward_pe"] = r["forward_pe"]
        if d["forward_eps_growth"] is None and r.get("forward_eps_growth") is not None:
            d["forward_eps_growth"] = r["forward_eps_growth"]
        if d["dividend_yield"] is None and r.get("dividend_yield") is not None:
            d["dividend_yield"] = r["dividend_yield"]
        if d["sector"] is None and r.get("sector"):
            d["sector"] = r["sector"]
        if d["forward_revenue_growth"] is None and r.get("forward_revenue_growth") is not None:
            d["forward_revenue_growth"] = r["forward_revenue_growth"]
        if d["forward_ps"] is None and r.get("forward_ps") is not None:
            d["forward_ps"] = r["forward_ps"]

    # Sort by weight, take top N that have BOTH forward_pe and forward_eps_growth
    all_tickers = sorted(by_ticker.values(), key=lambda x: -x["combined_weight"])
    stocks = []
    for s in all_tickers:
        if s["forward_pe"] is not None and s["forward_eps_growth"] is not None:
            stocks.append({
                "ticker": s["ticker"],
                "name": s["name"],
                "short_name": shorten_stock_name(s["name"]),
                "pct": round(s["combined_weight"], 2),
                "forward_pe": round(s["forward_pe"], 2),
                "forward_eps_growth": round(s["forward_eps_growth"], 2),
                "dividend_yield": round(s["dividend_yield"], 2) if s["dividend_yield"] is not None else None,
                "sector": s["sector"],
                "forward_revenue_growth": round(s["forward_revenue_growth"], 2) if s["forward_revenue_growth"] is not None else None,
                "forward_ps": round(s["forward_ps"], 2) if s["forward_ps"] is not None else None,
            })
            if len(stocks) >= top_n:
                break

    # Portfolio-level averages (harmonic for P/E & P/S, weighted arithmetic for growth)
    wtd_inv_pe_sum, wtd_inv_pe_wt = 0.0, 0.0
    wtd_eg_sum, wtd_eg_wt = 0.0, 0.0
    wtd_dy_sum, wtd_dy_wt = 0.0, 0.0
    wtd_rg_sum, wtd_rg_wt = 0.0, 0.0
    wtd_inv_ps_sum, wtd_inv_ps_wt = 0.0, 0.0
    for tk_data in by_ticker.values():
        w = tk_data["combined_weight"]
        if w <= 0:
            continue
        fpe = tk_data.get("forward_pe")
        if fpe is not None and fpe > 0:
            wtd_inv_pe_sum += w / fpe
            wtd_inv_pe_wt += w
        feg = _clamp_growth(tk_data.get("forward_eps_growth"))
        if feg is not None:
            wtd_eg_sum += w * feg
            wtd_eg_wt += w
        dy = tk_data.get("dividend_yield")
        if dy is not None:
            wtd_dy_sum += w * dy
            wtd_dy_wt += w
        frg = _clamp_growth(tk_data.get("forward_revenue_growth"))
        if frg is not None:
            wtd_rg_sum += w * frg
            wtd_rg_wt += w
        fps = tk_data.get("forward_ps")
        if fps is not None and fps > 0:
            wtd_inv_ps_sum += w / fps
            wtd_inv_ps_wt += w

    avg_pe = round(wtd_inv_pe_wt / wtd_inv_pe_sum, 2) if wtd_inv_pe_sum > 0 else None
    avg_eg = round(wtd_eg_sum / wtd_eg_wt, 2) if wtd_eg_wt > 0 else None
    avg_dy = round(wtd_dy_sum / wtd_dy_wt, 2) if wtd_dy_wt > 0 else None
    avg_rg = round(wtd_rg_sum / wtd_rg_wt, 2) if wtd_rg_wt > 0 else None
    avg_ps = round(wtd_inv_ps_wt / wtd_inv_ps_sum, 2) if wtd_inv_ps_sum > 0 else None
    exp_ret = round(avg_eg + avg_dy, 2) if avg_eg is not None and avg_dy is not None else None

    return {
        "stocks": stocks,
        "portfolio_avg": {
            "forward_pe": avg_pe,
            "eps_growth": avg_eg,
            "div_yield": avg_dy,
            "rev_growth": avg_rg,
            "forward_ps": avg_ps,
            "expected_return": exp_ret,
        }
    }


def compute_overlap(all_rows):
    """
    Find stocks held by multiple managers.

    Returns list of dicts sorted by manager count (descending):
        [{ticker, name, display_label, managers: [names], manager_count,
          total_value, avg_pct, sector, industry}]
    """
    ticker_data = defaultdict(lambda: {
        "managers": [], "total_value": 0, "pcts": [],
        "name": "", "ticker": "", "sector": None, "industry": None,
    })

    for r in all_rows:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        d = ticker_data[tk]
        d["ticker"] = tk
        d["name"] = r.get("name", "")
        d["managers"].append(r["manager"])
        d["total_value"] += r.get("value_usd", 0)
        d["pcts"].append(r.get("pct_of_portfolio", 0))
        if r.get("sector"):
            d["sector"] = r["sector"]
        if r.get("industry"):
            d["industry"] = r["industry"]

    results = []
    for tk, d in ticker_data.items():
        if len(d["managers"]) >= 2:
            results.append({
                "ticker": d["ticker"],
                "name": d["name"],
                "display_label": display_label(d["name"], d["ticker"]),
                "managers": sorted(set(d["managers"])),
                "manager_count": len(set(d["managers"])),
                "total_value": d["total_value"],
                "avg_pct": round(sum(d["pcts"]) / len(d["pcts"]), 2),
                "sector": d["sector"],
                "industry": d["industry"],
            })

    results.sort(key=lambda x: (-x["manager_count"], -x["total_value"]))
    return results


def compute_sector_breakdown(all_rows):
    """
    Aggregate holdings by sector and industry.

    Returns dict with:
        sectors: [{name, total_value, pct, count, top_stocks}]
        industries: [{name, sector, total_value, pct, count}]
        by_manager: {manager_name: [{sector, value, pct}]}
    """
    total_value = sum(r.get("value_usd", 0) for r in all_rows)
    if total_value == 0:
        total_value = 1

    sector_data = defaultdict(lambda: {"value": 0, "count": 0, "stocks": [], "by_ticker": {}})
    industry_data = defaultdict(lambda: {"value": 0, "count": 0, "sector": "", "by_ticker": {}})
    manager_sector = defaultdict(lambda: defaultdict(float))

    for r in all_rows:
        sector = r.get("sector") or "Unknown"
        industry = r.get("industry") or "Unknown"
        val = r.get("value_usd", 0)
        tk = r.get("ticker", "N/A")
        mgr = r.get("manager", "")

        sector_data[sector]["value"] += val
        sector_data[sector]["count"] += 1
        sector_data[sector]["stocks"].append({
            "ticker": tk,
            "name": r.get("name", ""),
            "display_label": display_label(r),
            "value": val,
        })
        # Track per-ticker detail for sector
        if tk != "N/A":
            sd_bt = sector_data[sector]["by_ticker"]
            if tk not in sd_bt:
                sd_bt[tk] = {"name": shorten_stock_name(r.get("name", "")), "value": 0, "managers": set()}
            sd_bt[tk]["value"] += val
            sd_bt[tk]["managers"].add(mgr)

        industry_data[industry]["value"] += val
        industry_data[industry]["count"] += 1
        industry_data[industry]["sector"] = sector
        # Track per-ticker detail for industry
        if tk != "N/A":
            id_bt = industry_data[industry]["by_ticker"]
            if tk not in id_bt:
                id_bt[tk] = {"name": shorten_stock_name(r.get("name", "")), "value": 0, "managers": set()}
            id_bt[tk]["value"] += val
            id_bt[tk]["managers"].add(mgr)

        manager_sector[r["manager"]][sector] += val

    def _build_stocks_detail(by_ticker, cat_total):
        """Build top-8 stocks_detail list from by_ticker dict."""
        items = sorted(by_ticker.items(), key=lambda x: -x[1]["value"])[:8]
        return [{
            "name": f"{d['name']} ({tk})",
            "pct": round(d["value"] / cat_total * 100, 1) if cat_total > 0 else 0,
            "managers": sorted(d["managers"]),
        } for tk, d in items]

    sectors = []
    for name, d in sector_data.items():
        top = sorted(d["stocks"], key=lambda x: x["value"], reverse=True)[:5]
        sectors.append({
            "name": name,
            "total_value": d["value"],
            "pct": round(d["value"] / total_value * 100, 2),
            "count": d["count"],
            "top_stocks": top,
            "stocks_detail": _build_stocks_detail(d["by_ticker"], d["value"]),
        })
    sectors.sort(key=lambda x: -x["total_value"])

    # Build GICS-normalized sector aggregation for ACWI comparison
    try:
        from financial_data import normalize_sector_name
    except ImportError:
        def normalize_sector_name(n):
            return n
    norm_agg = defaultdict(lambda: {"value": 0, "count": 0})
    for s in sectors:
        gics_name = normalize_sector_name(s["name"])
        norm_agg[gics_name]["value"] += s["total_value"]
        norm_agg[gics_name]["count"] += s["count"]
    normalized_sectors = []
    for name, dd in norm_agg.items():
        normalized_sectors.append({
            "name": name,
            "total_value": dd["value"],
            "pct": round(dd["value"] / total_value * 100, 2),
            "count": dd["count"],
        })
    normalized_sectors.sort(key=lambda x: -x["total_value"])

    industries = []
    for name, d in industry_data.items():
        industries.append({
            "name": name,
            "sector": d["sector"],
            "total_value": d["value"],
            "pct": round(d["value"] / total_value * 100, 2),
            "count": d["count"],
            "stocks_detail": _build_stocks_detail(d["by_ticker"], d["value"]),
        })
    industries.sort(key=lambda x: -x["total_value"])

    by_manager = {}
    for mgr, sector_vals in manager_sector.items():
        mgr_total = sum(sector_vals.values()) or 1
        by_manager[mgr] = sorted(
            [{"sector": s, "value": v, "pct": round(v / mgr_total * 100, 2)}
             for s, v in sector_vals.items()],
            key=lambda x: -x["value"]
        )

    return {"sectors": sectors, "normalized_sectors": normalized_sectors, "industries": industries, "by_manager": by_manager}


def compute_geo_breakdown(all_rows):
    """
    Aggregate holdings by country.

    Returns dict with:
        countries: [{name, total_value, pct, count}]  (raw yfinance names)
        normalized_countries: [{name, total_value, pct, count}]  (iShares-normalized)
    """
    total_value = sum(r.get("value_usd", 0) for r in all_rows)
    if total_value == 0:
        total_value = 1

    country_data = defaultdict(lambda: {"value": 0, "count": 0, "by_ticker": {}})
    for r in all_rows:
        country = r.get("country") or "Unknown"
        val = r.get("value_usd", 0)
        tk = r.get("ticker", "N/A")
        mgr = r.get("manager", "")
        country_data[country]["value"] += val
        country_data[country]["count"] += 1
        if tk != "N/A":
            cd_bt = country_data[country]["by_ticker"]
            if tk not in cd_bt:
                cd_bt[tk] = {"name": shorten_stock_name(r.get("name", "")), "value": 0, "managers": set()}
            cd_bt[tk]["value"] += val
            cd_bt[tk]["managers"].add(mgr)

    def _geo_stocks_detail(by_ticker, cat_total):
        items = sorted(by_ticker.items(), key=lambda x: -x[1]["value"])[:8]
        return [{
            "name": f"{d['name']} ({tk})",
            "pct": round(d["value"] / cat_total * 100, 1) if cat_total > 0 else 0,
            "managers": sorted(d["managers"]),
        } for tk, d in items]

    countries = []
    for name, d in country_data.items():
        countries.append({
            "name": name,
            "total_value": d["value"],
            "pct": round(d["value"] / total_value * 100, 2),
            "count": d["count"],
            "stocks_detail": _geo_stocks_detail(d["by_ticker"], d["value"]),
        })
    countries.sort(key=lambda x: -x["total_value"])

    # Build iShares-normalized country aggregation for ACWI comparison
    try:
        from financial_data import normalize_country_name
    except ImportError:
        def normalize_country_name(n):
            return n
    norm_agg = defaultdict(lambda: {"value": 0, "count": 0})
    for c in countries:
        norm_name = normalize_country_name(c["name"])
        norm_agg[norm_name]["value"] += c["total_value"]
        norm_agg[norm_name]["count"] += c["count"]
    normalized_countries = []
    for name, dd in norm_agg.items():
        normalized_countries.append({
            "name": name,
            "total_value": dd["value"],
            "pct": round(dd["value"] / total_value * 100, 2),
            "count": dd["count"],
        })
    normalized_countries.sort(key=lambda x: -x["total_value"])

    return {"countries": countries, "normalized_countries": normalized_countries}


def _apply_manager_weights(all_rows, manager_weights=None):
    """
    Compute each stock's weight in the combined portfolio,
    applying manager_weights if provided.

    Returns (weighted_rows, total_weight) where each row in weighted_rows
    has an added 'combined_weight' field (% of total combined portfolio).
    """
    managers = list({r["manager"] for r in all_rows})
    if manager_weights and any(v > 0 for v in manager_weights.values()):
        mgr_wt = {m: manager_weights.get(m, 0) for m in managers}
    else:
        mgr_wt = {m: 100.0 / len(managers) for m in managers}

    total_wt = sum(mgr_wt.values()) or 1

    weighted = []
    for r in all_rows:
        mw = mgr_wt.get(r["manager"], 0)
        pct = r.get("pct_of_portfolio", 0) or 0
        combined = (pct / 100.0) * (mw / total_wt) * 100.0
        row_copy = dict(r)
        row_copy["combined_weight"] = round(combined, 4)
        weighted.append(row_copy)
    return weighted, total_wt


def compute_sector_treemap(all_rows, manager_weights=None):
    """
    Build hierarchical data for sector and industry treemaps with manager breakdown.

    Returns:
        {
            "sectors": [{name, pct, total_value, count,
                         manager_shares: [{manager, value, pct_of_sector}],
                         top_stocks: [{ticker, name, display_label, value,
                                       manager, filing_quarter_return,
                                       weight_in_combined}]}],
            "industries": [same structure]
        }
    """
    if not all_rows:
        return {"sectors": [], "industries": []}

    weighted, _ = _apply_manager_weights(all_rows, manager_weights)
    total_combined = sum(r["combined_weight"] for r in weighted)
    if total_combined == 0:
        total_combined = 1

    def _build_group(group_key):
        group_data = defaultdict(lambda: {
            "value": 0, "count": 0, "stocks": [],
            "mgr_values": defaultdict(float),
        })
        for r in weighted:
            gname = r.get(group_key) or "Unknown"
            d = group_data[gname]
            d["value"] += r.get("value_usd", 0)
            d["count"] += 1
            d["mgr_values"][r["manager"]] += r.get("value_usd", 0)
            d["stocks"].append({
                "ticker": r.get("ticker", "N/A"),
                "name": r.get("name", ""),
                "display_label": display_label(r),
                "value": r.get("value_usd", 0),
                "manager": r["manager"],
                "filing_quarter_return": r.get("filing_quarter_return_pct"),
                "weight_in_combined": r["combined_weight"],
            })

        results = []
        for name, d in group_data.items():
            top = sorted(d["stocks"], key=lambda x: x["weight_in_combined"], reverse=True)[:10]
            mgr_total = d["value"] or 1
            mgr_shares = sorted(
                [{"manager": m, "value": v, "pct_of_sector": round(v / mgr_total * 100, 1)}
                 for m, v in d["mgr_values"].items()],
                key=lambda x: -x["value"]
            )
            pct = sum(s["weight_in_combined"] for s in d["stocks"])
            results.append({
                "name": name,
                "pct": round(pct, 2),
                "total_value": d["value"],
                "count": d["count"],
                "manager_shares": mgr_shares,
                "top_stocks": top,
            })
        results.sort(key=lambda x: -x["pct"])
        return results

    return {
        "sectors": _build_group("sector"),
        "industries": _build_group("industry"),
    }


def compute_geo_treemap(all_rows, manager_weights=None):
    """
    Build hierarchical data for geographic exposure treemap.

    Returns:
        {
            "countries": [{name, pct, total_value, count,
                           manager_shares: [{manager, value, pct_of_country}],
                           top_stocks: [{ticker, name, display_label, value,
                                         manager, filing_quarter_return,
                                         weight_in_combined}]}]
        }
    """
    if not all_rows:
        return {"countries": []}

    weighted, _ = _apply_manager_weights(all_rows, manager_weights)

    country_data = defaultdict(lambda: {
        "value": 0, "count": 0, "stocks": [],
        "mgr_values": defaultdict(float),
    })

    for r in weighted:
        country = r.get("country") or "Unknown"
        d = country_data[country]
        d["value"] += r.get("value_usd", 0)
        d["count"] += 1
        d["mgr_values"][r["manager"]] += r.get("value_usd", 0)
        d["stocks"].append({
            "ticker": r.get("ticker", "N/A"),
            "name": r.get("name", ""),
            "display_label": display_label(r),
            "value": r.get("value_usd", 0),
            "manager": r["manager"],
            "filing_quarter_return": r.get("filing_quarter_return_pct"),
            "weight_in_combined": r["combined_weight"],
        })

    countries = []
    for name, d in country_data.items():
        top = sorted(d["stocks"], key=lambda x: x["weight_in_combined"], reverse=True)[:10]
        c_total = d["value"] or 1
        mgr_shares = sorted(
            [{"manager": m, "value": v, "pct_of_country": round(v / c_total * 100, 1)}
             for m, v in d["mgr_values"].items()],
            key=lambda x: -x["value"]
        )
        pct = sum(s["weight_in_combined"] for s in d["stocks"])
        countries.append({
            "name": name,
            "pct": round(pct, 2),
            "total_value": d["value"],
            "count": d["count"],
            "manager_shares": mgr_shares,
            "top_stocks": top,
        })
    countries.sort(key=lambda x: -x["pct"])

    return {"countries": countries}


def compute_category_stocks(all_rows, cat_type, cat_name, manager_weights=None, normalize_fn=None):
    """
    Get top stocks in a category (sector/industry/country) with manager breakdown.

    Args:
        all_rows: list of holding rows
        cat_type: "sector", "industry", or "country"
        cat_name: the category value to filter on (e.g., "Information Technology")
        manager_weights: optional {manager: weight} dict
        normalize_fn: optional function to normalize row values before comparing

    Returns:
        {stocks: [{ticker, name, short_name, pct, managers: [{name, pct}]}]}
    """
    # Filter rows by category
    filtered = []
    for r in all_rows:
        raw_val = r.get(cat_type) or "Unknown"
        val = normalize_fn(raw_val) if normalize_fn else raw_val
        if val == cat_name:
            filtered.append(r)
    if not filtered:
        return {"stocks": []}

    # Apply manager weights for combined portfolio %
    weighted, _ = _apply_manager_weights(all_rows, manager_weights)

    # Re-filter from weighted rows
    weighted_filtered = []
    for r in weighted:
        raw_val = r.get(cat_type) or "Unknown"
        val = normalize_fn(raw_val) if normalize_fn else raw_val
        if val == cat_name:
            weighted_filtered.append(r)

    # Aggregate by ticker
    by_ticker = defaultdict(lambda: {"combined_weight": 0, "name": "", "ticker": "", "managers": defaultdict(float)})
    for r in weighted_filtered:
        tk = r.get("ticker", "N/A")
        if tk == "N/A":
            continue
        d = by_ticker[tk]
        d["combined_weight"] += r.get("combined_weight", 0)
        d["name"] = r.get("name", "")
        d["ticker"] = tk
        d["managers"][r["manager"]] += r.get("combined_weight", 0)

    # Sort and take top 8
    top = sorted(by_ticker.values(), key=lambda x: -x["combined_weight"])[:8]
    stocks = []
    for s in top:
        stocks.append({
            "ticker": s["ticker"],
            "name": s["name"],
            "short_name": shorten_stock_name(s["name"]),
            "pct": round(s["combined_weight"], 2),
            "managers": sorted(
                [{"name": m, "pct": round(p, 2)} for m, p in s["managers"].items()],
                key=lambda x: -x["pct"]
            ),
        })
    return {"stocks": stocks}


def compute_qoq_diff(current_rows, previous_rows):
    """
    Compare two sets of holdings rows (current vs previous quarter).

    Returns dict keyed by manager name:
        {manager: {
            new_positions: [{name, ticker, value, pct}],
            exited_positions: [{name, ticker, value, pct}],
            changed_positions: [{name, ticker, prev_pct, curr_pct, change_pct,
                                  prev_value, curr_value, prev_rank, curr_rank}],
            unchanged_count: int
        }}
    """
    def _index_by_manager_ticker(rows):
        result = defaultdict(dict)
        for r in rows:
            result[r["manager"]][r.get("ticker", r["name"])] = r
        return result

    curr_idx = _index_by_manager_ticker(current_rows)
    prev_idx = _index_by_manager_ticker(previous_rows)

    all_managers = set(curr_idx.keys()) | set(prev_idx.keys())
    diff = {}

    for mgr in sorted(all_managers):
        curr_holdings = curr_idx.get(mgr, {})
        prev_holdings = prev_idx.get(mgr, {})

        curr_tickers = set(curr_holdings.keys())
        prev_tickers = set(prev_holdings.keys())

        new_tickers = curr_tickers - prev_tickers
        exited_tickers = prev_tickers - curr_tickers
        common_tickers = curr_tickers & prev_tickers

        new_positions = []
        for tk in sorted(new_tickers):
            r = curr_holdings[tk]
            new_positions.append({
                "name": r.get("name", ""), "ticker": r.get("ticker", "N/A"),
                "value": r.get("value_usd", 0), "pct": r.get("pct_of_portfolio", 0),
            })

        exited_positions = []
        for tk in sorted(exited_tickers):
            r = prev_holdings[tk]
            exited_positions.append({
                "name": r.get("name", ""), "ticker": r.get("ticker", "N/A"),
                "value": r.get("value_usd", 0), "pct": r.get("pct_of_portfolio", 0),
            })

        changed_positions = []
        unchanged = 0
        for tk in sorted(common_tickers):
            cr = curr_holdings[tk]
            pr = prev_holdings[tk]
            curr_pct = cr.get("pct_of_portfolio", 0)
            prev_pct = pr.get("pct_of_portfolio", 0)
            change = round(curr_pct - prev_pct, 2)

            if abs(change) >= 0.5:
                changed_positions.append({
                    "name": cr.get("name", ""), "ticker": cr.get("ticker", "N/A"),
                    "prev_pct": prev_pct, "curr_pct": curr_pct,
                    "change_pct": change,
                    "prev_value": pr.get("value_usd", 0),
                    "curr_value": cr.get("value_usd", 0),
                    "prev_rank": pr.get("rank", 0),
                    "curr_rank": cr.get("rank", 0),
                })
            else:
                unchanged += 1

        changed_positions.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

        diff[mgr] = {
            "new_positions": new_positions,
            "exited_positions": exited_positions,
            "changed_positions": changed_positions,
            "unchanged_count": unchanged,
        }

    return diff


# ── Key Highlights (5 institutional takeaways) ─────────────────────────────

def _generate_key_highlights(stats, sector_data, geo_data, top_stocks, qtd_ret,
                              fwd_pe, eps_growth, div_yield, exp_return,
                              sectors, countries, earn_parts, risk_parts,
                              weighted_rows, all_mgr_names, filing_qtr,
                              earn_beat_tickers=None, earn_miss_tickers=None):
    """Generate 5 punchy, data-driven key takeaways in institutional analyst voice."""
    highlights = []

    # 1. Sector concentration & recent performance
    h1_title = "Sector Tilt Defines the Portfolio"
    h1_body = ""
    if sectors:
        top_sec = sectors[0]
        top2_pct = sectors[0]["pct"] + (sectors[1]["pct"] if len(sectors) > 1 else 0)
        h1_body = (
            f"{top_sec['name']} dominates at {top_sec['pct']:.1f}% of portfolio weight"
            f"{', with the top two sectors comprising ' + f'{top2_pct:.0f}%' if len(sectors) > 1 else ''}. "
        )
        if len(sectors) >= 3:
            sec_names = ", ".join(s["name"] for s in sectors[:3])
            h1_body += f"The three largest sector bets are {sec_names}. "
        if qtd_ret is not None:
            direction = "up" if qtd_ret > 0 else "down"
            h1_body += f"The portfolio is {direction} {abs(qtd_ret):.1f}% QTD on a weighted basis"
            if filing_qtr:
                h1_body += f" as of {filing_qtr}"
            h1_body += ". "
        if top2_pct > 50:
            h1_body += "This level of sector concentration amplifies both upside and downside risk from sector-specific catalysts."
        else:
            h1_body += "Sector diversification is reasonable, limiting single-sector event risk."
    highlights.append({"title": h1_title, "body": h1_body.strip(),
                        "meta": {"sectors": [s["name"] for s in sectors[:3]], "tickers": []}})

    # 2. Style/strategy differences between managers
    h2_title = "Manager Strategies Show Distinct Fingerprints"
    h2_body = ""
    if len(all_mgr_names) > 1:
        # Analyze overlap in top holdings
        ticker_mgr_map = defaultdict(set)
        for r in weighted_rows:
            tk = r.get("ticker", "N/A")
            if tk != "N/A":
                ticker_mgr_map[tk].add(r["manager"])
        overlap_count = sum(1 for mgrs in ticker_mgr_map.values() if len(mgrs) > 1)
        total_tickers = len(ticker_mgr_map)
        overlap_pct = round(overlap_count / total_tickers * 100, 0) if total_tickers > 0 else 0

        h2_body = (
            f"Across {len(all_mgr_names)} managers, only {overlap_count} of {total_tickers} unique tickers "
            f"({overlap_pct:.0f}%) are held by multiple managers. "
        )
        if overlap_pct < 15:
            h2_body += "The low overlap implies highly differentiated investment theses and limited crowding risk. "
        elif overlap_pct < 30:
            h2_body += "Moderate overlap suggests some consensus positions but distinct portfolio construction. "
        else:
            h2_body += "Significant overlap indicates convergent thinking among managers, creating concentration in consensus names. "

        # Top shared positions
        shared = sorted(ticker_mgr_map.items(), key=lambda x: -len(x[1]))
        top_shared = [(tk, mgrs) for tk, mgrs in shared if len(mgrs) > 1][:3]
        if top_shared:
            shared_str = ", ".join(f"{tk} ({len(mgrs)} mgrs)" for tk, mgrs in top_shared)
            h2_body += f"Most widely held: {shared_str}."
    else:
        h2_body = (
            f"Single-manager portfolio. "
            f"Position sizing discipline is evident with the top 5 holdings at "
            f"{sum(s['pct'] for s in top_stocks[:5]):.1f}% of total weight."
        )
    if len(all_mgr_names) > 1:
        h2_meta_tickers = [tk for tk, _ in top_shared[:3]] if top_shared else []
    else:
        h2_meta_tickers = [s["ticker"] for s in top_stocks[:3]]
    highlights.append({"title": h2_title, "body": h2_body.strip(),
                        "meta": {"tickers": h2_meta_tickers, "sectors": []}})

    # 3. Valuation & forward growth
    h3_title = "Valuation Embeds Growth Premium"
    h3_parts = []
    if fwd_pe is not None and eps_growth is not None:
        peg = round(fwd_pe / eps_growth, 1) if eps_growth > 0 else None
        h3_parts.append(
            f"At {fwd_pe:.1f}x forward P/E against {eps_growth:.1f}% consensus EPS growth, "
            f"the portfolio"
        )
        if peg is not None:
            h3_parts[-1] += f" trades at a {peg:.1f}x PEG ratio"
            if peg < 1.0:
                h3_parts[-1] += " — attractively priced relative to growth."
            elif peg < 1.5:
                h3_parts[-1] += " — fairly valued for the growth profile."
            else:
                h3_parts[-1] += " — a premium multiple reflecting quality or momentum expectations."
        else:
            h3_parts[-1] += " carries negative earnings growth, suggesting the valuation is dependent on a turnaround thesis."
    elif fwd_pe is not None:
        h3_parts.append(f"The portfolio trades at {fwd_pe:.1f}x forward P/E.")
    if div_yield is not None:
        h3_parts.append(f"The {div_yield:.1f}% weighted dividend yield provides a return floor.")
    if exp_return is not None:
        h3_parts.append(
            f"Implied total return (EPS growth + yield) is {exp_return:.1f}%, "
            f"{'above long-run equity expectations' if exp_return > 10 else 'in line with historical averages' if exp_return > 6 else 'below typical equity return hurdles'}."
        )
    h3_body = " ".join(h3_parts) if h3_parts else "Insufficient valuation data to assess forward return expectations."
    highlights.append({"title": h3_title, "body": h3_body,
                        "meta": {"tickers": [], "sectors": []}})

    # 4. Earnings quality
    h4_title = "Earnings Execution Signals Quality"
    eps_beat_rate = stats.get("eps_beat_rate")
    eps_beat_count = stats.get("eps_beat_count", 0)
    eps_total = stats.get("eps_total_count", 0)
    h4_parts = []
    if eps_beat_rate is not None and eps_total > 0:
        h4_parts.append(
            f"Of {eps_total} portfolio holdings with reported earnings, {eps_beat_count} ({eps_beat_rate:.0f}%) "
            f"beat consensus EPS estimates. "
        )
        if eps_beat_rate >= 75:
            h4_parts.append("This is a strong beat rate, suggesting the portfolio skews toward companies with execution discipline and conservative guidance. ")
        elif eps_beat_rate >= 60:
            h4_parts.append("Beat rates are in line with the broader market average, reflecting typical sell-side estimate accuracy. ")
        else:
            h4_parts.append("Below-average beat rates suggest either aggressive consensus expectations or fundamental headwinds in several holdings. ")
        # Pull notable beats/misses from earn_parts if available
        for ep in earn_parts:
            if "Notable beats:" in ep or "Notable misses:" in ep:
                h4_parts.append(ep + " ")
    else:
        h4_parts.append("Earnings data not yet available for this reporting period.")
    h4_body = "".join(h4_parts).strip()
    h4_meta_tickers = (earn_beat_tickers or [])[:3] + (earn_miss_tickers or [])[:3]
    highlights.append({"title": h4_title, "body": h4_body,
                        "meta": {"tickers": h4_meta_tickers, "sectors": []}})

    # 5. Cross-cutting theme / risk-opportunity
    h5_title = "Key Risk: What Could Derail This Portfolio"
    h5_parts = []
    us_pct = next((c["pct"] for c in countries if c["name"] == "United States"), 0) if countries else 0
    if sectors and sectors[0]["pct"] > 35:
        h5_parts.append(
            f"The portfolio's {sectors[0]['pct']:.0f}% allocation to {sectors[0]['name']} "
            f"creates outsized sensitivity to sector-specific regulation, multiple compression, or earnings deceleration in that space. "
        )
    if top_stocks and top_stocks[0]["pct"] > 8:
        h5_parts.append(
            f"Single-name risk is elevated with {top_stocks[0]['ticker']} at {top_stocks[0]['pct']:.1f}% — "
            f"an adverse event in this name alone could materially impact portfolio returns. "
        )
    if us_pct > 90:
        h5_parts.append(
            f"At {us_pct:.0f}% domestic exposure, the portfolio has minimal international diversification, "
            f"leaving it fully exposed to US macro and policy risk. "
        )
    elif us_pct < 60:
        intl_pct = 100 - us_pct
        h5_parts.append(
            f"With {intl_pct:.0f}% international exposure, currency translation and geopolitical risk are non-trivial factors. "
        )
    if not h5_parts:
        h5_parts.append("The portfolio shows reasonable diversification across names, sectors, and geographies. Idiosyncratic risk appears well-managed.")
    h5_body = "".join(h5_parts).strip()
    h5_meta_tickers = [top_stocks[0]["ticker"]] if top_stocks and top_stocks[0]["pct"] > 8 else []
    h5_meta_sectors = [sectors[0]["name"]] if sectors and sectors[0]["pct"] > 35 else []
    highlights.append({"title": h5_title, "body": h5_body,
                        "meta": {"tickers": h5_meta_tickers, "sectors": h5_meta_sectors}})

    return highlights


# ── Written Analysis (CIO-oriented) ────────────────────────────────────────

def generate_written_analysis(all_rows, manager_weights=None):
    """
    Generate CIO-oriented written analysis paragraphs from actual data.
    Returns dict with sections: overview, sectors, geography, valuation,
    earnings, risks — each a string paragraph.
    """
    if not all_rows:
        return {}

    stats = compute_summary_stats(all_rows, manager_weights)
    sector_data = compute_sector_breakdown(all_rows)
    geo_data = compute_geo_breakdown(all_rows)
    weighted_rows, _ = _apply_manager_weights(all_rows, manager_weights)

    managers = list({r["manager"] for r in all_rows})
    n_mgrs = len(managers)
    unique = stats.get("unique_stocks", 0)
    total_value = stats.get("total_value", 0)
    total_str = f"${total_value / 1e9:.1f}B" if total_value >= 1e9 else f"${total_value / 1e6:.0f}M"
    filing_qtr = stats.get("filing_quarter", "")

    # Top positions
    top_stocks = stats.get("top_stocks_by_pct", [])
    top5_pct = sum(s["pct"] for s in top_stocks[:5]) if top_stocks else 0

    # QTD return from weighted return
    wr = stats.get("weighted_return", {})
    qtd_ret = wr.get("qtd_weighted_return")

    # ── Manager weight listing ──
    mgr_weight_parts = []
    all_mgr_names = sorted({r["manager"] for r in all_rows})
    wts = manager_weights or {}
    has_wts = any(v > 0 for v in wts.values())
    if has_wts and len(all_mgr_names) > 1:
        for m in all_mgr_names:
            w = wts.get(m, 0)
            if w > 0:
                mgr_weight_parts.append(f"{m} ({w:.0f}%)")
    elif len(all_mgr_names) > 1:
        eq = round(100.0 / len(all_mgr_names), 1)
        for m in all_mgr_names:
            mgr_weight_parts.append(f"{m} ({eq:.0f}%)")

    # ── Portfolio Overview ──
    overview_parts = [
        f"The combined portfolio encompasses {n_mgrs} institutional manager{'s' if n_mgrs > 1 else ''} "
        f"with {unique} unique stocks and aggregate holdings of {total_str}",
    ]
    if filing_qtr:
        overview_parts[0] += f" as of {filing_qtr}"
    overview_parts[0] += "."

    if mgr_weight_parts:
        overview_parts.append(
            f"Portfolio weights: {', '.join(mgr_weight_parts)}."
        )

    if top_stocks:
        top3 = ", ".join(f"{s['ticker']} ({s['pct']:.1f}%)" for s in top_stocks[:3])
        overview_parts.append(
            f"The top 5 positions account for {top5_pct:.1f}% of portfolio weight, "
            f"led by {top3}."
        )
    if qtd_ret is not None:
        direction = "gained" if qtd_ret > 0 else "declined"
        overview_parts.append(
            f"The portfolio has {direction} {abs(qtd_ret):.1f}% quarter-to-date on a weighted basis."
        )

    # ── Sector Positioning ──
    sectors = sector_data.get("sectors", [])
    sector_parts = []
    if sectors:
        top_sec = sectors[0]
        top3_sec = ", ".join(f"{s['name']} ({s['pct']:.1f}%)" for s in sectors[:3])
        sector_parts.append(f"Sector allocation is led by {top3_sec}.")
        if len(sectors) >= 2:
            top2_pct = sectors[0]["pct"] + sectors[1]["pct"]
            sector_parts.append(
                f"The top two sectors represent {top2_pct:.0f}% of the portfolio, "
                f"indicating {'significant concentration' if top2_pct > 50 else 'moderate diversification'}."
            )

    # ── Geographic Exposure ──
    countries = geo_data.get("countries", [])
    geo_parts = []
    if countries:
        us_pct = next((c["pct"] for c in countries if c["name"] == "United States"), 0)
        intl_pct = round(100 - us_pct, 1)
        geo_parts.append(f"Geographic allocation is {us_pct:.0f}% US / {intl_pct:.0f}% international.")
        intl_countries = [c for c in countries if c["name"] != "United States" and c["pct"] >= 1.0]
        if intl_countries:
            top_intl = ", ".join(f"{c['name']} ({c['pct']:.1f}%)" for c in intl_countries[:4])
            geo_parts.append(f"Notable international exposures include {top_intl}.")

    # ── Valuation & Growth ──
    fwd_pe = stats.get("weighted_forward_pe")
    eps_growth = stats.get("weighted_eps_growth")
    div_yield = stats.get("weighted_div_yield")
    exp_return = stats.get("expected_return")
    val_parts = []
    if fwd_pe is not None:
        val_parts.append(f"The portfolio trades at {fwd_pe:.1f}x weighted forward P/E.")
    if eps_growth is not None:
        val_parts.append(f"Consensus forward EPS growth is {eps_growth:.1f}%.")
    if div_yield is not None:
        val_parts.append(f"Weighted dividend yield is {div_yield:.1f}%.")
    if exp_return is not None:
        val_parts.append(
            f"Implied expected return (EPS growth + dividend yield) is {exp_return:.1f}%, "
            f"suggesting a {'constructive' if exp_return > 8 else 'moderate'} forward outlook."
        )

    # ── Earnings Quality ──
    eps_beat_rate = stats.get("eps_beat_rate")
    eps_beat_count = stats.get("eps_beat_count", 0)
    eps_total = stats.get("eps_total_count", 0)
    earn_parts = []
    unique_beats, unique_misses = [], []
    if eps_beat_rate is not None and eps_total > 0:
        earn_parts.append(
            f"{eps_beat_rate:.0f}% of portfolio holdings ({eps_beat_count} of {eps_total}) "
            f"beat consensus EPS estimates in the reporting quarter."
        )
        # Find notable beats/misses from weighted rows
        beat_rows = []
        miss_rows = []
        for r in weighted_rows:
            beat_d = r.get("filing_eps_beat_dollars")
            beat_p = r.get("filing_eps_beat_pct")
            if beat_d is not None and beat_p is not None:
                entry = {"ticker": r.get("ticker", "N/A"), "name": shorten_stock_name(r.get("name", "")),
                         "beat_pct": beat_p, "weight": r.get("combined_weight", 0)}
                if beat_d > 0 and abs(beat_p) >= 5:
                    beat_rows.append(entry)
                elif beat_d < 0 and abs(beat_p) >= 5:
                    miss_rows.append(entry)
        # Deduplicate by ticker
        seen_tk = set()
        unique_beats, unique_misses = [], []
        for b in sorted(beat_rows, key=lambda x: -x["beat_pct"]):
            if b["ticker"] not in seen_tk:
                seen_tk.add(b["ticker"])
                unique_beats.append(b)
        seen_tk.clear()
        for m in sorted(miss_rows, key=lambda x: x["beat_pct"]):
            if m["ticker"] not in seen_tk:
                seen_tk.add(m["ticker"])
                unique_misses.append(m)
        if unique_beats[:3]:
            beat_str = ", ".join(f"{b['ticker']} (+{b['beat_pct']:.0f}%)" for b in unique_beats[:3])
            earn_parts.append(f"Notable beats: {beat_str}.")
        if unique_misses[:3]:
            miss_str = ", ".join(f"{m['ticker']} ({m['beat_pct']:.0f}%)" for m in unique_misses[:3])
            earn_parts.append(f"Notable misses: {miss_str}.")

    # ── Key Risks ──
    risk_parts = []
    if top5_pct > 30:
        risk_parts.append(
            f"Concentration risk: Top 5 positions represent {top5_pct:.0f}% of the portfolio."
        )
    if sectors and sectors[0]["pct"] > 30:
        risk_parts.append(
            f"Sector concentration: {sectors[0]['name']} at {sectors[0]['pct']:.0f}% creates single-sector risk."
        )
    if top_stocks and top_stocks[0]["pct"] > 8:
        risk_parts.append(
            f"Single-name risk: {top_stocks[0]['ticker']} represents {top_stocks[0]['pct']:.1f}% of the portfolio."
        )
    if not risk_parts:
        risk_parts.append("The portfolio shows reasonable diversification across positions and sectors.")

    # ── Key Highlights (5 institutional-quality takeaways) ──
    _earn_beat_tks = [b["ticker"] for b in unique_beats[:3]]
    _earn_miss_tks = [m["ticker"] for m in unique_misses[:3]]
    highlights = _generate_key_highlights(
        stats, sector_data, geo_data, top_stocks, qtd_ret,
        fwd_pe, eps_growth, div_yield, exp_return,
        sectors, countries, earn_parts, risk_parts,
        weighted_rows, all_mgr_names, filing_qtr,
        earn_beat_tickers=_earn_beat_tks, earn_miss_tickers=_earn_miss_tks,
    )

    return {
        "highlights": highlights,
        "overview": " ".join(overview_parts),
        "sectors": " ".join(sector_parts),
        "geography": " ".join(geo_parts),
        "valuation": " ".join(val_parts),
        "earnings": " ".join(earn_parts),
        "risks": " ".join(risk_parts),
    }


# ── Portfolio Table Data ────────────────────────────────────────────────────

def compute_portfolio_table_data(all_rows, manager_weights=None, top_n=10):
    """
    Compute portfolio table data for the UI: weighted combined portfolio + per-manager.

    Returns dict:
        weighted: {rows: [{pct, name, ticker, filing_price, current_price, qtd_return,
                           forward_pe, forward_eps_growth, filing_reported_eps, eps_beat,
                           trailing_eps, forward_eps}], totals: {...}}
        managers: {manager_name: {rows: [...], totals: {...}}}
    """
    if not all_rows:
        return {"weighted": {"rows": [], "totals": {}}, "managers": {}}

    weighted_rows, _ = _apply_manager_weights(all_rows, manager_weights)

    def _build_table(rows, use_combined_weight=False):
        """Build table rows from a list of holdings."""
        # Aggregate by ticker
        by_ticker = {}
        for r in rows:
            tk = r.get("ticker", "N/A")
            key = tk if tk != "N/A" else r.get("name", "Unknown")
            if key not in by_ticker:
                by_ticker[key] = {
                    "ticker": tk,
                    "name": r.get("name", "Unknown"),
                    "sector": None,
                    "industry": None,
                    "pct": 0.0,
                    "filing_price": None,
                    "current_price": None,
                    "qtd_return": None,
                    "forward_pe": None,
                    "forward_eps_growth": None,
                    "filing_reported_eps": None,
                    "eps_beat_dollars": None,
                    "eps_beat_pct": None,
                    "monthly_returns": None,
                }
            d = by_ticker[key]
            if use_combined_weight:
                d["pct"] += r.get("combined_weight", 0)
            else:
                d["pct"] += r.get("pct_of_portfolio", 0)
            # Take first non-None for each field
            if d["sector"] is None and r.get("sector"):
                d["sector"] = r["sector"]
            if d["industry"] is None and r.get("industry"):
                d["industry"] = r["industry"]
            if d["monthly_returns"] is None and r.get("monthly_returns"):
                d["monthly_returns"] = r["monthly_returns"]
            for field, src in [
                ("filing_price", "filing_price_qtr_end"),
                ("current_price", "current_price"),
                ("qtd_return", "qtd_return_pct"),
                ("forward_pe", "forward_pe"),
                ("forward_eps_growth", "forward_eps_growth"),
                ("filing_reported_eps", "filing_reported_eps"),
                ("eps_beat_dollars", "filing_eps_beat_dollars"),
                ("eps_beat_pct", "filing_eps_beat_pct"),
            ]:
                if d[field] is None and r.get(src) is not None:
                    d[field] = r[src]

        # Sort by pct descending
        all_stocks = sorted(by_ticker.values(), key=lambda x: -x["pct"])

        # Compute totals from ALL holdings (not just top_n)
        total_pct = sum(s["pct"] for s in all_stocks)
        # Weighted QTD return, P/E (harmonic), EPS growth
        qtd_sum, qtd_wt = 0.0, 0.0
        pe_inv_sum, pe_wt = 0.0, 0.0
        eg_sum, eg_wt = 0.0, 0.0
        for s in all_stocks:
            w = s["pct"]
            if w <= 0:
                continue
            if s["qtd_return"] is not None:
                qtd_sum += s["qtd_return"] * w
                qtd_wt += w
            if s["forward_pe"] is not None and s["forward_pe"] > 0:
                pe_inv_sum += w / s["forward_pe"]
                pe_wt += w
            clamped_eg = _clamp_growth(s["forward_eps_growth"])
            if clamped_eg is not None:
                eg_sum += clamped_eg * w
                eg_wt += w

        # Compute weighted monthly returns for totals
        # Find month labels from first stock that has monthly_returns
        month_labels = []
        for s in all_stocks:
            if s.get("monthly_returns"):
                month_labels = [m["month"] for m in s["monthly_returns"]]
                break
        monthly_totals = []
        if month_labels:
            for mi in range(len(month_labels)):
                m_sum, m_wt = 0.0, 0.0
                for s in all_stocks:
                    w = s["pct"]
                    if w <= 0:
                        continue
                    mr = s.get("monthly_returns")
                    if mr and mi < len(mr) and mr[mi].get("return_pct") is not None:
                        m_sum += mr[mi]["return_pct"] * w
                        m_wt += w
                monthly_totals.append({
                    "month": month_labels[mi],
                    "return_pct": round(m_sum / m_wt, 2) if m_wt > 0 else None,
                })

        totals = {
            "pct": round(total_pct, 2),
            "qtd_return": round(qtd_sum / qtd_wt, 2) if qtd_wt > 0 else None,
            "forward_pe": round(pe_wt / pe_inv_sum, 2) if pe_inv_sum > 0 else None,
            "forward_eps_growth": round(eg_sum / eg_wt, 2) if eg_wt > 0 else None,
        }
        if monthly_totals:
            totals["monthly_returns"] = monthly_totals

        # Format rows for top_n display
        display_rows = []
        for s in all_stocks[:top_n]:
            row_data = {
                "pct": round(s["pct"], 2),
                "name": shorten_stock_name(s["name"]),
                "ticker": s["ticker"],
                "sector": s.get("sector") or "—",
                "industry": s.get("industry") or "—",
                "filing_price": s["filing_price"],
                "current_price": s["current_price"],
                "qtd_return": s["qtd_return"],
                "forward_pe": s["forward_pe"],
                "forward_eps_growth": s["forward_eps_growth"],
                "filing_reported_eps": s["filing_reported_eps"],
                "eps_beat_dollars": s["eps_beat_dollars"],
                "eps_beat_pct": s["eps_beat_pct"],
            }
            if s.get("monthly_returns"):
                row_data["monthly_returns"] = s["monthly_returns"]
            display_rows.append(row_data)

        return {"rows": display_rows, "totals": totals}

    # Weighted combined portfolio
    weighted_table = _build_table(weighted_rows, use_combined_weight=True)

    # Per-manager tables
    by_mgr = defaultdict(list)
    for r in all_rows:
        by_mgr[r["manager"]].append(r)

    manager_tables = {}
    for mgr, rows in sorted(by_mgr.items()):
        manager_tables[mgr] = _build_table(rows, use_combined_weight=False)

    return {"weighted": weighted_table, "managers": manager_tables}
