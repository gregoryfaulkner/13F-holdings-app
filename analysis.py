"""
Portfolio Analysis Functions
============================
Overlap analysis, sector breakdown, quarter-over-quarter diff,
weighted portfolio return, and summary statistics for SEC 13F holdings data.
"""

from collections import defaultdict, Counter


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

    # Compute weighted forward metrics (deduplicated by ticker)
    wtd_inv_pe_sum = 0.0   # sum of (weight / forward_pe) for harmonic mean
    wtd_inv_pe_wt = 0.0
    wtd_eps_growth = 0.0
    wtd_eps_wt = 0.0
    wtd_div_yield = 0.0
    wtd_div_wt = 0.0
    for tk_data in pct_by_ticker.values():
        w = tk_data["combined_weight"]
        if w <= 0:
            continue
        fpe = tk_data.get("forward_pe")
        if fpe is not None and fpe > 0:
            wtd_inv_pe_sum += w / fpe
            wtd_inv_pe_wt += w
        feg = tk_data.get("forward_eps_growth")
        if feg is not None:
            wtd_eps_growth += w * feg
            wtd_eps_wt += w
        dy = tk_data.get("dividend_yield")
        if dy is not None:
            wtd_div_yield += w * dy
            wtd_div_wt += w

    weighted_forward_pe = round(wtd_inv_pe_wt / wtd_inv_pe_sum, 2) if wtd_inv_pe_sum > 0 else None
    weighted_eps_growth = round(wtd_eps_growth / wtd_eps_wt, 2) if wtd_eps_wt > 0 else None
    weighted_div_yield = round(wtd_div_yield / wtd_div_wt, 2) if wtd_div_wt > 0 else None
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
            })
            if len(stocks) >= top_n:
                break

    # Portfolio-level averages (harmonic for P/E, weighted arithmetic for others)
    wtd_inv_pe_sum, wtd_inv_pe_wt = 0.0, 0.0
    wtd_eg_sum, wtd_eg_wt = 0.0, 0.0
    wtd_dy_sum, wtd_dy_wt = 0.0, 0.0
    for tk_data in by_ticker.values():
        w = tk_data["combined_weight"]
        if w <= 0:
            continue
        fpe = tk_data.get("forward_pe")
        if fpe is not None and fpe > 0:
            wtd_inv_pe_sum += w / fpe
            wtd_inv_pe_wt += w
        feg = tk_data.get("forward_eps_growth")
        if feg is not None:
            wtd_eg_sum += w * feg
            wtd_eg_wt += w
        dy = tk_data.get("dividend_yield")
        if dy is not None:
            wtd_dy_sum += w * dy
            wtd_dy_wt += w

    avg_pe = round(wtd_inv_pe_wt / wtd_inv_pe_sum, 2) if wtd_inv_pe_sum > 0 else None
    avg_eg = round(wtd_eg_sum / wtd_eg_wt, 2) if wtd_eg_wt > 0 else None
    avg_dy = round(wtd_dy_sum / wtd_dy_wt, 2) if wtd_dy_wt > 0 else None
    exp_ret = round(avg_eg + avg_dy, 2) if avg_eg is not None and avg_dy is not None else None

    return {
        "stocks": stocks,
        "portfolio_avg": {
            "forward_pe": avg_pe,
            "eps_growth": avg_eg,
            "div_yield": avg_dy,
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

    sector_data = defaultdict(lambda: {"value": 0, "count": 0, "stocks": []})
    industry_data = defaultdict(lambda: {"value": 0, "count": 0, "sector": ""})
    manager_sector = defaultdict(lambda: defaultdict(float))

    for r in all_rows:
        sector = r.get("sector") or "Unknown"
        industry = r.get("industry") or "Unknown"
        val = r.get("value_usd", 0)

        sector_data[sector]["value"] += val
        sector_data[sector]["count"] += 1
        sector_data[sector]["stocks"].append({
            "ticker": r.get("ticker", "N/A"),
            "name": r.get("name", ""),
            "display_label": display_label(r),
            "value": val,
        })

        industry_data[industry]["value"] += val
        industry_data[industry]["count"] += 1
        industry_data[industry]["sector"] = sector

        manager_sector[r["manager"]][sector] += val

    sectors = []
    for name, d in sector_data.items():
        top = sorted(d["stocks"], key=lambda x: x["value"], reverse=True)[:5]
        sectors.append({
            "name": name,
            "total_value": d["value"],
            "pct": round(d["value"] / total_value * 100, 2),
            "count": d["count"],
            "top_stocks": top,
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

    country_data = defaultdict(lambda: {"value": 0, "count": 0})
    for r in all_rows:
        country = r.get("country") or "Unknown"
        val = r.get("value_usd", 0)
        country_data[country]["value"] += val
        country_data[country]["count"] += 1

    countries = []
    for name, d in country_data.items():
        countries.append({
            "name": name,
            "total_value": d["value"],
            "pct": round(d["value"] / total_value * 100, 2),
            "count": d["count"],
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
