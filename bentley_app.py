"""
Investment Manager Holdings Dashboard — Web UI
===============================================
A Flask web app that wraps holdings.py with a browser-based interface.
Manage managers, configure settings, run fetches, and view treemap — all from your browser.

Usage:
    pip install flask edgartools matplotlib numpy
    python bentley_app.py

Then open http://localhost:5000
"""

import io
import json
import os
import queue
import sys
import threading
import time
import zipfile
from datetime import datetime

from flask import Flask, jsonify, request, Response, send_from_directory, send_file

# ── App setup ─────────────────────────────────────────────────────────────────

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, "holdings_config.json")

app = Flask(__name__)

# Global state for SSE streaming
progress_queues = {}
run_lock = threading.Lock()
last_results = {}  # store latest run results

# ── Config persistence ────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "managers_13f": {
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
    },
    "managers_nport": {},
    "top_n": 20,
    "max_date": "2025-12-31",
    "identity": "Investment Manager Holdings Fetcher holdings@example.com",
}


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            if k not in cfg:
                cfg[k] = v
        return cfg
    return dict(DEFAULT_CONFIG)


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


# ── Holdings engine (imports from holdings.py) ────────────────────────────────

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import holdings  # noqa: E402


def run_fetch(cfg, progress_q):
    """Run the full holdings fetch using config, streaming progress to queue."""
    import csv as csv_mod
    import importlib

    importlib.reload(holdings)
    from edgar import set_identity
    set_identity(cfg.get("identity", DEFAULT_CONFIG["identity"]))
    holdings.MAX_DATE = cfg.get("max_date", "2025-12-31")
    holdings.TOP_N = cfg.get("top_n", 20)
    holdings.OUTPUT_DIR = APP_DIR

    combined_rows = []
    run_date = datetime.today().strftime("%Y%m%d")
    results = {"managers": [], "csvs": [], "errors": [], "all_rows": []}

    def log(msg):
        progress_q.put({"type": "log", "message": msg})

    log(f"Starting fetch — MAX_DATE: {holdings.MAX_DATE} | TOP_N: {holdings.TOP_N}")

    managers_13f = cfg.get("managers_13f", {})
    total = len(managers_13f) + len(cfg.get("managers_nport", {}))
    done = 0

    for manager_name, cik in managers_13f.items():
        log(f"[13F] Fetching {manager_name} (CIK: {cik})...")
        try:
            period, filed_at, n_total, rows = holdings.fetch_13f(manager_name, cik)
            log(f"  ✅ {manager_name}: {n_total} positions | Period: {period}")
            filename = holdings.write_individual_csv(manager_name, period, rows)
            combined_rows.extend(rows)
            results["managers"].append({
                "name": manager_name, "status": "success",
                "period": period, "filed_at": filed_at,
                "positions": n_total, "csv": filename,
            })
            results["csvs"].append(filename)
        except Exception as e:
            log(f"  ❌ {manager_name}: {e}")
            results["managers"].append({
                "name": manager_name, "status": "error", "error": str(e),
            })
            results["errors"].append(f"{manager_name}: {e}")
        done += 1
        progress_q.put({"type": "progress", "done": done, "total": total})

    for manager_name, info in cfg.get("managers_nport", {}).items():
        log(f"[N-PORT] Fetching {manager_name} (CIK: {info['cik']})...")
        try:
            period, filed_at, n_total, rows = holdings.fetch_nport(
                manager_name, info["cik"], info["series_keyword"]
            )
            log(f"  ✅ {manager_name}: {n_total} holdings | Period: {period}")
            filename = holdings.write_individual_csv(manager_name, period, rows)
            combined_rows.extend(rows)
            results["managers"].append({
                "name": manager_name, "status": "success",
                "period": period, "filed_at": filed_at,
                "positions": n_total, "csv": filename,
            })
            results["csvs"].append(filename)
        except Exception as e:
            log(f"  ❌ {manager_name}: {e}")
            results["managers"].append({
                "name": manager_name, "status": "error", "error": str(e),
            })
            results["errors"].append(f"{manager_name}: {e}")
        done += 1
        progress_q.put({"type": "progress", "done": done, "total": total})

    # Combined CSV
    if combined_rows:
        combined_name = f"all_managers_top20_{run_date}.csv"
        combined_path = os.path.join(APP_DIR, combined_name)
        with open(combined_path, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=holdings.FIELDNAMES)
            writer.writeheader()
            writer.writerows(combined_rows)
        results["csvs"].append(combined_name)
        log(f"Combined CSV: {combined_name} ({len(combined_rows)} rows)")

    # Store all rows for treemap
    results["all_rows"] = combined_rows
    results["run_date"] = run_date

    log("Done!")
    progress_q.put({"type": "complete", "results": {
        k: v for k, v in results.items() if k != "all_rows"
    }})
    return results


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(load_config())


@app.route("/api/config", methods=["POST"])
def update_config():
    cfg = load_config()
    updates = request.json
    cfg.update(updates)
    save_config(cfg)
    return jsonify({"status": "ok"})


@app.route("/api/managers", methods=["POST"])
def add_manager():
    cfg = load_config()
    data = request.json
    name = data.get("name", "").strip()
    cik = data.get("cik", "").strip()
    mtype = data.get("type", "13f")

    if not name or not cik:
        return jsonify({"error": "Name and CIK are required"}), 400

    if mtype == "nport":
        keyword = data.get("series_keyword", "").strip()
        if not keyword:
            return jsonify({"error": "Series keyword is required for N-PORT"}), 400
        cfg["managers_nport"][name] = {"cik": cik, "series_keyword": keyword}
    else:
        cfg["managers_13f"][name] = cik

    save_config(cfg)
    return jsonify({"status": "ok"})


@app.route("/api/managers", methods=["DELETE"])
def delete_manager():
    cfg = load_config()
    data = request.json
    name = data.get("name", "")
    mtype = data.get("type", "13f")

    if mtype == "nport":
        cfg["managers_nport"].pop(name, None)
    else:
        cfg["managers_13f"].pop(name, None)

    save_config(cfg)
    return jsonify({"status": "ok"})


@app.route("/api/run", methods=["POST"])
def start_run():
    if not run_lock.acquire(blocking=False):
        return jsonify({"error": "A fetch is already running"}), 409

    cfg = load_config()
    run_id = str(int(time.time()))
    q = queue.Queue()
    progress_queues[run_id] = q

    def _run():
        global last_results
        try:
            results = run_fetch(cfg, q)
            last_results = results
        finally:
            run_lock.release()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"run_id": run_id})


@app.route("/api/stream/<run_id>")
def stream(run_id):
    q = progress_queues.get(run_id)
    if not q:
        return jsonify({"error": "Unknown run ID"}), 404

    def generate():
        while True:
            try:
                msg = q.get(timeout=120)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") == "complete":
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/results")
def get_results():
    # Return results without the big all_rows (sent separately via /api/treemap-data)
    safe = {k: v for k, v in last_results.items() if k != "all_rows"}
    return jsonify(safe)


@app.route("/api/treemap-data")
def treemap_data():
    """Return all_rows grouped by manager for the treemap."""
    rows = last_results.get("all_rows", [])
    if not rows:
        return jsonify({"managers": []})

    from collections import defaultdict
    by_manager = defaultdict(list)
    for r in rows:
        by_manager[r["manager"]].append({
            "name": r["name"],
            "ticker": r["ticker"],
            "pct": r["pct_of_portfolio"],
            "value": r["value_usd"],
        })

    managers = []
    for mgr_name, stocks in by_manager.items():
        managers.append({"manager": mgr_name, "stocks": stocks})

    return jsonify({"managers": managers})


@app.route("/api/search-company")
def search_company():
    """Search SEC EDGAR company search for 13F filers → name + CIK."""
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    import urllib.request
    import urllib.parse
    import xml.etree.ElementTree as ET

    ua = "Investment Manager Holdings Fetcher holdings@example.com"

    # Use EDGAR company search (Atom XML output) filtered to 13F-HR filers
    try:
        search_url = (
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            f"company={urllib.parse.quote(q)}&CIK=&type=13F-HR&dateb="
            "&owner=include&count=10&search_text=&action=getcompany&output=atom"
        )
        req = urllib.request.Request(search_url, headers={
            "User-Agent": ua,
            "Accept": "application/atom+xml",
        })
        with urllib.request.urlopen(req, timeout=8) as resp:
            xml_data = resp.read().decode("utf-8")

        root = ET.fromstring(xml_data)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        matches = []
        for entry in root.findall("atom:entry", ns):
            title_el = entry.find("atom:title", ns)
            summary_el = entry.find("atom:summary", ns)
            if title_el is None or title_el.text is None:
                continue

            title = title_el.text.strip()
            # Title format: "COMPANY NAME CIK_NUMBER"  or just company name
            # Extract CIK from summary: "CIK=0001061768, ..."
            cik = ""
            if summary_el is not None and summary_el.text:
                import re
                cik_match = re.search(r"CIK=(\d+)", summary_el.text)
                if cik_match:
                    cik = str(int(cik_match.group(1)))  # strip leading zeros

            # Also try extracting CIK from the link href
            if not cik:
                link_el = entry.find("atom:link", ns)
                if link_el is not None:
                    href = link_el.get("href", "")
                    cik_match = re.search(r"CIK=(\d+)", href)
                    if cik_match:
                        cik = str(int(cik_match.group(1)))

            # Clean up title (remove trailing CIK if present)
            import re
            name = re.sub(r"\s+\d{10}$", "", title).strip()
            if not name:
                name = title

            if cik:
                matches.append({"name": name, "cik": cik})

        if matches:
            return jsonify(matches)
    except Exception:
        pass

    return jsonify([])


@app.route("/api/download-all")
def download_all():
    """Create a ZIP of all CSV output files and return it."""
    csvs = last_results.get("csvs", [])
    if not csvs:
        return jsonify({"error": "No files to download. Run a fetch first."}), 404

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in csvs:
            fpath = os.path.join(APP_DIR, fname)
            if os.path.exists(fpath):
                zf.write(fpath, fname)

    buf.seek(0)
    run_date = last_results.get("run_date", datetime.today().strftime("%Y%m%d"))
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"manager_holdings_{run_date}.zip",
    )


@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(APP_DIR, filename)


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Investment Manager Holdings</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; }
.card { background: #1e293b; border-radius: 12px; border: 1px solid #334155; }
.btn-primary { background: #3b82f6; color: white; padding: 8px 20px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
.btn-primary:hover { background: #2563eb; transform: translateY(-1px); }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
.btn-secondary { background: #475569; color: white; padding: 8px 20px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
.btn-secondary:hover { background: #64748b; }
.btn-danger { background: #ef4444; color: white; padding: 4px 12px; border-radius: 6px; font-size: 13px; cursor: pointer; }
.btn-danger:hover { background: #dc2626; }
.input-field { background: #0f172a; border: 1px solid #475569; color: #e2e8f0; padding: 8px 12px; border-radius: 8px; width: 100%; }
.input-field:focus { outline: none; border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59,130,246,0.3); }
select.input-field { appearance: auto; }
.log-area { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; height: 300px; overflow-y: auto; white-space: pre-wrap; line-height: 1.6; }
.progress-bar { height: 6px; background: #334155; border-radius: 3px; overflow: hidden; }
.progress-fill { height: 100%; background: linear-gradient(90deg, #3b82f6, #8b5cf6); transition: width 0.3s; border-radius: 3px; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; padding: 10px 12px; color: #94a3b8; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #334155; }
td { padding: 10px 12px; border-bottom: 1px solid #1e293b; }
tr:hover td { background: rgba(59,130,246,0.05); }
.tab { padding: 8px 16px; cursor: pointer; border-bottom: 2px solid transparent; color: #94a3b8; font-weight: 500; }
.tab.active { border-color: #3b82f6; color: #e2e8f0; }
.fade-in { animation: fadeIn 0.3s ease; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
.badge { font-size: 11px; padding: 2px 8px; border-radius: 12px; font-weight: 600; }
.badge-13f { background: #1e3a5f; color: #60a5fa; }
.badge-nport { background: #3b1f4e; color: #c084fc; }

/* Typeahead dropdown */
.typeahead-dropdown { position: absolute; top: 100%; left: 0; right: 0; background: #1e293b; border: 1px solid #475569; border-radius: 8px; max-height: 240px; overflow-y: auto; z-index: 50; display: none; }
.typeahead-dropdown.active { display: block; }
.typeahead-item { padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #334155; }
.typeahead-item:hover, .typeahead-item.selected { background: #334155; }
.typeahead-item .ta-name { color: #e2e8f0; font-weight: 500; }
.typeahead-item .ta-meta { color: #94a3b8; font-size: 12px; }

/* Treemap */
.treemap-container { display: grid; gap: 8px; }
.manager-box { background: #1e293b; border-radius: 10px; border: 1px solid #334155; overflow: hidden; }
.manager-box-header { padding: 8px 12px; font-weight: 700; font-size: 14px; color: #e2e8f0; border-bottom: 1px solid #334155; background: rgba(59,130,246,0.08); }
.stock-grid { display: flex; flex-wrap: wrap; padding: 6px; gap: 3px; }
.stock-cell { display: flex; align-items: center; justify-content: center; text-align: center; border-radius: 4px; font-size: 11px; color: #fff; overflow: hidden; padding: 2px; line-height: 1.2; font-weight: 600; min-width: 30px; min-height: 26px; cursor: default; transition: filter 0.15s; }
.stock-cell:hover { filter: brightness(1.2); z-index: 1; }
.stock-cell .cell-inner { display: flex; flex-direction: column; align-items: center; gap: 0; }
.stock-cell .cell-name { font-weight: 600; font-size: 10px; }
.stock-cell .cell-ticker { font-weight: 700; font-size: 11px; opacity: 0.9; }
.stock-cell .cell-pct { font-weight: 400; font-size: 9px; opacity: 0.7; }
</style>
</head>
<body class="min-h-screen">

<!-- Header -->
<div class="border-b border-slate-700 px-6 py-4 flex items-center justify-between">
  <div>
    <h1 class="text-xl font-bold text-white">Investment Manager Holdings</h1>
    <p class="text-sm text-slate-400 mt-1">SEC 13F &amp; N-PORT Filing Fetcher</p>
  </div>
  <div class="flex items-center gap-3">
    <span id="statusBadge" class="text-sm text-slate-400">Ready</span>
    <button id="downloadAllBtn" class="btn-secondary hidden" onclick="downloadAll()">
      &#128230; Download All
    </button>
    <button id="runBtn" class="btn-primary" onclick="startRun()">
      &#9654; Fetch Holdings
    </button>
  </div>
</div>

<!-- Main content -->
<div class="flex" style="height: calc(100vh - 73px);">

  <!-- Left panel: Managers + Results -->
  <div class="flex-1 overflow-y-auto p-6">
    <div class="card p-5 mb-5">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-white">Managers</h2>
        <span id="managerCount" class="text-sm text-slate-400"></span>
      </div>
      <table id="managerTable">
        <thead>
          <tr><th>Name</th><th>CIK</th><th></th></tr>
        </thead>
        <tbody id="managerBody"></tbody>
      </table>
    </div>

    <!-- Add Manager Form -->
    <div class="card p-5 mb-5">
      <h3 class="text-sm font-semibold text-slate-300 mb-3">Add Manager</h3>
      <div class="grid grid-cols-3 gap-3">
        <div class="relative" id="nameFieldWrap">
          <input id="addName" class="input-field" placeholder="Start typing manager/firm name..." autocomplete="off"
                 oninput="onNameInput()" onkeydown="onNameKeydown(event)" onfocus="onNameInput()">
          <div id="typeaheadDropdown" class="typeahead-dropdown"></div>
        </div>
        <input id="addCik" class="input-field" placeholder="CIK (auto-filled)">
        <button class="btn-primary" onclick="addManager()">Add</button>
      </div>
    </div>

    <!-- Results area -->
    <div id="resultsArea" class="hidden">
      <div class="card p-5 mb-5">
        <div class="flex items-center justify-between mb-3">
          <h2 class="text-lg font-semibold text-white">Results</h2>
          <div id="resultsSummary"></div>
        </div>
        <div id="csvLinks" class="flex flex-wrap gap-2 mb-4"></div>
      </div>
      <!-- Treemap -->
      <div id="treemapArea" class="card p-5 mb-5">
        <h2 class="text-lg font-semibold text-white mb-4">Portfolio Treemap</h2>
        <div id="treemapContainer" class="treemap-container"></div>
      </div>
    </div>
  </div>

  <!-- Right panel: Settings + Log -->
  <div class="w-96 border-l border-slate-700 overflow-y-auto p-6 flex flex-col gap-5">

    <!-- Settings -->
    <div class="card p-5">
      <h2 class="text-sm font-semibold text-slate-300 mb-3">Settings</h2>
      <div class="space-y-3">
        <div>
          <label class="text-xs text-slate-400">Top N Holdings</label>
          <input id="topN" type="number" class="input-field" min="1" max="100" value="20">
        </div>
        <div>
          <label class="text-xs text-slate-400">Max Filing Date</label>
          <input id="maxDate" type="date" class="input-field" value="2025-12-31">
        </div>
        <div>
          <label class="text-xs text-slate-400">SEC Identity</label>
          <input id="identity" class="input-field" value="">
        </div>
        <button class="btn-primary w-full mt-2" onclick="saveSettings()">Save Settings</button>
      </div>
    </div>

    <!-- Progress Log -->
    <div class="card p-5 flex-1 flex flex-col">
      <h2 class="text-sm font-semibold text-slate-300 mb-2">Progress</h2>
      <div class="progress-bar mb-2">
        <div id="progressFill" class="progress-fill" style="width: 0%"></div>
      </div>
      <div id="logArea" class="log-area flex-1">Waiting to start...</div>
    </div>
  </div>
</div>

<script>
let config = {};
let typeaheadTimer = null;
let typeaheadResults = [];
let typeaheadIndex = -1;

// ─── Config ──────────────────────────────────────────────────────────
async function loadConfig() {
  const res = await fetch('/api/config');
  config = await res.json();
  renderManagers();
  renderSettings();
}

function renderManagers() {
  const body = document.getElementById('managerBody');
  let html = '';
  let count = 0;

  for (const [name, cik] of Object.entries(config.managers_13f || {})) {
    count++;
    html += `<tr class="fade-in">
      <td class="font-medium text-white">${esc(name)}</td>
      <td class="text-slate-400 font-mono text-sm">${esc(cik)}</td>
      <td><button class="btn-danger" onclick="deleteManager('${escAttr(name)}','13f')">Remove</button></td>
    </tr>`;
  }

  body.innerHTML = html;
  document.getElementById('managerCount').textContent = count + ' managers';
}

function renderSettings() {
  document.getElementById('topN').value = config.top_n || 20;
  document.getElementById('maxDate').value = config.max_date || '2025-12-31';
  document.getElementById('identity').value = config.identity || '';
}

async function saveSettings() {
  config.top_n = parseInt(document.getElementById('topN').value) || 20;
  config.max_date = document.getElementById('maxDate').value;
  config.identity = document.getElementById('identity').value;
  await fetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(config),
  });
  showToast('Settings saved');
}

// ─── Typeahead ───────────────────────────────────────────────────────
function onNameInput() {
  const q = document.getElementById('addName').value.trim();
  if (q.length < 2) {
    hideTypeahead();
    return;
  }
  clearTimeout(typeaheadTimer);
  typeaheadTimer = setTimeout(() => fetchTypeahead(q), 250);
}

async function fetchTypeahead(q) {
  try {
    const res = await fetch('/api/search-company?q=' + encodeURIComponent(q));
    typeaheadResults = await res.json();
    typeaheadIndex = -1;
    renderTypeahead();
  } catch (e) {
    hideTypeahead();
  }
}

function renderTypeahead() {
  const dd = document.getElementById('typeaheadDropdown');
  if (!typeaheadResults.length) { hideTypeahead(); return; }
  dd.innerHTML = typeaheadResults.map((r, i) =>
    `<div class="typeahead-item${i === typeaheadIndex ? ' selected' : ''}" onmousedown="selectTypeahead(${i})">
      <div class="ta-name">${esc(r.name)}</div>
      <div class="ta-meta">CIK: ${esc(r.cik)}</div>
    </div>`
  ).join('');
  dd.classList.add('active');
}

function hideTypeahead() {
  document.getElementById('typeaheadDropdown').classList.remove('active');
  typeaheadResults = [];
  typeaheadIndex = -1;
}

function selectTypeahead(idx) {
  const item = typeaheadResults[idx];
  if (!item) return;
  document.getElementById('addName').value = item.name;
  document.getElementById('addCik').value = item.cik;
  hideTypeahead();
}

function onNameKeydown(e) {
  const dd = document.getElementById('typeaheadDropdown');
  if (!dd.classList.contains('active')) return;
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    typeaheadIndex = Math.min(typeaheadIndex + 1, typeaheadResults.length - 1);
    renderTypeahead();
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    typeaheadIndex = Math.max(typeaheadIndex - 1, 0);
    renderTypeahead();
  } else if (e.key === 'Enter' && typeaheadIndex >= 0) {
    e.preventDefault();
    selectTypeahead(typeaheadIndex);
  } else if (e.key === 'Escape') {
    hideTypeahead();
  }
}

// Hide typeahead when clicking outside
document.addEventListener('click', function(e) {
  if (!document.getElementById('nameFieldWrap').contains(e.target)) {
    hideTypeahead();
  }
});

// ─── Manager CRUD ────────────────────────────────────────────────────
async function addManager() {
  const name = document.getElementById('addName').value.trim();
  const cik = document.getElementById('addCik').value.trim();

  if (!name || !cik) { alert('Name and CIK are required'); return; }

  const res = await fetch('/api/managers', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name, cik, type: '13f'}),
  });

  if (res.ok) {
    document.getElementById('addName').value = '';
    document.getElementById('addCik').value = '';
    await loadConfig();
    showToast('Manager added');
  } else {
    const err = await res.json();
    alert(err.error);
  }
}

async function deleteManager(name, type) {
  if (!confirm('Remove ' + name + '?')) return;
  await fetch('/api/managers', {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name, type}),
  });
  await loadConfig();
  showToast('Manager removed');
}

// ─── Fetch run ───────────────────────────────────────────────────────
async function startRun() {
  await saveSettings();

  const btn = document.getElementById('runBtn');
  btn.disabled = true;
  btn.innerHTML = '&#9203; Running...';
  document.getElementById('statusBadge').textContent = 'Fetching...';
  document.getElementById('progressFill').style.width = '0%';
  document.getElementById('logArea').textContent = '';
  document.getElementById('resultsArea').classList.add('hidden');
  document.getElementById('downloadAllBtn').classList.add('hidden');

  const res = await fetch('/api/run', {method: 'POST'});
  if (!res.ok) {
    const err = await res.json();
    alert(err.error);
    btn.disabled = false;
    btn.innerHTML = '&#9654; Fetch Holdings';
    return;
  }

  const {run_id} = await res.json();
  const evtSource = new EventSource('/api/stream/' + run_id);
  const logEl = document.getElementById('logArea');

  evtSource.onmessage = function(e) {
    const msg = JSON.parse(e.data);

    if (msg.type === 'log') {
      logEl.textContent += msg.message + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }
    else if (msg.type === 'progress') {
      const pct = Math.round((msg.done / msg.total) * 100);
      document.getElementById('progressFill').style.width = pct + '%';
      document.getElementById('statusBadge').textContent = msg.done + '/' + msg.total + ' managers';
    }
    else if (msg.type === 'complete') {
      evtSource.close();
      btn.disabled = false;
      btn.innerHTML = '&#9654; Fetch Holdings';
      document.getElementById('statusBadge').textContent = 'Complete';
      document.getElementById('progressFill').style.width = '100%';
      showResults(msg.results);
      loadTreemap();
    }
  };

  evtSource.onerror = function() {
    evtSource.close();
    btn.disabled = false;
    btn.innerHTML = '&#9654; Fetch Holdings';
    document.getElementById('statusBadge').textContent = 'Error';
  };
}

// ─── Results + Download ──────────────────────────────────────────────
function showResults(results) {
  const area = document.getElementById('resultsArea');
  area.classList.remove('hidden');

  const success = results.managers.filter(m => m.status === 'success').length;
  const errors = results.managers.filter(m => m.status === 'error').length;
  document.getElementById('resultsSummary').innerHTML =
    `<span class="text-green-400 font-semibold">${success} succeeded</span>` +
    (errors ? ` &nbsp; <span class="text-red-400 font-semibold">${errors} failed</span>` : '');

  const csvDiv = document.getElementById('csvLinks');
  csvDiv.innerHTML = (results.csvs || []).map(f =>
    `<a href="/files/${f}" download class="text-blue-400 hover:text-blue-300 text-sm underline">&#128196; ${f}</a>`
  ).join('');

  // Show download all button
  if ((results.csvs || []).length > 0) {
    document.getElementById('downloadAllBtn').classList.remove('hidden');
  }
}

function downloadAll() {
  window.location.href = '/api/download-all';
}

// ─── Treemap ─────────────────────────────────────────────────────────
const TREEMAP_COLORS = [
  '#3b82f6','#8b5cf6','#ec4899','#f59e0b','#10b981',
  '#ef4444','#06b6d4','#84cc16','#f97316','#6366f1',
  '#14b8a6','#e879f9','#22d3ee','#a3e635',
];

async function loadTreemap() {
  try {
    const res = await fetch('/api/treemap-data');
    const data = await res.json();
    renderTreemap(data.managers || []);
  } catch (e) {
    document.getElementById('treemapContainer').innerHTML =
      '<p class="text-slate-400">Could not load treemap data.</p>';
  }
}

function renderTreemap(managers) {
  const container = document.getElementById('treemapContainer');
  if (!managers.length) {
    container.innerHTML = '<p class="text-slate-400">No data yet. Run a fetch first.</p>';
    return;
  }

  // Set grid columns based on count
  const cols = managers.length <= 4 ? 2 : managers.length <= 9 ? 3 : 4;
  container.style.gridTemplateColumns = `repeat(${cols}, 1fr)`;

  container.innerHTML = managers.map((mgr, mi) => {
    const color = TREEMAP_COLORS[mi % TREEMAP_COLORS.length];
    const stocksHtml = buildStockCells(mgr.stocks, color);
    return `<div class="manager-box fade-in">
      <div class="manager-box-header" style="border-left: 4px solid ${color}">${esc(mgr.manager)}</div>
      <div class="stock-grid">${stocksHtml}</div>
    </div>`;
  }).join('');
}

function buildStockCells(stocks, baseColor) {
  // stocks sorted by pct descending
  const sorted = [...stocks].sort((a, b) => b.pct - a.pct);
  const totalPct = sorted.reduce((s, st) => s + st.pct, 0);
  if (totalPct === 0) return '<span class="text-slate-500 text-xs p-2">No data</span>';

  // We'll make each cell proportional in area.  Use a container of ~300px wide x variable height.
  // Each stock gets a fraction of the total area.  We'll use flex-basis.
  const TOTAL_AREA = 400 * 220; // approximate pixel area for stock grid

  return sorted.map((st, i) => {
    const frac = st.pct / totalPct;
    const area = Math.max(frac * TOTAL_AREA, 900); // min area 30x30
    const side = Math.round(Math.sqrt(area));
    const w = Math.max(side, 36);
    const h = Math.max(Math.round(area / w), 26);

    // Vary color brightness per stock
    const hue = hexToHSL(baseColor);
    const lightness = Math.max(25, Math.min(55, hue.l + (i % 5) * 5 - 10));
    const bg = `hsl(${hue.h}, ${hue.s}%, ${lightness}%)`;

    const ticker = st.ticker !== 'N/A' ? st.ticker : '';
    const shortName = st.name.length > 14 ? st.name.substring(0, 12) + '…' : st.name;
    return `<div class="stock-cell" style="width:${w}px;height:${h}px;background:${bg}"
                 title="${esc(st.name)}${ticker ? ' (' + esc(ticker) + ')' : ''} — ${st.pct}%">
      <div class="cell-inner">
        <span class="cell-name">${esc(shortName)}</span>
        ${ticker ? '<span class="cell-ticker">' + esc(ticker) + '</span>' : ''}
        <span class="cell-pct">${st.pct}%</span>
      </div>
    </div>`;
  }).join('');
}

function hexToHSL(hex) {
  let r = parseInt(hex.slice(1,3),16)/255;
  let g = parseInt(hex.slice(3,5),16)/255;
  let b = parseInt(hex.slice(5,7),16)/255;
  const max = Math.max(r,g,b), min = Math.min(r,g,b);
  let h, s, l = (max+min)/2;
  if (max === min) { h = s = 0; }
  else {
    const d = max - min;
    s = l > 0.5 ? d/(2-max-min) : d/(max+min);
    switch(max) {
      case r: h = ((g-b)/d + (g<b?6:0))/6; break;
      case g: h = ((b-r)/d + 2)/6; break;
      case b: h = ((r-g)/d + 4)/6; break;
    }
  }
  return { h: Math.round(h*360), s: Math.round(s*100), l: Math.round(l*100) };
}

// ─── Utilities ───────────────────────────────────────────────────────
function showToast(msg) {
  const toast = document.createElement('div');
  toast.textContent = msg;
  toast.style.cssText = 'position:fixed;bottom:20px;right:20px;background:#22c55e;color:white;padding:10px 20px;border-radius:8px;font-weight:600;z-index:9999;animation:fadeIn 0.3s';
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 2000);
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function escAttr(s) {
  return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/"/g,'&quot;');
}

loadConfig();
</script>
</body>
</html>"""


@app.route("/")
def dashboard():
    return DASHBOARD_HTML


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
        print(f"Created default config: {CONFIG_PATH}")

    print("\n" + "=" * 50)
    print("  Investment Manager Holdings")
    print("  Open http://localhost:8080 in your browser")
    print("=" * 50 + "\n")

    app.run(host="0.0.0.0", port=8080, debug=False, threaded=True)
