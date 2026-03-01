"""
Financial Data Enrichment — yfinance wrapper (Two-Quarter)
==========================================================
Fetches two-quarter returns, P/E ratios, EPS beat data, forward metrics,
dividend yield, and sector/industry for stock tickers using yfinance.

Uses ThreadPoolExecutor for concurrent fetching and an in-memory cache
to avoid redundant API calls.

Install:  pip install yfinance python-dateutil
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import yfinance as yf

# ── In-memory cache ──────────────────────────────────────────────────────────

_cache = {}
_cache_lock = threading.Lock()

# ── Static sector/industry/country fallback for common stocks ────────────────
# Used when yfinance returns None for sector info (API flakiness).
# Format: ticker -> (sector, industry, country)

_SECTOR_FALLBACK = {
    # ── Technology ─────────────────────────────────────────────────────────
    "AAPL":  ("Technology", "Consumer Electronics", "United States"),
    "MSFT":  ("Technology", "Software—Infrastructure", "United States"),
    "NVDA":  ("Technology", "Semiconductors", "United States"),
    "AVGO":  ("Technology", "Semiconductors", "United States"),
    "CSCO":  ("Technology", "Communication Equipment", "United States"),
    "ORCL":  ("Technology", "Software—Infrastructure", "United States"),
    "ADBE":  ("Technology", "Software—Application", "United States"),
    "CRM":   ("Technology", "Software—Application", "United States"),
    "AMD":   ("Technology", "Semiconductors", "United States"),
    "INTC":  ("Technology", "Semiconductors", "United States"),
    "QCOM":  ("Technology", "Semiconductors", "United States"),
    "IBM":   ("Technology", "Information Technology Services", "United States"),
    "NOW":   ("Technology", "Software—Application", "United States"),
    "INTU":  ("Technology", "Software—Application", "United States"),
    "PANW":  ("Technology", "Software—Infrastructure", "United States"),
    "SNPS":  ("Technology", "Software—Application", "United States"),
    "CDNS":  ("Technology", "Software—Application", "United States"),
    "AMAT":  ("Technology", "Semiconductor Equipment & Materials", "United States"),
    "LRCX":  ("Technology", "Semiconductor Equipment & Materials", "United States"),
    "KLAC":  ("Technology", "Semiconductor Equipment & Materials", "United States"),
    "MU":    ("Technology", "Semiconductors", "United States"),
    "MRVL":  ("Technology", "Semiconductors", "United States"),
    "TXN":   ("Technology", "Semiconductors", "United States"),
    "SHOP":  ("Technology", "Software—Application", "Canada"),
    "TSM":   ("Technology", "Semiconductors", "Taiwan"),
    "ASML":  ("Technology", "Semiconductor Equipment & Materials", "Netherlands"),
    "SAP":   ("Technology", "Software—Application", "Germany"),
    "PLTR":  ("Technology", "Software—Application", "United States"),
    "CRWD":  ("Technology", "Software—Infrastructure", "United States"),
    "FTNT":  ("Technology", "Software—Infrastructure", "United States"),
    "WDAY":  ("Technology", "Software—Application", "United States"),
    "TEAM":  ("Technology", "Software—Application", "Australia"),
    "DDOG":  ("Technology", "Software—Application", "United States"),
    "ZS":    ("Technology", "Software—Infrastructure", "United States"),
    "NET":   ("Technology", "Software—Infrastructure", "United States"),
    "SNOW":  ("Technology", "Software—Application", "United States"),
    "HUBS":  ("Technology", "Software—Application", "United States"),
    "ADSK":  ("Technology", "Software—Application", "United States"),
    "ANSS":  ("Technology", "Software—Application", "United States"),
    "NXPI":  ("Technology", "Semiconductors", "Netherlands"),
    "ON":    ("Technology", "Semiconductors", "United States"),
    "MPWR":  ("Technology", "Semiconductors", "United States"),
    "SMCI":  ("Technology", "Computer Hardware", "United States"),
    "DELL":  ("Technology", "Computer Hardware", "United States"),
    "HPQ":   ("Technology", "Computer Hardware", "United States"),
    "HPE":   ("Technology", "Communication Equipment", "United States"),
    "KEYS":  ("Technology", "Scientific & Technical Instruments", "United States"),
    "MCHP":  ("Technology", "Semiconductors", "United States"),
    "SWKS":  ("Technology", "Semiconductors", "United States"),
    "GFS":   ("Technology", "Semiconductors", "United States"),
    "ARM":   ("Technology", "Semiconductors", "United Kingdom"),
    "UBER":  ("Technology", "Software—Application", "United States"),
    "ABNB":  ("Consumer Cyclical", "Travel Services", "United States"),
    "COIN":  ("Technology", "Software—Application", "United States"),
    "RKLB":  ("Industrials", "Aerospace & Defense", "United States"),
    "TWLO":  ("Technology", "Software—Application", "United States"),
    "MDB":   ("Technology", "Software—Application", "United States"),
    "DOCU":  ("Technology", "Software—Application", "United States"),
    "ZM":    ("Technology", "Software—Application", "United States"),
    "OKTA":  ("Technology", "Software—Infrastructure", "United States"),
    "TTD":   ("Technology", "Software—Application", "United States"),
    "VEEV":  ("Technology", "Software—Application", "United States"),
    "BILL":  ("Technology", "Software—Application", "United States"),
    "GDDY":  ("Technology", "Software—Infrastructure", "United States"),
    "GEN":   ("Technology", "Software—Infrastructure", "United States"),
    "EPAM":  ("Technology", "Information Technology Services", "United States"),
    "IT":    ("Technology", "Information Technology Services", "United States"),
    "ACN":   ("Technology", "Information Technology Services", "Ireland"),
    "CTSH":  ("Technology", "Information Technology Services", "United States"),
    "INFY":  ("Technology", "Information Technology Services", "India"),
    "WIT":   ("Technology", "Information Technology Services", "India"),
    # ── Communication Services ─────────────────────────────────────────────
    "GOOGL": ("Communication Services", "Internet Content & Information", "United States"),
    "GOOG":  ("Communication Services", "Internet Content & Information", "United States"),
    "META":  ("Communication Services", "Internet Content & Information", "United States"),
    "T":     ("Communication Services", "Telecom Services", "United States"),
    "VZ":    ("Communication Services", "Telecom Services", "United States"),
    "TMUS":  ("Communication Services", "Telecom Services", "United States"),
    "DIS":   ("Communication Services", "Entertainment", "United States"),
    "NFLX":  ("Communication Services", "Entertainment", "United States"),
    "CMCSA": ("Communication Services", "Entertainment", "United States"),
    "CHTR":  ("Communication Services", "Entertainment", "United States"),
    "EA":    ("Communication Services", "Electronic Gaming & Multimedia", "United States"),
    "TTWO":  ("Communication Services", "Electronic Gaming & Multimedia", "United States"),
    "RBLX":  ("Communication Services", "Electronic Gaming & Multimedia", "United States"),
    "WBD":   ("Communication Services", "Entertainment", "United States"),
    "PARA":  ("Communication Services", "Entertainment", "United States"),
    "FOX":   ("Communication Services", "Entertainment", "United States"),
    "FOXA":  ("Communication Services", "Entertainment", "United States"),
    "SPOT":  ("Communication Services", "Internet Content & Information", "Sweden"),
    "SNAP":  ("Communication Services", "Internet Content & Information", "United States"),
    "PINS":  ("Communication Services", "Internet Content & Information", "United States"),
    "MTCH":  ("Communication Services", "Internet Content & Information", "United States"),
    # ── Consumer Cyclical ──────────────────────────────────────────────────
    "AMZN":  ("Consumer Cyclical", "Internet Retail", "United States"),
    "TSLA":  ("Consumer Cyclical", "Auto Manufacturers", "United States"),
    "HD":    ("Consumer Cyclical", "Home Improvement Retail", "United States"),
    "MCD":   ("Consumer Cyclical", "Restaurants", "United States"),
    "SBUX":  ("Consumer Cyclical", "Restaurants", "United States"),
    "NKE":   ("Consumer Cyclical", "Footwear & Accessories", "United States"),
    "LOW":   ("Consumer Cyclical", "Home Improvement Retail", "United States"),
    "TJX":   ("Consumer Cyclical", "Apparel Retail", "United States"),
    "BKNG":  ("Consumer Cyclical", "Travel Services", "United States"),
    "F":     ("Consumer Cyclical", "Auto Manufacturers", "United States"),
    "GM":    ("Consumer Cyclical", "Auto Manufacturers", "United States"),
    "TM":    ("Consumer Cyclical", "Auto Manufacturers", "Japan"),
    "ROST":  ("Consumer Cyclical", "Apparel Retail", "United States"),
    "DHI":   ("Consumer Cyclical", "Residential Construction", "United States"),
    "LEN":   ("Consumer Cyclical", "Residential Construction", "United States"),
    "PHM":   ("Consumer Cyclical", "Residential Construction", "United States"),
    "NVR":   ("Consumer Cyclical", "Residential Construction", "United States"),
    "CMG":   ("Consumer Cyclical", "Restaurants", "United States"),
    "YUM":   ("Consumer Cyclical", "Restaurants", "United States"),
    "DPZ":   ("Consumer Cyclical", "Restaurants", "United States"),
    "ORLY":  ("Consumer Cyclical", "Specialty Retail", "United States"),
    "AZO":   ("Consumer Cyclical", "Specialty Retail", "United States"),
    "EBAY":  ("Consumer Cyclical", "Internet Retail", "United States"),
    "ETSY":  ("Consumer Cyclical", "Internet Retail", "United States"),
    "LULU":  ("Consumer Cyclical", "Apparel Retail", "Canada"),
    "RCL":   ("Consumer Cyclical", "Travel Services", "United States"),
    "MAR":   ("Consumer Cyclical", "Lodging", "United States"),
    "HLT":   ("Consumer Cyclical", "Lodging", "United States"),
    "EXPE":  ("Consumer Cyclical", "Travel Services", "United States"),
    "LVS":   ("Consumer Cyclical", "Resorts & Casinos", "United States"),
    "WYNN":  ("Consumer Cyclical", "Resorts & Casinos", "United States"),
    "APTV":  ("Consumer Cyclical", "Auto Parts", "Ireland"),
    "BWA":   ("Consumer Cyclical", "Auto Parts", "United States"),
    "GPC":   ("Consumer Cyclical", "Specialty Retail", "United States"),
    # ── Consumer Defensive ─────────────────────────────────────────────────
    "PG":    ("Consumer Defensive", "Household & Personal Products", "United States"),
    "COST":  ("Consumer Defensive", "Discount Stores", "United States"),
    "WMT":   ("Consumer Defensive", "Discount Stores", "United States"),
    "KO":    ("Consumer Defensive", "Beverages—Non-Alcoholic", "United States"),
    "PEP":   ("Consumer Defensive", "Beverages—Non-Alcoholic", "United States"),
    "PM":    ("Consumer Defensive", "Tobacco", "United States"),
    "MO":    ("Consumer Defensive", "Tobacco", "United States"),
    "BTI":   ("Consumer Defensive", "Tobacco", "United Kingdom"),
    "CL":    ("Consumer Defensive", "Household & Personal Products", "United States"),
    "EL":    ("Consumer Defensive", "Household & Personal Products", "United States"),
    "KMB":   ("Consumer Defensive", "Household & Personal Products", "United States"),
    "GIS":   ("Consumer Defensive", "Packaged Foods", "United States"),
    "K":     ("Consumer Defensive", "Packaged Foods", "United States"),
    "HSY":   ("Consumer Defensive", "Confectioners", "United States"),
    "MDLZ":  ("Consumer Defensive", "Confectioners", "United States"),
    "STZ":   ("Consumer Defensive", "Beverages—Brewers", "United States"),
    "DEO":   ("Consumer Defensive", "Beverages—Wineries & Distilleries", "United Kingdom"),
    "BUD":   ("Consumer Defensive", "Beverages—Brewers", "Belgium"),
    "ADM":   ("Consumer Defensive", "Farm Products", "United States"),
    "SYY":   ("Consumer Defensive", "Food Distribution", "United States"),
    "KDP":   ("Consumer Defensive", "Beverages—Non-Alcoholic", "United States"),
    "MNST":  ("Consumer Defensive", "Beverages—Non-Alcoholic", "United States"),
    "KR":    ("Consumer Defensive", "Grocery Stores", "United States"),
    "TGT":   ("Consumer Defensive", "Discount Stores", "United States"),
    "DG":    ("Consumer Defensive", "Discount Stores", "United States"),
    "DLTR":  ("Consumer Defensive", "Discount Stores", "United States"),
    "SJM":   ("Consumer Defensive", "Packaged Foods", "United States"),
    "CAG":   ("Consumer Defensive", "Packaged Foods", "United States"),
    "HRL":   ("Consumer Defensive", "Packaged Foods", "United States"),
    "CHD":   ("Consumer Defensive", "Household & Personal Products", "United States"),
    "CLX":   ("Consumer Defensive", "Household & Personal Products", "United States"),
    # ── Financial Services ─────────────────────────────────────────────────
    "BRK.B": ("Financial Services", "Insurance—Diversified", "United States"),
    "BRK-B": ("Financial Services", "Insurance—Diversified", "United States"),
    "JPM":   ("Financial Services", "Banks—Diversified", "United States"),
    "V":     ("Financial Services", "Credit Services", "United States"),
    "MA":    ("Financial Services", "Credit Services", "United States"),
    "GS":    ("Financial Services", "Capital Markets", "United States"),
    "MS":    ("Financial Services", "Capital Markets", "United States"),
    "BAC":   ("Financial Services", "Banks—Diversified", "United States"),
    "C":     ("Financial Services", "Banks—Diversified", "United States"),
    "WFC":   ("Financial Services", "Banks—Diversified", "United States"),
    "BLK":   ("Financial Services", "Asset Management", "United States"),
    "SCHW":  ("Financial Services", "Capital Markets", "United States"),
    "AXP":   ("Financial Services", "Credit Services", "United States"),
    "SPGI":  ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "BX":    ("Financial Services", "Asset Management", "United States"),
    "CB":    ("Financial Services", "Insurance—Property & Casualty", "United States"),
    "MMC":   ("Financial Services", "Insurance Brokers", "United States"),
    "PGR":   ("Financial Services", "Insurance—Property & Casualty", "United States"),
    "AIG":   ("Financial Services", "Insurance—Diversified", "United States"),
    "MET":   ("Financial Services", "Insurance—Life", "United States"),
    "PRU":   ("Financial Services", "Insurance—Life", "United States"),
    "AFL":   ("Financial Services", "Insurance—Life", "United States"),
    "ALL":   ("Financial Services", "Insurance—Property & Casualty", "United States"),
    "TRV":   ("Financial Services", "Insurance—Property & Casualty", "United States"),
    "HIG":   ("Financial Services", "Insurance—Diversified", "United States"),
    "AON":   ("Financial Services", "Insurance Brokers", "Ireland"),
    "ICE":   ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "CME":   ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "MCO":   ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "MSCI":  ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "USB":   ("Financial Services", "Banks—Regional", "United States"),
    "PNC":   ("Financial Services", "Banks—Regional", "United States"),
    "TFC":   ("Financial Services", "Banks—Regional", "United States"),
    "COF":   ("Financial Services", "Credit Services", "United States"),
    "DFS":   ("Financial Services", "Credit Services", "United States"),
    "SYF":   ("Financial Services", "Credit Services", "United States"),
    "FITB":  ("Financial Services", "Banks—Regional", "United States"),
    "MTB":   ("Financial Services", "Banks—Regional", "United States"),
    "HBAN":  ("Financial Services", "Banks—Regional", "United States"),
    "RF":    ("Financial Services", "Banks—Regional", "United States"),
    "CFG":   ("Financial Services", "Banks—Regional", "United States"),
    "KEY":   ("Financial Services", "Banks—Regional", "United States"),
    "KKR":   ("Financial Services", "Asset Management", "United States"),
    "APO":   ("Financial Services", "Asset Management", "United States"),
    "ARES":  ("Financial Services", "Asset Management", "United States"),
    "OWL":   ("Financial Services", "Asset Management", "United States"),
    "RJF":   ("Financial Services", "Capital Markets", "United States"),
    "IBKR":  ("Financial Services", "Capital Markets", "United States"),
    "NDAQ":  ("Financial Services", "Financial Data & Stock Exchanges", "United States"),
    "FIS":   ("Financial Services", "Information Technology Services", "United States"),
    "FISV":  ("Financial Services", "Information Technology Services", "United States"),
    "GPN":   ("Financial Services", "Information Technology Services", "United States"),
    "PYPL":  ("Financial Services", "Credit Services", "United States"),
    "SQ":    ("Financial Services", "Credit Services", "United States"),
    "TROW":  ("Financial Services", "Asset Management", "United States"),
    "BEN":   ("Financial Services", "Asset Management", "United States"),
    "IVZ":   ("Financial Services", "Asset Management", "United States"),
    "HSBC":  ("Financial Services", "Banks—Diversified", "United Kingdom"),
    "IBN":   ("Financial Services", "Banks—Regional", "India"),
    # ── Healthcare ─────────────────────────────────────────────────────────
    "UNH":   ("Healthcare", "Healthcare Plans", "United States"),
    "JNJ":   ("Healthcare", "Drug Manufacturers—General", "United States"),
    "ABBV":  ("Healthcare", "Drug Manufacturers—General", "United States"),
    "LLY":   ("Healthcare", "Drug Manufacturers—General", "United States"),
    "MRK":   ("Healthcare", "Drug Manufacturers—General", "United States"),
    "PFE":   ("Healthcare", "Drug Manufacturers—General", "United States"),
    "TMO":   ("Healthcare", "Diagnostics & Research", "United States"),
    "ABT":   ("Healthcare", "Medical Devices", "United States"),
    "AMGN":  ("Healthcare", "Drug Manufacturers—General", "United States"),
    "GILD":  ("Healthcare", "Drug Manufacturers—General", "United States"),
    "BMY":   ("Healthcare", "Drug Manufacturers—General", "United States"),
    "MDT":   ("Healthcare", "Medical Devices", "United States"),
    "ISRG":  ("Healthcare", "Medical Instruments & Supplies", "United States"),
    "SYK":   ("Healthcare", "Medical Devices", "United States"),
    "CI":    ("Healthcare", "Healthcare Plans", "United States"),
    "ELV":   ("Healthcare", "Healthcare Plans", "United States"),
    "NVO":   ("Healthcare", "Drug Manufacturers—General", "Denmark"),
    "AZN":   ("Healthcare", "Drug Manufacturers—General", "United Kingdom"),
    "BSX":   ("Healthcare", "Medical Devices", "United States"),
    "BDX":   ("Healthcare", "Medical Instruments & Supplies", "United States"),
    "EW":    ("Healthcare", "Medical Devices", "United States"),
    "ZTS":   ("Healthcare", "Drug Manufacturers—Specialty & Generic", "United States"),
    "VRTX":  ("Healthcare", "Biotechnology", "United States"),
    "REGN":  ("Healthcare", "Biotechnology", "United States"),
    "MRNA":  ("Healthcare", "Biotechnology", "United States"),
    "BIIB":  ("Healthcare", "Drug Manufacturers—General", "United States"),
    "IQV":   ("Healthcare", "Diagnostics & Research", "United States"),
    "DHR":   ("Healthcare", "Diagnostics & Research", "United States"),
    "A":     ("Healthcare", "Diagnostics & Research", "United States"),
    "DXCM":  ("Healthcare", "Medical Devices", "United States"),
    "BAX":   ("Healthcare", "Medical Instruments & Supplies", "United States"),
    "HCA":   ("Healthcare", "Medical Care Facilities", "United States"),
    "HUM":   ("Healthcare", "Healthcare Plans", "United States"),
    "CNC":   ("Healthcare", "Healthcare Plans", "United States"),
    "MOH":   ("Healthcare", "Healthcare Plans", "United States"),
    "CVS":   ("Healthcare", "Healthcare Plans", "United States"),
    "MCK":   ("Healthcare", "Medical Distribution", "United States"),
    "CAH":   ("Healthcare", "Medical Distribution", "United States"),
    "ABC":   ("Healthcare", "Medical Distribution", "United States"),
    "GEHC":  ("Healthcare", "Medical Devices", "United States"),
    "ALNY":  ("Healthcare", "Biotechnology", "United States"),
    "ILMN":  ("Healthcare", "Diagnostics & Research", "United States"),
    "IDXX":  ("Healthcare", "Diagnostics & Research", "United States"),
    "GSK":   ("Healthcare", "Drug Manufacturers—General", "United Kingdom"),
    "SNY":   ("Healthcare", "Drug Manufacturers—General", "France"),
    # ── Energy ─────────────────────────────────────────────────────────────
    "XOM":   ("Energy", "Oil & Gas Integrated", "United States"),
    "CVX":   ("Energy", "Oil & Gas Integrated", "United States"),
    "COP":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "SLB":   ("Energy", "Oil & Gas Equipment & Services", "United States"),
    "EOG":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "PSX":   ("Energy", "Oil & Gas Refining & Marketing", "United States"),
    "VLO":   ("Energy", "Oil & Gas Refining & Marketing", "United States"),
    "MPC":   ("Energy", "Oil & Gas Refining & Marketing", "United States"),
    "SHEL":  ("Energy", "Oil & Gas Integrated", "United Kingdom"),
    "TTE":   ("Energy", "Oil & Gas Integrated", "France"),
    "BP":    ("Energy", "Oil & Gas Integrated", "United Kingdom"),
    "PXD":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "DVN":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "OXY":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "WMB":   ("Energy", "Oil & Gas Midstream", "United States"),
    "KMI":   ("Energy", "Oil & Gas Midstream", "United States"),
    "OKE":   ("Energy", "Oil & Gas Midstream", "United States"),
    "ET":    ("Energy", "Oil & Gas Midstream", "United States"),
    "EPD":   ("Energy", "Oil & Gas Midstream", "United States"),
    "ENB":   ("Energy", "Oil & Gas Midstream", "Canada"),
    "HES":   ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "FANG":  ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "HAL":   ("Energy", "Oil & Gas Equipment & Services", "United States"),
    "BKR":   ("Energy", "Oil & Gas Equipment & Services", "United States"),
    "CTRA":  ("Energy", "Oil & Gas Exploration & Production", "United States"),
    "TRP":   ("Energy", "Oil & Gas Midstream", "Canada"),
    "SU":    ("Energy", "Oil & Gas Integrated", "Canada"),
    "CNQ":   ("Energy", "Oil & Gas Exploration & Production", "Canada"),
    "E":     ("Energy", "Oil & Gas Integrated", "Italy"),
    "EQNR":  ("Energy", "Oil & Gas Integrated", "Norway"),
    # ── Industrials ────────────────────────────────────────────────────────
    "BA":    ("Industrials", "Aerospace & Defense", "United States"),
    "RTX":   ("Industrials", "Aerospace & Defense", "United States"),
    "LMT":   ("Industrials", "Aerospace & Defense", "United States"),
    "GE":    ("Industrials", "Aerospace & Defense", "United States"),
    "CAT":   ("Industrials", "Farm & Heavy Construction Machinery", "United States"),
    "HON":   ("Industrials", "Conglomerates", "United States"),
    "UNP":   ("Industrials", "Railroads", "United States"),
    "UPS":   ("Industrials", "Integrated Freight & Logistics", "United States"),
    "DE":    ("Industrials", "Farm & Heavy Construction Machinery", "United States"),
    "MMM":   ("Industrials", "Conglomerates", "United States"),
    "ETN":   ("Industrials", "Specialty Industrial Machinery", "Ireland"),
    "EMR":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "ITW":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "FDX":   ("Industrials", "Integrated Freight & Logistics", "United States"),
    "WM":    ("Industrials", "Waste Management", "United States"),
    "RSG":   ("Industrials", "Waste Management", "United States"),
    "GD":    ("Industrials", "Aerospace & Defense", "United States"),
    "NOC":   ("Industrials", "Aerospace & Defense", "United States"),
    "GEV":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "TDG":   ("Industrials", "Aerospace & Defense", "United States"),
    "HWM":   ("Industrials", "Aerospace & Defense", "United States"),
    "CSX":   ("Industrials", "Railroads", "United States"),
    "NSC":   ("Industrials", "Railroads", "United States"),
    "CPRT":  ("Industrials", "Specialty Business Services", "United States"),
    "CTAS":  ("Industrials", "Specialty Business Services", "United States"),
    "FAST":  ("Industrials", "Building Products & Equipment", "United States"),
    "VRSK":  ("Industrials", "Consulting Services", "United States"),
    "ROK":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "AME":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "DOV":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "PCAR":  ("Industrials", "Farm & Heavy Construction Machinery", "United States"),
    "PWR":   ("Industrials", "Engineering & Construction", "United States"),
    "WAB":   ("Industrials", "Railroads", "United States"),
    "TT":    ("Industrials", "Building Products & Equipment", "Ireland"),
    "CARR":  ("Industrials", "Building Products & Equipment", "United States"),
    "IR":    ("Industrials", "Specialty Industrial Machinery", "United States"),
    "XYL":   ("Industrials", "Specialty Industrial Machinery", "United States"),
    "SWK":   ("Industrials", "Tools & Accessories", "United States"),
    "GWW":   ("Industrials", "Industrial Distribution", "United States"),
    "ODFL":  ("Industrials", "Trucking", "United States"),
    "DAL":   ("Industrials", "Airlines", "United States"),
    "UAL":   ("Industrials", "Airlines", "United States"),
    "LUV":   ("Industrials", "Airlines", "United States"),
    "CNI":   ("Industrials", "Railroads", "Canada"),
    "CP":    ("Industrials", "Railroads", "Canada"),
    # ── Utilities ──────────────────────────────────────────────────────────
    "NEE":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "DUK":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "SO":    ("Utilities", "Utilities—Regulated Electric", "United States"),
    "AEP":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "D":     ("Utilities", "Utilities—Regulated Electric", "United States"),
    "SRE":   ("Utilities", "Utilities—Diversified", "United States"),
    "EXC":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "XEL":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "ED":    ("Utilities", "Utilities—Regulated Electric", "United States"),
    "WEC":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "PCG":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "EIX":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "AWK":   ("Utilities", "Utilities—Regulated Water", "United States"),
    "ES":    ("Utilities", "Utilities—Regulated Electric", "United States"),
    "ETR":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "PPL":   ("Utilities", "Utilities—Regulated Electric", "United States"),
    "FE":    ("Utilities", "Utilities—Regulated Electric", "United States"),
    "CEG":   ("Utilities", "Utilities—Independent Power Producers", "United States"),
    "VST":   ("Utilities", "Utilities—Independent Power Producers", "United States"),
    # ── Real Estate ────────────────────────────────────────────────────────
    "PLD":   ("Real Estate", "REIT—Industrial", "United States"),
    "AMT":   ("Real Estate", "REIT—Specialty", "United States"),
    "CCI":   ("Real Estate", "REIT—Specialty", "United States"),
    "SPG":   ("Real Estate", "REIT—Retail", "United States"),
    "O":     ("Real Estate", "REIT—Retail", "United States"),
    "EQIX":  ("Real Estate", "REIT—Specialty", "United States"),
    "PSA":   ("Real Estate", "REIT—Specialty", "United States"),
    "DLR":   ("Real Estate", "REIT—Specialty", "United States"),
    "WELL":  ("Real Estate", "REIT—Healthcare Facilities", "United States"),
    "VICI":  ("Real Estate", "REIT—Specialty", "United States"),
    "SBAC":  ("Real Estate", "REIT—Specialty", "United States"),
    "CBRE":  ("Real Estate", "Real Estate Services", "United States"),
    "AVB":   ("Real Estate", "REIT—Residential", "United States"),
    "EQR":   ("Real Estate", "REIT—Residential", "United States"),
    "ARE":   ("Real Estate", "REIT—Office", "United States"),
    "MAA":   ("Real Estate", "REIT—Residential", "United States"),
    "IRM":   ("Real Estate", "REIT—Specialty", "United States"),
    # ── Basic Materials ────────────────────────────────────────────────────
    "LIN":   ("Basic Materials", "Specialty Chemicals", "United Kingdom"),
    "APD":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "SHW":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "RIO":   ("Basic Materials", "Other Industrial Metals & Mining", "United Kingdom"),
    "BHP":   ("Basic Materials", "Other Industrial Metals & Mining", "Australia"),
    "VALE":  ("Basic Materials", "Other Industrial Metals & Mining", "Brazil"),
    "NEM":   ("Basic Materials", "Gold", "United States"),
    "GOLD":  ("Basic Materials", "Gold", "Canada"),
    "FCX":   ("Basic Materials", "Copper", "United States"),
    "ECL":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "DD":    ("Basic Materials", "Specialty Chemicals", "United States"),
    "PPG":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "NUE":   ("Basic Materials", "Steel", "United States"),
    "DOW":   ("Basic Materials", "Chemicals", "United States"),
    "CTVA":  ("Basic Materials", "Agricultural Inputs", "United States"),
    "VMC":   ("Basic Materials", "Building Materials", "United States"),
    "MLM":   ("Basic Materials", "Building Materials", "United States"),
    "ALB":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "IFF":   ("Basic Materials", "Specialty Chemicals", "United States"),
    "CE":    ("Basic Materials", "Chemicals", "United States"),
    "SCCO":  ("Basic Materials", "Copper", "United States"),
    "TECK":  ("Basic Materials", "Other Industrial Metals & Mining", "Canada"),
}

# ── Ticker aliases — maps wrong/old tickers to correct ones ───────────────
# Some CUSIP→ticker resolvers return outdated or variant tickers.
_TICKER_ALIASES = {
    "FB":    "META",
    "ALTM":  "MO",     # Altus Midstream vs Altria confusion
    "ANTM":  "ELV",    # Anthem → Elevance Health
    "TWTR":  "X",      # Twitter (delisted, but may appear in old data)
    "LUMN":  "LUMN",
    "DISCA": "WBD",
    "DISCK": "WBD",
    "VIAC":  "PARA",
    "VIACA": "PARA",
    "KSU":   "CP",     # Kansas City Southern → Canadian Pacific
    "XLNX":  "AMD",    # Xilinx acquired by AMD
    "ATVI":  "MSFT",   # Activision acquired by Microsoft
    "CERN":  "ORCL",   # Cerner acquired by Oracle
    "CTXS":  "CLOUD",  # Citrix taken private
    "BRK.A": "BRK.B",  # Normalize class A → B for sector lookup
    "BRK/A": "BRK.B",
    "BRK/B": "BRK.B",
    "BRK A": "BRK.B",
    "BRK B": "BRK.B",
    "BF.B":  "BF.B",   # Brown-Forman
    "BF-B":  "BF.B",
    "GEV":   "GEV",    # GE Vernova
    "GEHC":  "GEHC",   # GE HealthCare
}

# ── Company name → ticker map for N/A tickers ────────────────────────────────
# Maps common NPORT/13F holding names to tickers for sector fallback.

_NAME_TO_TICKER = {
    "PHILIP MORRIS INTERNATIONAL": "PM",
    "PHILIP MORRIS INTERNATIONAL INC": "PM",
    "PHILIP MORRIS INTERNATIONAL INC.": "PM",
    "META PLATFORMS INC": "META",
    "META PLATFORMS": "META",
    "ALPHABET INC": "GOOGL",
    "MICROSOFT CORP": "MSFT",
    "MICROSOFT CORPORATION": "MSFT",
    "APPLE INC": "AAPL",
    "AMAZON.COM INC": "AMZN",
    "AMAZON COM INC": "AMZN",
    "NVIDIA CORP": "NVDA",
    "NVIDIA CORPORATION": "NVDA",
    "BROADCOM INC": "AVGO",
    "TESLA INC": "TSLA",
    "BERKSHIRE HATHAWAY": "BRK.B",
    "JPMORGAN CHASE": "JPM",
    "JPMORGAN CHASE & CO": "JPM",
    "UNITEDHEALTH GROUP": "UNH",
    "UNITEDHEALTH GROUP INC": "UNH",
    "JOHNSON & JOHNSON": "JNJ",
    "EXXON MOBIL CORP": "XOM",
    "EXXON MOBIL CORPORATION": "XOM",
    "PROCTER & GAMBLE": "PG",
    "PROCTER & GAMBLE CO": "PG",
    "HOME DEPOT INC": "HD",
    "VISA INC": "V",
    "MASTERCARD INC": "MA",
    "CHEVRON CORP": "CVX",
    "COCA-COLA CO": "KO",
    "PEPSICO INC": "PEP",
    "COSTCO WHOLESALE CORP": "COST",
    "WALMART INC": "WMT",
    "ABBOTT LABORATORIES": "ABT",
    "ABBVIE INC": "ABBV",
    "ELI LILLY & CO": "LLY",
    "ELI LILLY AND CO": "LLY",
    "MERCK & CO INC": "MRK",
    "PFIZER INC": "PFE",
    "THERMO FISHER SCIENTIFIC": "TMO",
    "THERMO FISHER SCIENTIFIC INC": "TMO",
    "CISCO SYSTEMS INC": "CSCO",
    "ORACLE CORP": "ORCL",
    "ORACLE CORPORATION": "ORCL",
    "ADOBE INC": "ADBE",
    "SALESFORCE INC": "CRM",
    "ADVANCED MICRO DEVICES": "AMD",
    "INTEL CORP": "INTC",
    "QUALCOMM INC": "QCOM",
    "INTERNATIONAL BUSINESS MACHINES": "IBM",
    "AT&T INC": "T",
    "AT&T INC.": "T",
    "VERIZON COMMUNICATIONS INC": "VZ",
    "VERIZON COMMUNICATIONS INC.": "VZ",
    "T-MOBILE US INC": "TMUS",
    "WALT DISNEY CO": "DIS",
    "NETFLIX INC": "NFLX",
    "COMCAST CORP": "CMCSA",
    "BOEING CO": "BA",
    "CATERPILLAR INC": "CAT",
    "HONEYWELL INTERNATIONAL": "HON",
    "UNION PACIFIC CORP": "UNP",
    "GENERAL ELECTRIC CO": "GE",
    "GOLDMAN SACHS GROUP INC": "GS",
    "MORGAN STANLEY": "MS",
    "BANK OF AMERICA CORP": "BAC",
    "CITIGROUP INC": "C",
    "WELLS FARGO & CO": "WFC",
    "BLACKROCK INC": "BLK",
    "CHARLES SCHWAB CORP": "SCHW",
    "AMERICAN EXPRESS CO": "AXP",
    "TAIWAN SEMICONDUCTOR MFG": "TSM",
    "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSM",
    "ASML HOLDING NV": "ASML",
    "NOVO NORDISK": "NVO",
    "NOVO-NORDISK A/S": "NVO",
    "ASTRAZENECA PLC": "AZN",
    "SHELL PLC": "SHEL",
    "BP PLC": "BP",
    "TOTALENERGIES SE": "TTE",
    "RIO TINTO PLC": "RIO",
    "BHP GROUP LTD": "BHP",
    "VALE SA": "VALE",
    "TOYOTA MOTOR CORP": "TM",
    "MCDONALD'S CORP": "MCD",
    "MCDONALDS CORP": "MCD",
    "STARBUCKS CORP": "SBUX",
    "NIKE INC": "NKE",
    "LOCKHEED MARTIN CORP": "LMT",
    "RAYTHEON TECHNOLOGIES": "RTX",
    "RTX CORP": "RTX",
    "NEXTERA ENERGY INC": "NEE",
    "DUKE ENERGY CORP": "DUK",
    "SOUTHERN CO": "SO",
    "BRITISH AMERICAN TOBACCO": "BTI",
    "ALTRIA GROUP INC": "MO",
    "LINDE PLC": "LIN",
    "CONOCOPHILLIPS": "COP",
    "SCHLUMBERGER LTD": "SLB",
    "SLB": "SLB",
    "FREEPORT-MCMORAN INC": "FCX",
    "DEERE & CO": "DE",
    "S&P GLOBAL INC": "SPGI",
    "PROLOGIS INC": "PLD",
    "AMERICAN TOWER CORP": "AMT",
    "BOOKING HOLDINGS INC": "BKNG",
    "SERVICENOW INC": "NOW",
    "INTUIT INC": "INTU",
    "APPLIED MATERIALS INC": "AMAT",
    "LAM RESEARCH CORP": "LRCX",
    "TEXAS INSTRUMENTS INC": "TXN",
    "MICRON TECHNOLOGY INC": "MU",
    # More names commonly seen in NPORT filings
    "PROGRESSIVE CORP": "PGR",
    "PROGRESSIVE CORPORATION": "PGR",
    "CIGNA GROUP": "CI",
    "CIGNA CORP": "CI",
    "CIGNA": "CI",
    "ENBRIDGE INC": "ENB",
    "ENBRIDGE": "ENB",
    "AMERICAN INTERNATIONAL GROUP": "AIG",
    "AMERICAN INTL GROUP INC": "AIG",
    "CHUBB LTD": "CB",
    "CHUBB LIMITED": "CB",
    "XCEL ENERGY INC": "XEL",
    "XCEL ENERGY": "XEL",
    "ICICI BANK LTD": "IBN",
    "ICICI BANK": "IBN",
    "MARSH & MCLENNAN": "MMC",
    "MARSH & MCLENNAN COS": "MMC",
    "S&P GLOBAL": "SPGI",
    "INTERCONTINENTAL EXCHANGE": "ICE",
    "CME GROUP INC": "CME",
    "MOODY'S CORP": "MCO",
    "MOODYS CORP": "MCO",
    "METLIFE INC": "MET",
    "PRUDENTIAL FINANCIAL": "PRU",
    "AFLAC INC": "AFL",
    "ALLSTATE CORP": "ALL",
    "TRAVELERS COS INC": "TRV",
    "AON PLC": "AON",
    "HSBC HOLDINGS PLC": "HSBC",
    "CANADIAN NATIONAL RAILWAY": "CNI",
    "CANADIAN PACIFIC KANSAS CITY": "CP",
    "CONSTELLATION ENERGY": "CEG",
    "VISTRA CORP": "VST",
    "DOMINION ENERGY INC": "D",
    "SEMPRA": "SRE",
    "EXELON CORP": "EXC",
    "CONSOLIDATED EDISON": "ED",
    "WEC ENERGY GROUP": "WEC",
    "PACIFIC GAS & ELECTRIC": "PCG",
    "EVERSOURCE ENERGY": "ES",
    "ENTERGY CORP": "ETR",
    "FIRSTENERGY CORP": "FE",
    "WILLIAMS COS INC": "WMB",
    "KINDER MORGAN INC": "KMI",
    "ONEOK INC": "OKE",
    "ENTERPRISE PRODUCTS PARTNERS": "EPD",
    "TC ENERGY CORP": "TRP",
    "SUNCOR ENERGY INC": "SU",
    "CANADIAN NATURAL RESOURCES": "CNQ",
    "EQUINOR ASA": "EQNR",
    "DANAHER CORP": "DHR",
    "AGILENT TECHNOLOGIES INC": "A",
    "BECTON DICKINSON AND CO": "BDX",
    "EDWARDS LIFESCIENCES": "EW",
    "BOSTON SCIENTIFIC CORP": "BSX",
    "VERTEX PHARMACEUTICALS": "VRTX",
    "REGENERON PHARMACEUTICALS": "REGN",
    "MODERNA INC": "MRNA",
    "CVS HEALTH CORP": "CVS",
    "MCKESSON CORP": "MCK",
    "CARDINAL HEALTH INC": "CAH",
    "HUMANA INC": "HUM",
    "CENTENE CORP": "CNC",
    "GLAXOSMITHKLINE PLC": "GSK",
    "GSK PLC": "GSK",
    "SANOFI SA": "SNY",
    "SANOFI": "SNY",
    "ZOETIS INC": "ZTS",
    "ACCENTURE PLC": "ACN",
    "INFOSYS LTD": "INFY",
    "PALANTIR TECHNOLOGIES": "PLTR",
    "CROWDSTRIKE HOLDINGS": "CRWD",
    "FORTINET INC": "FTNT",
    "PALO ALTO NETWORKS INC": "PANW",
    "SNOWFLAKE INC": "SNOW",
    "DATADOG INC": "DDOG",
    "CLOUDFLARE INC": "NET",
    "UBER TECHNOLOGIES INC": "UBER",
    "AIRBNB INC": "ABNB",
    "PAYPAL HOLDINGS INC": "PYPL",
    "BLOCK INC": "SQ",
    "DIGITAL REALTY TRUST": "DLR",
    "WELLTOWER INC": "WELL",
    "SIMON PROPERTY GROUP": "SPG",
    "PUBLIC STORAGE": "PSA",
    "AMERICAN TOWER": "AMT",
    "IRON MOUNTAIN INC": "IRM",
    "CROWN CASTLE INC": "CCI",
    "SBA COMMUNICATIONS": "SBAC",
    "DEVON ENERGY CORP": "DVN",
    "OCCIDENTAL PETROLEUM": "OXY",
    "PIONEER NATURAL RESOURCES": "PXD",
    "HESS CORP": "HES",
    "HALLIBURTON CO": "HAL",
    "BAKER HUGHES CO": "BKR",
    "SCHLUMBERGER NV": "SLB",
    "NUCOR CORP": "NUE",
    "FREEPORT MCMORAN": "FCX",
    "NEWMONT CORP": "NEM",
    "BARRICK GOLD CORP": "GOLD",
    "SOUTHERN COPPER CORP": "SCCO",
    "TECK RESOURCES LTD": "TECK",
    "ECOLAB INC": "ECL",
    "DUPONT DE NEMOURS INC": "DD",
    "PPG INDUSTRIES INC": "PPG",
    "SHERWIN-WILLIAMS CO": "SHW",
    "CORTEVA INC": "CTVA",
    "AIR PRODUCTS AND CHEMICALS": "APD",
    "GENERAL DYNAMICS CORP": "GD",
    "NORTHROP GRUMMAN CORP": "NOC",
    "L3HARRIS TECHNOLOGIES": "LHX",
    "TRANSDIGM GROUP INC": "TDG",
    "CSX CORP": "CSX",
    "NORFOLK SOUTHERN CORP": "NSC",
    "EMERSON ELECTRIC CO": "EMR",
    "PARKER-HANNIFIN CORP": "PH",
    "ILLINOIS TOOL WORKS": "ITW",
    "EATON CORP PLC": "ETN",
    "AUTOMATIC DATA PROCESSING": "ADP",
    "VERISK ANALYTICS INC": "VRSK",
    "REPUBLIC SERVICES INC": "RSG",
    "WASTE MANAGEMENT INC": "WM",
    "DELTA AIR LINES INC": "DAL",
    "UNITED AIRLINES HOLDINGS": "UAL",
    "SOUTHWEST AIRLINES CO": "LUV",
    "CONSTELLATION BRANDS INC": "STZ",
    "MONSTER BEVERAGE CORP": "MNST",
    "KEURIG DR PEPPER INC": "KDP",
    "TARGET CORP": "TGT",
    "KROGER CO": "KR",
    "DOLLAR GENERAL CORP": "DG",
    "SYSCO CORP": "SYY",
    "ARCHER-DANIELS-MIDLAND": "ADM",
    "CHURCH & DWIGHT CO": "CHD",
    "CLOROX CO": "CLX",
    "CHIPOTLE MEXICAN GRILL": "CMG",
    "YUM BRANDS INC": "YUM",
    "MARRIOTT INTERNATIONAL": "MAR",
    "HILTON WORLDWIDE HOLDINGS": "HLT",
    "D.R. HORTON INC": "DHI",
    "LENNAR CORP": "LEN",
    "O'REILLY AUTOMOTIVE INC": "ORLY",
    "AUTOZONE INC": "AZO",
    "ROSS STORES INC": "ROST",
    "LULULEMON ATHLETICA": "LULU",
    "KKR & CO INC": "KKR",
    "APOLLO GLOBAL MANAGEMENT": "APO",
    "U.S. BANCORP": "USB",
    "PNC FINANCIAL SERVICES": "PNC",
    "CAPITAL ONE FINANCIAL": "COF",
    "DISCOVER FINANCIAL SERVICES": "DFS",
    "FIDELITY NATIONAL INFORMATION": "FIS",
    "FISERV INC": "FISV",
    "GLOBAL PAYMENTS INC": "GPN",
    "MSCI INC": "MSCI",
    "NASDAQ INC": "NDAQ",
}


def lookup_sector_fallback(ticker, name=None):
    """
    Look up sector/industry/country from static fallback.
    Tries ticker first (with alias resolution), then company name mapping.
    Returns (sector, industry, country) or (None, None, None).
    """
    t = (ticker or "").upper().strip()
    # Resolve ticker aliases first
    if t in _TICKER_ALIASES:
        t = _TICKER_ALIASES[t]
    if t in _SECTOR_FALLBACK:
        return _SECTOR_FALLBACK[t]
    # Try name-based lookup
    if name:
        n = name.upper().strip()
        # Exact match
        if n in _NAME_TO_TICKER:
            tk = _NAME_TO_TICKER[n]
            if tk in _SECTOR_FALLBACK:
                return _SECTOR_FALLBACK[tk]
        # Partial match: check if any key is contained in the name
        for key, tk in _NAME_TO_TICKER.items():
            if key in n:
                if tk in _SECTOR_FALLBACK:
                    return _SECTOR_FALLBACK[tk]
                break
    return (None, None, None)


def _cache_key(ticker, quarter_end):
    return f"{ticker}|{quarter_end}"


# ── Quarter date helpers ─────────────────────────────────────────────────────

def get_quarter_boundaries(period_of_report):
    """
    Given a period_of_report string (e.g. '2025-09-30'), return:
      (quarter_start, quarter_end, next_quarter_end)
    as date strings in YYYY-MM-DD format.
    """
    dt = datetime.strptime(period_of_report[:10], "%Y-%m-%d")
    month = dt.month

    if month <= 3:
        qtr_end = datetime(dt.year, 3, 31)
        qtr_start = datetime(dt.year, 1, 1)
    elif month <= 6:
        qtr_end = datetime(dt.year, 6, 30)
        qtr_start = datetime(dt.year, 4, 1)
    elif month <= 9:
        qtr_end = datetime(dt.year, 9, 30)
        qtr_start = datetime(dt.year, 7, 1)
    else:
        qtr_end = datetime(dt.year, 12, 31)
        qtr_start = datetime(dt.year, 10, 1)

    next_qtr_end = qtr_end + relativedelta(months=3)
    next_qtr_end = (next_qtr_end.replace(day=1) + relativedelta(months=1) - timedelta(days=1))

    return (
        qtr_start.strftime("%Y-%m-%d"),
        qtr_end.strftime("%Y-%m-%d"),
        next_qtr_end.strftime("%Y-%m-%d"),
    )


def get_prior_quarter_boundaries(period_of_report):
    """
    Given a period_of_report string, return the PRIOR quarter boundaries:
      (prior_qtr_start, prior_qtr_end)
    E.g. for '2025-09-30' (Q3), prior = Q2: ('2025-04-01', '2025-06-30')
    """
    qtr_start, _, _ = get_quarter_boundaries(period_of_report)
    prior_qtr_end = (datetime.strptime(qtr_start, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prior_start, prior_end, _ = get_quarter_boundaries(prior_qtr_end)
    return prior_start, prior_end


def _get_close_price(hist, target_date, window_days=5):
    """Get closing price nearest to target_date within a window."""
    if hist is None or hist.empty:
        return None
    dt = datetime.strptime(target_date[:10], "%Y-%m-%d")
    for offset in range(window_days + 1):
        for direction in [0, -1, 1]:
            check = dt + timedelta(days=offset * direction)
            check_str = check.strftime("%Y-%m-%d")
            if check_str in hist.index.strftime("%Y-%m-%d").values:
                idx = hist.index[hist.index.strftime("%Y-%m-%d") == check_str][0]
                return float(hist.loc[idx, "Close"])
    return None


def _safe_float(val):
    """Convert to float, returning None if not possible."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


# ── EPS helpers ──────────────────────────────────────────────────────────────

def _trailing_12m_eps(earnings_df, as_of_date):
    """
    Sum the 4 most recent reported EPS values as of `as_of_date`.
    Returns trailing 12-month EPS or None.
    """
    if earnings_df is None or earnings_df.empty:
        return None
    as_of = datetime.strptime(as_of_date[:10], "%Y-%m-%d")
    valid = []
    for idx in earnings_df.index:
        try:
            earn_date = idx.to_pydatetime().replace(tzinfo=None)
        except Exception:
            continue
        # Include earnings reported up to 45 days after the as_of date
        # (to catch late-reported earnings for that quarter)
        if earn_date <= as_of + timedelta(days=45):
            reported = _safe_float(earnings_df.loc[idx].get("Reported EPS"))
            if reported is not None:
                valid.append((earn_date, reported))
    valid.sort(key=lambda x: x[0], reverse=True)
    if len(valid) >= 4:
        return sum(v[1] for v in valid[:4])
    return None


def _match_eps_to_quarter(earnings_df, qtr_end_date):
    """
    Find the earnings report closest to a quarter end.
    Returns (reported_eps, consensus_eps, beat_dollars, beat_pct) or all None.
    """
    if earnings_df is None or earnings_df.empty:
        return None, None, None, None
    qtr_end_dt = datetime.strptime(qtr_end_date[:10], "%Y-%m-%d")
    best_match = None
    best_distance = float("inf")
    for idx in earnings_df.index:
        try:
            earn_date = idx.to_pydatetime().replace(tzinfo=None)
        except Exception:
            continue
        delta = (earn_date - qtr_end_dt).days
        if -30 <= delta <= 90:
            dist = abs(delta)
            if dist < best_distance:
                best_distance = dist
                best_match = idx
    if best_match is None:
        return None, None, None, None
    row = earnings_df.loc[best_match]
    reported = _safe_float(row.get("Reported EPS"))
    consensus = _safe_float(row.get("EPS Estimate"))
    beat_dollars = None
    beat_pct = None
    if reported is not None and consensus is not None:
        beat_dollars = round(reported - consensus, 4)
        if abs(consensus) > 0.001:
            beat_pct = round((reported - consensus) / abs(consensus) * 100, 2)
    return reported, consensus, beat_dollars, beat_pct


# ── Empty result template ────────────────────────────────────────────────────

def _empty_result():
    """Return a dict with all enrichment fields set to None."""
    return {
        # Prior quarter
        "prior_price_qtr_end": None,
        "prior_quarter_return_pct": None,
        "prior_reported_eps": None,
        "prior_consensus_eps": None,
        "prior_eps_beat_dollars": None,
        "prior_eps_beat_pct": None,
        # Filing/most recent quarter
        "filing_price_qtr_end": None,
        "filing_quarter_return_pct": None,
        "filing_reported_eps": None,
        "filing_consensus_eps": None,
        "filing_eps_beat_dollars": None,
        "filing_eps_beat_pct": None,
        # Current / live
        "forward_pe": None,
        "forward_eps_growth": None,
        "dividend_yield": None,
        "trailing_eps": None,
        "forward_eps": None,
        # QTD (quarter-to-date since filing quarter end)
        "qtd_return_pct": None,
        "qtd_price_start": None,
        # Monthly returns within current quarter
        "monthly_returns": None,  # [{month: "Jan", return_pct: 3.2}, ...]
        # Current price (most recent close)
        "current_price": None,
        # Revenue / Sales
        "forward_revenue_growth": None,
        "forward_ps": None,
        # Static
        "sector": None,
        "industry": None,
        "country": None,
    }


# ── Single ticker fetch ─────────────────────────────────────────────────────

def fetch_ticker_data(ticker, quarter_end):
    """
    Fetch all financial enrichment data for a single ticker across two quarters.

    Returns dict with 21 fields — prior quarter (7), filing quarter (7),
    current/live (3), static (2). All default to None on failure.
    """
    key = _cache_key(ticker, quarter_end)
    with _cache_lock:
        if key in _cache:
            return _cache[key]

    result = _empty_result()

    try:
        t = yf.Ticker(ticker)

        # ── Quarter boundaries ────
        qtr_start, qtr_end_date, _ = get_quarter_boundaries(quarter_end)
        prior_qtr_start, prior_qtr_end = get_prior_quarter_boundaries(quarter_end)

        # ── Historical prices covering both quarters ────
        hist_start = (datetime.strptime(prior_qtr_start, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
        hist_end = (datetime.strptime(qtr_end_date, "%Y-%m-%d") + timedelta(days=10)).strftime("%Y-%m-%d")

        hist = None
        try:
            hist = t.history(start=hist_start, end=hist_end, auto_adjust=True)
        except Exception:
            pass

        # Prior quarter return + price
        if hist is not None and not hist.empty:
            p_prior_start = _get_close_price(hist, prior_qtr_start)
            p_prior_end = _get_close_price(hist, prior_qtr_end)
            result["prior_price_qtr_end"] = round(p_prior_end, 2) if p_prior_end else None
            if p_prior_start and p_prior_end and p_prior_start > 0:
                result["prior_quarter_return_pct"] = round((p_prior_end / p_prior_start - 1) * 100, 2)

            # Filing quarter return + price
            p_filing_start = _get_close_price(hist, qtr_start)
            p_filing_end = _get_close_price(hist, qtr_end_date)
            result["filing_price_qtr_end"] = round(p_filing_end, 2) if p_filing_end else None
            if p_filing_start and p_filing_end and p_filing_start > 0:
                result["filing_quarter_return_pct"] = round((p_filing_end / p_filing_start - 1) * 100, 2)

        # ── Info (forward P/E, growth, dividend, sector) ────
        info = {}
        try:
            info = t.info or {}
        except Exception:
            pass

        result["forward_pe"] = _safe_float(info.get("forwardPE"))
        result["sector"] = info.get("sector")
        result["industry"] = info.get("industry")
        result["country"] = info.get("country")

        # Sector/industry/country fallback from static map when yfinance returns None
        if not result["sector"]:
            fb_s, fb_i, fb_c = lookup_sector_fallback(ticker)
            if fb_s:
                result["sector"] = result["sector"] or fb_s
                result["industry"] = result["industry"] or fb_i
                result["country"] = result["country"] or fb_c

        # Dividend yield — yfinance `dividendYield` is already in percentage form
        # (e.g. 0.39 means 0.39%, 3.01 means 3.01%). Do NOT multiply by 100.
        raw_dy = _safe_float(info.get("dividendYield"))
        if raw_dy is not None:
            result["dividend_yield"] = round(raw_dy, 2)

        # Trailing EPS (trailing 4 quarters) and Forward EPS (forward 12 months)
        fwd_eps = _safe_float(info.get("forwardEps"))
        trail_eps = _safe_float(info.get("trailingEps"))
        if trail_eps is not None:
            result["trailing_eps"] = round(trail_eps, 2)
        if fwd_eps is not None:
            result["forward_eps"] = round(fwd_eps, 2)

        # Forward EPS growth — prefer analyst consensus next-year growth estimate
        _eps_growth_set = False
        try:
            ge = t.growth_estimates
            if ge is not None and not ge.empty and "+1y" in ge.index:
                next_yr = ge.loc["+1y", "stockTrend"]
                if next_yr is not None and not (isinstance(next_yr, float) and next_yr != next_yr):
                    result["forward_eps_growth"] = round(float(next_yr) * 100, 2)
                    _eps_growth_set = True
        except Exception:
            pass
        if not _eps_growth_set:
            fwd_growth = _safe_float(info.get("earningsGrowth"))
            if fwd_growth is not None:
                result["forward_eps_growth"] = round(fwd_growth * 100, 2)
            elif fwd_eps is not None and trail_eps is not None and abs(trail_eps) > 0.001:
                result["forward_eps_growth"] = round((fwd_eps - trail_eps) / abs(trail_eps) * 100, 2)

        # ── Forward revenue growth — analyst consensus next-year revenue growth ──
        _rev_growth_val = None
        try:
            re = t.revenue_estimate
            if re is not None and not re.empty and "+1y" in re.index:
                rev_gr = re.loc["+1y", "growth"]
                if rev_gr is not None and not (isinstance(rev_gr, float) and rev_gr != rev_gr):
                    _rev_growth_val = float(rev_gr)
                    result["forward_revenue_growth"] = round(_rev_growth_val * 100, 2)
        except Exception:
            pass
        if result["forward_revenue_growth"] is None:
            rg = _safe_float(info.get("revenueGrowth"))
            if rg is not None:
                _rev_growth_val = rg
                result["forward_revenue_growth"] = round(rg * 100, 2)

        # ── Forward P/S (Price-to-Sales) ────
        market_cap = _safe_float(info.get("marketCap"))
        total_revenue = _safe_float(info.get("totalRevenue"))
        if market_cap and total_revenue and total_revenue > 0:
            if _rev_growth_val is not None and _rev_growth_val > -1:
                fwd_revenue = total_revenue * (1 + _rev_growth_val)
                if fwd_revenue > 0:
                    result["forward_ps"] = round(market_cap / fwd_revenue, 2)
            else:
                result["forward_ps"] = round(market_cap / total_revenue, 2)

        # ── Earnings data for EPS beat ────
        earnings = None
        try:
            earnings = t.get_earnings_dates(limit=20)
        except Exception:
            pass

        # EPS match for prior quarter
        pr_rep, pr_con, pr_beat_d, pr_beat_p = _match_eps_to_quarter(earnings, prior_qtr_end)
        result["prior_reported_eps"] = pr_rep
        result["prior_consensus_eps"] = pr_con
        result["prior_eps_beat_dollars"] = pr_beat_d
        result["prior_eps_beat_pct"] = pr_beat_p

        # EPS match for filing quarter
        fl_rep, fl_con, fl_beat_d, fl_beat_p = _match_eps_to_quarter(earnings, qtr_end_date)
        result["filing_reported_eps"] = fl_rep
        result["filing_consensus_eps"] = fl_con
        result["filing_eps_beat_dollars"] = fl_beat_d
        result["filing_eps_beat_pct"] = fl_beat_p

        # ── QTD return (quarter-end+1 to previous trading day close) ────
        today = datetime.now()
        qtr_end_dt = datetime.strptime(qtr_end_date, "%Y-%m-%d")
        if today > qtr_end_dt:
            qtd_start = (qtr_end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            qtd_end = (today + timedelta(days=1)).strftime("%Y-%m-%d")  # yfinance end is exclusive
            try:
                qtd_hist = t.history(start=qtd_start, end=qtd_end, auto_adjust=True)
                if qtd_hist is not None and len(qtd_hist) >= 2:
                    qtd_start_price = float(qtd_hist["Close"].iloc[0])
                    qtd_end_price = float(qtd_hist["Close"].iloc[-1])  # last available close
                    if qtd_start_price > 0:
                        result["qtd_return_pct"] = round((qtd_end_price / qtd_start_price - 1) * 100, 2)
                        result["qtd_price_start"] = round(qtd_start_price, 2)
                    result["current_price"] = round(qtd_end_price, 2)

                    # ── Monthly returns within current quarter ────
                    # Determine months in the current quarter (quarter after filing quarter)
                    curr_q_start_month = qtr_end_dt.month + 1
                    curr_q_start_year = qtr_end_dt.year
                    if curr_q_start_month > 12:
                        curr_q_start_month = 1
                        curr_q_start_year += 1
                    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
                    monthly_rets = []
                    for mi in range(3):  # up to 3 months in a quarter
                        m = curr_q_start_month + mi
                        yr = curr_q_start_year
                        if m > 12:
                            m -= 12
                            yr += 1
                        # Month start and end
                        m_start_dt = datetime(yr, m, 1)
                        if m == 12:
                            m_end_dt = datetime(yr + 1, 1, 1)
                        else:
                            m_end_dt = datetime(yr, m + 1, 1)
                        # Only include months that have started
                        if m_start_dt > today:
                            break
                        is_current_month = (today.year == yr and today.month == m)
                        label = month_names[m - 1] + (" MTD" if is_current_month else "")
                        # Filter qtd_hist for this month
                        try:
                            m_data = qtd_hist[(qtd_hist.index >= m_start_dt.strftime("%Y-%m-%d")) &
                                              (qtd_hist.index < m_end_dt.strftime("%Y-%m-%d"))]
                            if m_data is not None and len(m_data) >= 1:
                                # Use last close of prior month (or qtd start) as base
                                prior = qtd_hist[qtd_hist.index < m_start_dt.strftime("%Y-%m-%d")]
                                if len(prior) > 0:
                                    base_price = float(prior["Close"].iloc[-1])
                                else:
                                    base_price = qtd_start_price
                                end_price = float(m_data["Close"].iloc[-1])
                                if base_price > 0:
                                    monthly_rets.append({
                                        "month": label,
                                        "return_pct": round((end_price / base_price - 1) * 100, 2)
                                    })
                                else:
                                    monthly_rets.append({"month": label, "return_pct": None})
                            else:
                                monthly_rets.append({"month": label, "return_pct": None})
                        except Exception:
                            monthly_rets.append({"month": label, "return_pct": None})
                    if monthly_rets:
                        result["monthly_returns"] = monthly_rets
            except Exception:
                pass

    except Exception:
        pass

    with _cache_lock:
        _cache[key] = result

    return result


# ── Batch fetch ──────────────────────────────────────────────────────────────

def batch_fetch_financial_data(tickers, quarter_end, max_workers=10,
                               progress_callback=None, **_kwargs):
    """
    Fetch financial data for multiple tickers concurrently.

    Args:
        tickers: list of ticker symbols
        quarter_end: quarter end date string (YYYY-MM-DD)
        max_workers: max concurrent threads
        progress_callback: callable(done, total) for progress updates

    Returns:
        dict mapping ticker -> enrichment dict
    """
    results = {}
    total = len(tickers)
    done = 0

    if not tickers:
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(fetch_ticker_data, ticker, quarter_end): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
            except Exception:
                results[ticker] = _empty_result()
            done += 1
            if progress_callback:
                try:
                    progress_callback(done, total)
                except Exception:
                    pass

    return results


def clear_cache():
    """Clear the in-memory cache."""
    with _cache_lock:
        _cache.clear()


# ── Sector Name Normalization (Yahoo Finance → GICS standard) ─────────────────

SECTOR_NAME_MAP = {
    "Technology":             "Information Technology",
    "Healthcare":             "Health Care",
    "Consumer Cyclical":      "Consumer Discretionary",
    "Consumer Defensive":     "Consumer Staples",
    "Financial Services":     "Financials",
    "Basic Materials":        "Materials",
    "Communication":          "Communication Services",
    # Already matching in both systems:
    "Communication Services": "Communication Services",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Real Estate":            "Real Estate",
    "Utilities":              "Utilities",
}


def normalize_sector_name(name):
    """Normalize a sector name to GICS standard (used by MSCI/ACWI).
    Handles both Yahoo Finance names and GICS names as input."""
    if not name:
        return "Unknown"
    return SECTOR_NAME_MAP.get(name, name)


# ── Country Name Normalization ───────────────────────────────────────────────

COUNTRY_NAME_MAP = {
    # yfinance / other sources → iShares ACWI Location standard
    "South Korea":          "Korea (South)",
    "Korea, Republic of":   "Korea (South)",
    "Republic of Korea":    "Korea (South)",
    "Cayman Islands":       "Cayman Islands",
    "Bermuda":              "Bermuda",
    "Hong Kong SAR":        "Hong Kong",
    # Most names already match between yfinance and iShares:
    # United States, Japan, United Kingdom, China, Canada, Taiwan, etc.
}


def normalize_country_name(name):
    """Normalize a country name to match iShares ACWI Location field."""
    if not name:
        return "Unknown"
    return COUNTRY_NAME_MAP.get(name, name)


# ── MSCI ACWI Benchmark Data ─────────────────────────────────────────────────

import time as _time  # noqa: E402
import csv as _csv  # noqa: E402
import io as _io  # noqa: E402
import urllib.request as _urllib_req  # noqa: E402

_acwi_cache = {"data": None, "timestamp": 0}
_acwi_cache_lock = threading.Lock()
_ACWI_CACHE_TTL = 86400  # 24 hours


def fetch_acwi_benchmark():
    """
    Fetch MSCI ACWI benchmark data (via iShares ACWI ETF).
    Returns dict with:
        sectors:      {gics_sector_name: weight_pct, ...}
        countries:    {country_name: weight_pct, ...}
        top_holdings: [{ticker, name, weight}, ...]

    Strategy: iShares CSV → yfinance → hardcoded fallback.
    Caches in memory for 24 hours.
    """
    with _acwi_cache_lock:
        if (_acwi_cache["data"] is not None and
                _time.time() - _acwi_cache["timestamp"] < _ACWI_CACHE_TTL):
            return _acwi_cache["data"]

    data = _fetch_acwi_from_ishares()
    if data is None:
        data = _fetch_acwi_from_yfinance()
    if data is None:
        data = _acwi_hardcoded()

    with _acwi_cache_lock:
        _acwi_cache["data"] = data
        _acwi_cache["timestamp"] = _time.time()
    return data


def _fetch_acwi_from_ishares():
    """Primary: download iShares ACWI ETF holdings CSV from BlackRock."""
    try:
        url = (
            "https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf/"
            "1467271812596.ajax?fileType=csv&fileName=ACWI_holdings&dataType=fund"
        )
        req = _urllib_req.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/csv,text/plain,*/*",
        })
        with _urllib_req.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        lines = raw.split("\n")

        # Find the header row (contains "Ticker" and "Sector")
        header_idx = None
        for i, line in enumerate(lines):
            if "Ticker" in line and "Sector" in line and "Weight" in line:
                header_idx = i
                break
        if header_idx is None:
            print("[ACWI] Could not find header row in iShares CSV")
            return None

        # Parse CSV from header onwards
        csv_text = "\n".join(lines[header_idx:])
        reader = _csv.DictReader(_io.StringIO(csv_text))

        sectors = {}
        countries = {}
        holdings = []

        for row in reader:
            ticker = (row.get("Ticker") or "").strip()
            name = (row.get("Name") or "").strip()
            sector = (row.get("Sector") or "").strip()
            location = (row.get("Location") or "").strip()
            asset_class = (row.get("Asset Class") or "").strip()

            try:
                weight = float(row.get("Weight (%)", 0))
            except (ValueError, TypeError):
                continue

            if weight <= 0 or not name:
                continue

            # Only count equity positions for sector/country
            if asset_class and asset_class.lower() not in ("equity", ""):
                continue

            if sector and sector != "-":
                sectors[sector] = sectors.get(sector, 0) + weight
            if location and location != "-":
                countries[location] = countries.get(location, 0) + weight
            if ticker and ticker != "-":
                holdings.append({"ticker": ticker, "name": name, "weight": round(weight, 4)})

        if not sectors:
            print("[ACWI] iShares CSV parsed but no sectors found")
            return None

        # Sort holdings by weight
        holdings.sort(key=lambda h: h["weight"], reverse=True)

        # Round sector/country weights
        sectors = {k: round(v, 2) for k, v in sorted(sectors.items(), key=lambda x: -x[1])}
        countries = {k: round(v, 2) for k, v in sorted(countries.items(), key=lambda x: -x[1])}

        print(f"[ACWI] Loaded from iShares CSV: {len(sectors)} sectors, "
              f"{len(countries)} countries, {len(holdings)} holdings")
        return {
            "sectors": sectors,
            "countries": countries,
            "top_holdings": holdings[:30],
            "source": "iShares CSV",
        }
    except Exception as e:
        print(f"[ACWI] iShares CSV failed: {e}")
        return None


def _fetch_acwi_from_yfinance():
    """Fallback: use yfinance for sector weights + top holdings."""
    try:
        acwi = yf.Ticker("ACWI")
        fund = acwi.funds_data

        sectors = {}
        try:
            sw = fund.sector_weightings
            # yfinance returns list of single-key dicts like [{"realestate": 0.02}, ...]
            _yf_sector_map = {
                "realestate": "Real Estate", "consumer_cyclical": "Consumer Discretionary",
                "basic_materials": "Materials", "consumer_defensive": "Consumer Staples",
                "technology": "Information Technology", "communication_services": "Communication Services",
                "financial_services": "Financials", "utilities": "Utilities",
                "industrials": "Industrials", "healthcare": "Health Care", "energy": "Energy",
            }
            if isinstance(sw, list):
                for item in sw:
                    for k, v in item.items():
                        gics = _yf_sector_map.get(k, k.title())
                        sectors[gics] = round(float(v) * 100, 2)
            elif isinstance(sw, dict):
                for k, v in sw.items():
                    gics = _yf_sector_map.get(k, k.title())
                    sectors[gics] = round(float(v) * 100, 2)
        except Exception:
            pass

        top_holdings = []
        try:
            th = fund.top_holdings
            if th is not None and hasattr(th, "iterrows"):
                for _, row in th.iterrows():
                    top_holdings.append({
                        "ticker": str(row.get("Symbol", row.name)).strip(),
                        "name": str(row.get("Name", "")).strip(),
                        "weight": round(float(row.get("% Assets", 0)) * 100, 4)
                        if float(row.get("% Assets", 0)) < 1
                        else round(float(row.get("% Assets", 0)), 4),
                    })
        except Exception:
            pass

        if sectors:
            print(f"[ACWI] Loaded from yfinance: {len(sectors)} sectors, "
                  f"{len(top_holdings)} top holdings (no country data)")
            return {
                "sectors": sectors,
                "countries": _acwi_hardcoded()["countries"],  # no country data from yfinance
                "top_holdings": top_holdings or _acwi_hardcoded()["top_holdings"],
                "source": "yfinance",
            }
        return None
    except Exception as e:
        print(f"[ACWI] yfinance fallback failed: {e}")
        return None


def _acwi_hardcoded():
    """Ultimate fallback: hardcoded ACWI approximate weights (as of late 2025)."""
    return {
        "sectors": {
            "Information Technology": 25.2,
            "Financials": 16.8,
            "Health Care": 11.0,
            "Consumer Discretionary": 10.8,
            "Industrials": 10.5,
            "Communication Services": 7.8,
            "Consumer Staples": 6.2,
            "Energy": 4.2,
            "Materials": 3.8,
            "Utilities": 2.7,
            "Real Estate": 2.1,
        },
        "countries": {
            "United States": 63.7,
            "Japan": 5.2,
            "United Kingdom": 3.5,
            "China": 2.8,
            "France": 2.7,
            "Canada": 2.7,
            "Switzerland": 2.3,
            "Germany": 2.1,
            "India": 2.0,
            "Australia": 1.7,
            "Taiwan": 1.6,
            "Korea (South)": 1.3,
            "Netherlands": 1.1,
            "Other": 7.3,
        },
        "top_holdings": [
            {"ticker": "AAPL", "name": "Apple Inc", "weight": 4.5},
            {"ticker": "NVDA", "name": "NVIDIA Corp", "weight": 4.2},
            {"ticker": "MSFT", "name": "Microsoft Corp", "weight": 3.9},
            {"ticker": "AMZN", "name": "Amazon.com Inc", "weight": 2.5},
            {"ticker": "META", "name": "Meta Platforms Inc", "weight": 1.7},
            {"ticker": "GOOGL", "name": "Alphabet Inc A", "weight": 1.3},
            {"ticker": "GOOG", "name": "Alphabet Inc C", "weight": 1.1},
            {"ticker": "TSLA", "name": "Tesla Inc", "weight": 1.1},
            {"ticker": "AVGO", "name": "Broadcom Inc", "weight": 1.0},
            {"ticker": "JPM", "name": "JPMorgan Chase & Co", "weight": 0.9},
        ],
        "source": "hardcoded (approx late 2025)",
    }
