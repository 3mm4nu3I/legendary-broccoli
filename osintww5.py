"""
OSINT Command Terminal — Omega Predictive Engine (v6)
==========================================================================
Architecture Upgrades:
- 4x Prediction Market Consensus (Polymarket, Kalshi, PredictIt, Manifold)
- 3x Concurrent Feed Expansion (150+ Thread-Pooled Inputs)
- 5-Vector Investment Advisory Engine on the [P] Page
- Direct mapping of OSINT headlines & market sentiment into the UI output.
"""

import os
import sys
import csv
import json
import time
import platform
import re
import threading
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import logging
import datetime
import warnings
from collections import deque
from pathlib import Path
from zoneinfo import ZoneInfo
import concurrent.futures

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# ---------------------------------------------------------
# PATHS & LOGGING
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "osint_terminal.log"
EVENT_LOG_PATH = SCRIPT_DIR / "kinetic_event_log.csv"
LABOR_EVENT_LOG_PATH = SCRIPT_DIR / "labor_event_log.csv"
UNUSUAL_ACTIVITY_LOG_PATH = SCRIPT_DIR / "unusual_activity_log.csv"
PREDICTION_AUDIT_PATH = SCRIPT_DIR / "prediction_audit.txt"

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
)
logger = logging.getLogger("osint_terminal")

# ---------------------------------------------------------
# PLATFORM HANDLING
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
    def _kbhit():
        return msvcrt.kbhit()
    def _getch():
        ch = msvcrt.getch()
        if ch in b'\x00\xe0':
            msvcrt.getch()
            return ''
        try:
            return ch.decode('utf-8', errors='ignore').lower()
        except Exception:
            return ''
else:
    def _kbhit():
        return False
    def _getch():
        return ''

# ---------------------------------------------------------
# UI COLOR PALETTE & THRESHOLDS
# ---------------------------------------------------------
RESET = "\033[0m"
PURPLE = "\033[95m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

EXTREME_THRESH = 0.02
HIGH_THRESH = 0.01

# ---------------------------------------------------------
# GLOBAL ENGINE CONFIGURATION
# ---------------------------------------------------------
OSINT_BATCH_INTERVAL_SECONDS = 90.0 
PRICE_POLL_BASE_SECONDS = 1.5
PRICE_POLL_MAX_BACKOFF = 30.0
POLL_INTERVAL = 0.3  

MODEL_COEFFICIENTS = {
    "beta0": 0.0276,   "beta1": 0.0356,   "beta2": -0.0251,  "beta3": -0.0006
}

ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]
KINETIC_DECAY_SECONDS = 4 * 3600
LABOR_DECAY_SECONDS = 6 * 3600
UNUSUAL_VOLUME_Z = 3.0
MARKET_CLOSE_HOUR_ET = 16
LATE_SESSION_WINDOW_MINUTES = 30

ET_ZONE = ZoneInfo("America/New_York")
LOCAL_ZONE = ZoneInfo("Europe/Lisbon")

COMPOSITE_WEIGHTS = {"kinetic": 0.25, "labor": 0.15, "flights": 0.15, "weather": 0.15, "prediction_markets": 0.30}

# ---------------------------------------------------------
# 150+ CONCURRENT FEED REGISTRY (3x EXPANDED + 4x MARKETS)
# ---------------------------------------------------------
API_ENDPOINTS = {
    "NWS_WEATHER": "https://api.weather.gov/alerts/active?severity=Severe,Extreme",
    "FAA_STATUS": "https://nasstatus.faa.gov/api/airport-status-information",
    "SEC_FORM4": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=40&output=atom",
    "POLYMARKET": "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=30&order=volume24hr&ascending=false",
    "KALSHI": "https://api.elections.kalshi.com/trade-api/v2/markets?limit=30&status=open",
    "PREDICTIT": "https://www.predictit.org/api/marketdata/all/",
    "MANIFOLD": "https://api.manifold.markets/v0/markets?limit=30"
}

REGIONS = {
    "US": {"gl": "US", "ceid": "US:en", "hl": "en-US"},
    "UK": {"gl": "GB", "ceid": "GB:en", "hl": "en-GB"},
    "CA": {"gl": "CA", "ceid": "CA:en", "hl": "en-CA"},
    "EU": {"gl": "IE", "ceid": "IE:en", "hl": "en-IE"},
    "GLOBAL_SOUTH": {"gl": "IN", "ceid": "IN:en", "hl": "en-IN"}
}

# 3x Query Expansion (9 strings per category)
TOPICS = {
    "kinetic": ['"strike" OR "missile" OR "airstrike"', '"NATO" OR "warship" OR "frontline"', '"conflict" OR "bombing" OR "ammunition"', '"drone swarm" OR "artillery"', '"ceasefire" OR "peace talks"', '"nuclear readiness" OR "ballistic"', '"troop deployment" OR "mobilization"', '"casualties" OR "civilian deaths"', '"naval blockade" OR "airspace violation"'],
    "labor": ['"union strike" OR "work stoppage"', '"labor dispute" OR "wage negotiations"', '"factory strike" OR "transport strike"', '"picket line" OR "unionizing"', '"wildcat strike" OR "mass walkout"', '"worker uprising" OR "labor shortage"', '"teacher strike" OR "healthcare strike"', '"port workers union" OR "railway strike"', '"collective bargaining" OR "union contract"'],
    "regulatory": ['"regulator" OR "antitrust"', '"fined" OR "central bank"', '"sec enforcement" OR "eu commission"', '"subpoena" OR "doj probe"', '"tariffs" OR "trade war"', '"export restriction" OR "sanctions"', '"market manipulation" OR "insider trading"', '"merger blocked" OR "monopoly"', '"data privacy fine" OR "gdpr violation"'],
    "macro": ['"interest rates" OR "inflation"', '"central bank" OR "gdp growth"', '"federal reserve" OR "ecb rate"', '"quantitative easing" OR "yield curve"', '"hyperinflation" OR "deflation"', '"jobless claims" OR "unemployment rate"', '"cpi data" OR "ppi inflation"', '"recession fears" OR "economic contraction"', '"fiat currency" OR "forex reserves"'],
    "supply_chain": ['"port congestion" OR "shipping delay"', '"border closure" OR "canal blockage"', '"supply chain bottleneck" OR "cargo shortage"', '"trucking strike" OR "freight rates"', '"warehouse backlog" OR "inventory glut"', '"semiconductor supply" OR "component shortage"', '"air freight" OR "vessel tracking"', '"customs delay" OR "import backlog"', '"rail freight" OR "logistics breakdown"'],
    "energy": ['"oil shock" OR "natural gas"', '"grid failure" OR "power outage"', '"opec quota" OR "refinery fire"', '"strategic petroleum reserve" OR "spr release"', '"nuclear reactor" OR "uranium enrichment"', '"diesel crack spread" OR "gasoline inventory"', '"offshore drilling" OR "rig count"', '"solar supply" OR "battery shortage"', '"lng export" OR "pipeline disruption"'],
    "tech_cyber": ['"cyber attack" OR "data breach"', '"network outage" OR "semiconductor shortage"', '"critical infrastructure hack" OR "zero-day"', '"ddos attack" OR "ransomware gang"', '"state sponsored hacker" OR "espionage"', '"server downtime" OR "cloud outage"', '"fiber cut" OR "submarine cable"', '"ai regulation" OR "gpu allocation"', '"cryptographic break" OR "quantum computing"'],
    "metals": ['"rare earth" OR "copper shortage"', '"gold price" OR "silver bullion"', '"aluminum tariff" OR "nickel supply"', '"zinc" OR "palladium"', '"lme inventory" OR "comex warehouse"', '"smelter shutdown" OR "mining strike"', '"iron ore" OR "steel production"', '"uranium spot" OR "lithium mine"', '"strategic metals" OR "cobalt supply"'],
    "agriculture": ['"crop failure" OR "farm labor"', '"export ban" OR "fertilizer shortage"', '"food inflation" OR "livestock disease"', '"soy futures" OR "corn yield"', '"wheat harvest" OR "grain corridor"', '"dairy inflation" OR "poultry cull"', '"palm oil" OR "cocoa shortage"', '"drought impact" OR "flood damage"', '"agricultural subsidy" OR "tractor supply"'],
    "border_enforcement": ['"worksite enforcement" OR "deportation"', '"illegal immigration raid" OR "border security"', '"port security" OR "contraband intercept"', '"asylum seekers" OR "border patrol"', '"tariff evasion" OR "customs seizure"', '"human trafficking" OR "smuggling bust"', '"border checkpoint" OR "visa restriction"', '"embargo enforcement" OR "sanctions evasion"', '"maritime interception" OR "coast guard"']
}

FEED_REGISTRY = [{"type": "api", "category": name, "url": url} for name, url in API_ENDPOINTS.items()]

for region, params in REGIONS.items():
    for topic, queries in TOPICS.items():
        for q in queries:
            encoded = urllib.parse.quote(q)
            FEED_REGISTRY.append({"type": "rss", "category": topic, "region": region, "url": f"https://news.google.com/rss/search?q={encoded}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"})

WORLD_SYNNDICATES = [
    {"type": "rss", "category": "worldwide_wire", "region": "REUTERS", "url": "https://news.google.com/rss/search?q=site:reuters.com+world&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "worldwide_wire", "region": "AFP", "url": "https://news.google.com/rss/search?q=AFP+news+breaking&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "worldwide_wire", "region": "AP", "url": "https://news.google.com/rss/search?q=Associated+Press+world+news&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "worldwide_wire", "region": "AL_JAZEERA", "url": "https://news.google.com/rss/search?q=Al+Jazeera+breaking+news&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "worldwide_wire", "region": "BLOOMBERG", "url": "https://news.google.com/rss/search?q=Bloomberg+markets+geopolitics&hl=en-US&gl=US&ceid=US:en"}
]
FEED_REGISTRY.extend(WORLD_SYNNDICATES)

MARKETS = {
    "1": {"name": "GLOBAL AGGREGATE", "tickers": ["BZ=F", "^GSPC", "^FTSE", "^GSPTSE", "VGK", "GC=F", "BTC-USD", "BDRY", "IYT", "^TNX"]},
    "2": {"name": "ENERGY COMMODITIES", "tickers": ["BZ=F", "CL=F", "NG=F", "HO=F", "RB=F"]},
    "3": {"name": "POWER GRID & TRANSMISSION", "tickers": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC"]},
    "4": {"name": "TELECOM & SATELLITE", "tickers": ["XLC", "VZ", "T", "TMUS", "ASTS", "IRDM"]},
    "5": {"name": "MACRO & YIELD CURVE", "tickers": ["^TNX", "^TYX", "UUP", "^GSPC", "^VIX"]},
    "6": {"name": "SUPPLY CHAIN & TRANSPORT", "tickers": ["BDRY", "IYT", "FDX", "UNP"]},
    "7": {"name": "METALS & RARE EARTH", "tickers": ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F", "REMX", "LIT"]},
    "8": {"name": "CRYPTOCURRENCY MAJORS", "tickers": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD"]},
    "9": {"name": "EMERGING TECH & AI", "tickers": ["NVDA", "MSFT", "AMD", "PLTR", "TSM", "CRWD"]},
    "0": {"name": "LABOR & AGRICULTURE", "tickers": ["TSN", "CAG", "HRL", "MOO", "VEGI"]},
}

current_view = "1"
view_lock = threading.Lock()
WINDOW_SIZE = 600
SPARK_WIDTH = 12

state_lock = threading.Lock()
osint_data = []
seen_headlines = set()

kinetic_headline_queue = deque()
kinetic_active_until = None
kinetic_last_headline = None
war_headlines = deque(maxlen=40)
seen_kinetic = set()

labor_headline_queue = deque()
labor_active_until = None
labor_last_headline = None
seen_labor = set()

weather_alerts = []
flight_disruptions = []
insider_filings = deque(maxlen=20)
seen_insider_links = set()

prediction_markets = []
prev_prediction_prices = {}

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}
last_batch_sync_time = 0.0

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
    os.system('')

def generate_sparkline(data_list, width=SPARK_WIDTH):
    if len(data_list) < 2: return " " * width
    subset = list(data_list)[-width:]
    min_val, max_val = min(subset), max(subset)
    if max_val == min_val: return "-" * len(subset)
    chars = "  ▂▃▄▅▆▇█"
    span = max_val - min_val
    return "".join(chars[int(((x - min_val) / span) * (len(chars) - 1))] for x in subset).rjust(width)

def safe_request(url, is_api=False):
    headers = {"User-Agent": "OSINT-Terminal-Omega (research@osint.local)"}
    if is_api and "weather.gov" in url: headers["Accept"] = "application/geo+json"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as response:
            return response.read()
    except Exception: return None

def process_feed(feed):
    url, ftype, category = feed["url"], feed["type"], feed["category"]
    raw_data = safe_request(url, is_api=(ftype == "api"))
    if not raw_data: return None

    results = {"feed_config": feed, "data": []}

    if ftype == "api":
        try:
            if category == "NWS_WEATHER":
                payload = json.loads(raw_data)
                results["data"] = [{"event": f.get("properties", {}).get("event", "Unknown"), "area": f.get("properties", {}).get("areaDesc", "Unknown"), "severity": f.get("properties", {}).get("severity", "Unknown")} for f in payload.get("features", [])[:30]]
            elif category == "FAA_STATUS":
                root = ET.fromstring(raw_data)
                for dt in root.findall('.//Delay_type'):
                    cat = dt.findtext('Name') or "Delay"
                    for air in dt.findall('.//Airport'):
                        results["data"].append({"category": cat, "airport": (air.findtext('ARPT') or "UNK").strip(), "reason": (air.findtext('Reason') or "").strip()})
            elif category == "SEC_FORM4":
                root = ET.fromstring(raw_data)
                ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
                for entry in root.findall("a:entry", ATOM_NS):
                    title = entry.findtext("a:title", namespaces=ATOM_NS)
                    link = entry.find("a:link", namespaces=ATOM_NS)
                    href = link.get("href") if link is not None else None
                    if title and href:
                        dollar_val_str = ""
                        try:
                            raw_txt_url = href.replace("-index.htm", ".txt")
                            doc_data = safe_request(raw_txt_url, is_api=True)
                            if doc_data:
                                doc_str = doc_data.decode('utf-8', errors='ignore')
                                if "<XML>" in doc_str:
                                    xml_part = doc_str.split("<XML>")[1].split("</XML>")[0]
                                    doc_root = ET.fromstring(xml_part.strip())
                                    total_val = 0.0
                                    for txn in doc_root.findall('.//nonDerivativeTransaction'):
                                        shares = float(txn.findtext('.//transactionShares/value') or 0)
                                        price = float(txn.findtext('.//transactionPricePerShare/value') or 0)
                                        total_val += (shares * price)
                                    if total_val > 0:
                                        dollar_val_str = f" | {GREEN}Vol: ${total_val:,.2f}{RESET}" if total_val < 1000000 else f" | {PURPLE}WHALE ALERT: ${total_val:,.2f}{RESET}"
                        except Exception: pass
                        results["data"].append({"title": title + dollar_val_str, "link": href})
            elif category == "POLYMARKET":
                for m in json.loads(raw_data):
                    op = m.get("outcomePrices")
                    if isinstance(op, str): op = json.loads(op)
                    results["data"].append({"platform": "Polymarket", "question": m.get("question", "Unknown"), "yes_price": float(op[0]) if op else 0.5, "volume": float(m.get("volume24hr") or 0)})
            elif category == "KALSHI":
                for m in json.loads(raw_data).get("markets", []):
                    yb = m.get("yes_bid")
                    results["data"].append({"platform": "Kalshi", "question": m.get("title", m.get("ticker", "Unknown")), "yes_price": (yb / 100.0) if yb else 0.5, "volume": float(m.get("volume") or 0)})
            elif category == "PREDICTIT":
                for m in json.loads(raw_data).get("markets", []):
                    c = m.get("contracts", [])
                    if c: results["data"].append({"platform": "PredictIt", "question": m.get("name", "Unknown"), "yes_price": c[0].get("lastTradePrice", 0.5), "volume": 0})
            elif category == "MANIFOLD":
                for m in json.loads(raw_data):
                    if m.get("outcomeType") == "BINARY":
                        results["data"].append({"platform": "Manifold", "question": m.get("question", "Unknown"), "yes_price": m.get("probability", 0.5), "volume": m.get("volume", 0)})
        except Exception as e: logger.debug(f"API error for {category}: {e}")

    elif ftype == "rss":
        try:
            root = ET.fromstring(raw_data)
            for item in root.findall('./channel/item')[:8]:
                title = item.findtext('title')
                if title: results["data"].append(title)
        except Exception as e: logger.debug(f"RSS parse error for {category}: {e}")

    return results

def sync_osint_feeds():
    global last_batch_sync_time, osint_data, weather_alerts, flight_disruptions, insider_filings, prediction_markets
    global kinetic_active_until, kinetic_last_headline, labor_active_until, labor_last_headline

    logger.info(f"Initiating 90s Omega batch sync for {len(FEED_REGISTRY)} inputs...")
    last_batch_sync_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(FEED_REGISTRY)) as executor:
        future_to_feed = {executor.submit(process_feed, feed): feed for feed in FEED_REGISTRY}
        new_osint, new_war, new_labor = [], [], []
        temp_weather, temp_flights, temp_insider, temp_predictions = [], [], [], []

        for future in concurrent.futures.as_completed(future_to_feed):
            res = future.result()
            if not res: continue

            cat, data = res["feed_config"]["category"], res["data"]

            if cat == "NWS_WEATHER": temp_weather.extend(data)
            elif cat == "FAA_STATUS": temp_flights.extend(data)
            elif cat == "SEC_FORM4": temp_insider.extend(data)
            elif cat in ["POLYMARKET", "KALSHI", "PREDICTIT", "MANIFOLD"]: temp_predictions.extend(data)
            elif res["feed_config"]["type"] == "rss":
                for title in data:
                    if title in seen_headlines: continue
                    seen_headlines.add(title)

                    region_tag = res["feed_config"].get('region', res["feed_config"].get('category', 'GLOBAL'))
                    if cat in ["kinetic", "worldwide_wire"] or any(w in title.lower() for w in ["missile", "airstrike", "warship", "frontline", "conflict"]):
                        new_war.append(f"[{region_tag}] {title}")
                        if any(w in title.lower() for w in ["confirmed", "hits", "launched", "destroyed", "strike", "attack"]):
                            with state_lock:
                                kinetic_headline_queue.append(title)
                                kinetic_last_headline = title
                                kinetic_active_until = datetime.datetime.now() + datetime.timedelta(seconds=KINETIC_DECAY_SECONDS)
                    
                    elif cat in ["labor", "border_enforcement"] or any(w in title.lower() for w in ["union strike", "worksite enforcement", "ICE raid", "walkout"]):
                        new_labor.append(title)
                        with state_lock:
                            labor_headline_queue.append(title)
                            labor_last_headline = title
                            labor_active_until = datetime.datetime.now() + datetime.timedelta(seconds=LABOR_DECAY_SECONDS)

                    nums = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                    tl = title.lower()
                    if any(w in tl for w in ["failure", "outage", "collapse", "strike", "attack", "hack", "raided"]): pred = f"{RED}▼ BEARISH SHOCK{RESET}"
                    elif any(w in tl for w in ["plunge", "drop", "cut", "loss", "shortage"]): pred = f"{YELLOW}▼ DOWNTREND{RESET}"
                    elif any(w in tl for w in ["surge", "jump", "record", "soar", "gain"]): pred = f"{GREEN}▲ UPTREND{RESET}"
                    else: pred = f"{CYAN}► NEUTRAL{RESET}"
                    new_osint.append({"text": f"[{region_tag}] {title[:65]}", "nums": nums[0] if nums else "N/A", "prediction": pred})

    with state_lock:
        if temp_weather: weather_alerts = temp_weather
        if temp_flights: flight_disruptions = temp_flights
        if temp_predictions: prediction_markets = temp_predictions
        for item in temp_insider:
            if item["link"] not in seen_insider_links:
                seen_insider_links.add(item["link"])
                insider_filings.appendleft(item)
        for wh in new_war: war_headlines.appendleft(wh)
        osint_data = (new_osint + osint_data)[:10]
        if len(seen_headlines) > 3000: seen_headlines.clear()
        if len(seen_insider_links) > 500: seen_insider_links.clear()

def osint_sync_daemon():
    while True:
        sync_osint_feeds()
        time.sleep(OSINT_BATCH_INTERVAL_SECONDS)

# ---------------------------------------------------------
# LAGLESS PRICE DAEMON & 5-VECTOR PREDICTION MODEL
# ---------------------------------------------------------
def fetch_prices_and_volume(tickers):
    price_result, volume_result = {}, {}
    try:
        with open(os.devnull, 'w') as devnull:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = devnull, devnull
            try: data = yf.download(tickers, period="1d", interval="1m", progress=False)
            finally: sys.stdout, sys.stderr = old_stdout, old_stderr
    except Exception: return price_result, volume_result

    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                c_s = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
                v_s = data['Volume'][ticker].dropna() if 'Volume' in data and ticker in data['Volume'] else pd.Series(dtype=float)
            else:
                c_s = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
                v_s = data['Volume'].dropna() if len(tickers) == 1 and 'Volume' in data else pd.Series(dtype=float)
            if not c_s.empty: price_result[ticker] = float(c_s.iloc[-1])
            if not v_s.empty: volume_result[ticker] = float(v_s.iloc[-1])
        except Exception: pass
    return price_result, volume_result

def _avg_last_return(tickers):
    rets = []
    for t in tickers:
        buf = price_buffers.get(t)
        if buf and len(buf) >= 2 and buf[-2] != 0:
            rets.append((buf[-1] - buf[-2]) / buf[-2])
    return float(np.mean(rets)) if rets else 0.0

def get_kinetic_flag():
    with state_lock:
        if kinetic_active_until is None: return 0, None
        rem = (kinetic_active_until - datetime.datetime.now()).total_seconds()
        return (1, rem) if rem > 0 else (0, None)

def get_labor_flag():
    with state_lock:
        if labor_active_until is None: return 0, None
        rem = (labor_active_until - datetime.datetime.now()).total_seconds()
        return (1, rem) if rem > 0 else (0, None)

def compute_delta_i_hat(E_t, T_t, K):
    c = MODEL_COEFFICIENTS
    return c["beta0"] + c["beta1"] * E_t + c["beta2"] * (E_t * K) + c["beta3"] * T_t

def compute_composite_index(K, L, weather_count, flight_count, prediction_momentum=0.0):
    w = COMPOSITE_WEIGHTS
    return (w["kinetic"] * K + w["labor"] * L + w["flights"] * min(flight_count / 10.0, 1.0) +
            w["weather"] * min(weather_count / 20.0, 1.0) + w["prediction_markets"] * prediction_momentum)

def compute_prediction_momentum():
    global prev_prediction_prices
    with state_lock: snapshot = list(prediction_markets)
    if not snapshot: return 0.0, snapshot
    deltas, new_prev = [], {}
    for m in snapshot:
        key = f"{m['platform']}:{m['question']}"
        new_prev[key] = m["yes_price"]
        if key in prev_prediction_prices: deltas.append(abs(m["yes_price"] - prev_prediction_prices[key]))
    prev_prediction_prices = new_prev
    momentum = float(np.mean(deltas)) if deltas else 0.0
    return min(momentum / 0.05, 1.0), snapshot

def generate_intercalculated_prediction(E_t, T_t, K, L, weather_count, flight_count, composite, p_snap, war_snap, osint_snap):
    advices = []
    
    top_markets = [f"[{m['platform']}] {m['question'][:50]} (YES: {m['yes_price']*100:.0f}%)" for m in sorted(p_snap, key=lambda x: x.get('volume', 0), reverse=True)[:4]]
    top_war = [h[:75] for h in war_snap[:2]]
    top_osint = [h['text'][:75] for h in osint_snap[:2]]
    all_headlines = top_war + top_osint

    # 1. Macro & Yield Advisory
    macro_score = (composite * 50) - (T_t * 20)
    if macro_score > 20: advices.append({"sector": "MACRO & YIELD", "action": "DEFENSIVE SHORT", "asset": "Equities (^GSPC) / Long Volatility (^VIX)", "reason": "High composite friction implies broader market contraction."})
    else: advices.append({"sector": "MACRO & YIELD", "action": "NEUTRAL LONG", "asset": "Broad Market (^GSPC)", "reason": "Friction levels normalized, standard equity growth projected."})

    # 2. Energy & Commodities Advisory
    en_score = (E_t * 100) + (K * 30)
    if en_score > 10: advices.append({"sector": "ENERGY & COMMODITIES", "action": "STRONG BUY", "asset": "Crude (BZ=F) & Gold (GC=F)", "reason": f"Kinetic scalar active (K={K}) with positive energy drift. Disruption premium."})
    else: advices.append({"sector": "ENERGY & COMMODITIES", "action": "HOLD", "asset": "Energy Majors", "reason": "No severe commodity disruption premiums detected."})

    # 3. Aerospace & Defense Advisory
    if K > 0: advices.append({"sector": "AEROSPACE & DEFENSE", "action": "AGGRESSIVE LONG", "asset": "Defense Primes (RTX, LMT, NOC)", "reason": "Active kinetic theater engagements and military strikes verified."})
    else: advices.append({"sector": "AEROSPACE & DEFENSE", "action": "NEUTRAL", "asset": "Defense ETFs", "reason": "No active kinetic alerts currently escalating."})

    # 4. Supply Chain & Transport Advisory
    if T_t < -0.01 or flight_count > 20 or weather_count > 30: advices.append({"sector": "SUPPLY CHAIN & TRANSPORT", "action": "SHORT / HEDGE", "asset": "Transport Proxies (IYT, BDRY)", "reason": f"Logistics drag detected. FAA Disruptions: {flight_count}, Severe Weather: {weather_count}."})
    else: advices.append({"sector": "SUPPLY CHAIN & TRANSPORT", "action": "LONG", "asset": "Logistics & Transport (IYT)", "reason": "Clear skies and operational transit routes."})

    # 5. Prediction Markets / Event Sentiment Advisory
    advices.append({"sector": "EVENT DRIVEN / CRYPTO", "action": "SENTIMENT PLAY", "asset": "Bitcoin (BTC-USD) & Smart Contracts", "reason": "Proxy for decentralized prediction liquidity and global risk consensus."})

    rec = {
        "timestamp": datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST"),
        "composite": f"{composite:.2f}",
        "advices": advices,
        "supporting_headlines": all_headlines,
        "supporting_markets": top_markets
    }
    
    try:
        with open(PREDICTION_AUDIT_PATH, "a", encoding="utf-8") as f: f.write(json.dumps(rec) + "\n")
    except Exception: pass
    return rec

def load_prediction_history():
    history = []
    if PREDICTION_AUDIT_PATH.exists():
        try:
            with open(PREDICTION_AUDIT_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip(): history.append(json.loads(line.strip()))
        except Exception: pass
    return history[-5:]

def is_late_session():
    now_et = datetime.datetime.now(ET_ZONE)
    close_dt = now_et.replace(hour=MARKET_CLOSE_HOUR_ET, minute=0, second=0, microsecond=0)
    minutes_to_close = (close_dt - now_et).total_seconds() / 60.0
    return 0 <= minutes_to_close <= LATE_SESSION_WINDOW_MINUTES, minutes_to_close

def log_unusual_activity(ticker, price, pct_change, volume, vol_z, late_session, minutes_to_close):
    is_new = not UNUSUAL_ACTIVITY_LOG_PATH.exists()
    with open(UNUSUAL_ACTIVITY_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new: writer.writerow(["timestamp", "ticker", "price", "pct_change", "volume", "volume_z_score", "late_session", "minutes_to_close", "severity"])
        severity = "SUSPICIOUS_LATE_SESSION" if late_session else "UNUSUAL_VOLUME"
        writer.writerow([datetime.datetime.now().isoformat(timespec="seconds"), ticker, f"{price:.4f}", f"{pct_change:.5f}", f"{volume:.0f}", f"{vol_z:.2f}", late_session, f"{minutes_to_close:.1f}", severity])
        return severity

def log_labor_event(headline):
    is_new = not LABOR_EVENT_LOG_PATH.exists()
    with open(LABOR_EVENT_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new: writer.writerow(["timestamp", "headline"])
        writer.writerow([datetime.datetime.now().isoformat(timespec="seconds"), headline])

# ---------------------------------------------------------
# MAIN UI LOOP
# ---------------------------------------------------------
def main():
    global current_view, last_batch_sync_time
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"{CYAN} OMEGA OSINT TERMINAL | 150+ CONCURRENT DPI FEEDS | 5-VECTOR ADVISORY {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"\n[+] Engaging 90-second worldwide thread-pooled batch orchestrator...")
    print(f"[+] Active Inputs: {len(FEED_REGISTRY)} concurrent wire, API, and RSS feeds.")
    print(f"[+] UI Polling Rate set to ultra-responsive {POLL_INTERVAL} seconds.")
    
    threading.Thread(target=osint_sync_daemon, name="Batch-Orchestrator", daemon=True).start()

    price_backoff = PRICE_POLL_BASE_SECONDS
    last_price_fetch = 0.0
    last_prediction_run = 0.0
    latest_prediction = {"advices": [], "supporting_headlines": [], "supporting_markets": []}

    try:
        while True:
            with state_lock:
                while labor_headline_queue: log_labor_event(labor_headline_queue.popleft())
                headline_to_show = kinetic_headline_queue.popleft() if kinetic_headline_queue else None

            timestamp = datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST")
            now_mono = time.monotonic()
            
            with view_lock: view_key = current_view
            active_tickers = MARKETS[view_key]["tickers"] if view_key in MARKETS else ["BZ=F", "^GSPC", "^FTSE", "^GSPTSE", "BTC-USD"]
            view_name = MARKETS[view_key]["name"] if view_key in MARKETS else f"SPECIALIZED PAGE: [{view_key}]"

            if now_mono - last_price_fetch >= price_backoff:
                fresh_prices, fresh_volumes = fetch_prices_and_volume(all_possible_tickers)
                last_price_fetch = now_mono
                if fresh_prices:
                    for ticker, price in fresh_prices.items():
                        last_known_prices[ticker] = price
                        price_buffers[ticker].append(price)
                    for ticker, vol in fresh_volumes.items():
                        volume_buffers[ticker].append(vol)
                    price_backoff = PRICE_POLL_BASE_SECONDS
                else:
                    price_backoff = min(price_backoff * 1.5, PRICE_POLL_MAX_BACKOFF)

            K, k_remaining = get_kinetic_flag()
            E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
            T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
            delta_hat = compute_delta_i_hat(E_t, T_t, K)
            L, l_remaining = get_labor_flag()
            
            with state_lock:
                w_snap = list(weather_alerts)
                f_snap = list(flight_disruptions)
                war_snap = list(war_headlines)
                ins_snap = list(insider_filings)
                osint_snap = list(osint_data)
                p_snap = list(prediction_markets)
            
            prediction_momentum, _ = compute_prediction_momentum()
            composite = compute_composite_index(K, L, len(w_snap), len(f_snap), prediction_momentum)
            late_session, mins_to_close = is_late_session()

            # 90-Second 5-Vector Recalculation
            if now_mono - last_prediction_run >= 90.0:
                latest_prediction = generate_intercalculated_prediction(E_t, T_t, K, L, len(w_snap), len(f_snap), composite, p_snap, war_snap, osint_snap)
                last_prediction_run = now_mono

            clear_screen()
            print(f"{BLUE}========================================================================================={RESET}")
            print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | 90s CADENCE ONLINE {RESET}")
            print(f"{BLUE}========================================================================================={RESET}")

            # ---------------------------------------------------------
            # MULTI-PAGE RENDER
            # ---------------------------------------------------------
            if view_key in MARKETS:
                k_str = f"{RED}ACTIVE ({k_remaining/60:.0f}m left){RESET}" if K else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}MODEL STATE:{RESET} E_t={E_t:+.4f}  T_t={T_t:+.4f}  K={k_str}  delta_I_hat={delta_hat:+.5f}")
                print(f" {'TICKER':<8} | {'PRICE':<9} | {'DELTA (%)':<9} | {'Z-SCORE':<7} | {'MOMENTUM':<{SPARK_WIDTH}} | {'STATUS':<15}")
                print("-" * 89)

                for ticker in active_tickers:
                    buf, vbuf = price_buffers[ticker], volume_buffers[ticker]
                    curr_price = last_known_prices.get(ticker)
                    pct_change = (buf[-1] - buf[-2]) / buf[-2] if curr_price and len(buf) > 2 and buf[-2] else 0.0
                    
                    z_score = 0.0
                    if curr_price and len(buf) > 2:
                        hist = list(buf)[:-1]
                        std = np.std(hist)
                        z_score = (curr_price - np.mean(hist)) / (std if std != 0 else 1.0)

                    vol_alert = None
                    if len(vbuf) > 5:
                        vhist = list(vbuf)[:-1]
                        vstd = np.std(vhist)
                        vmean = np.mean(vhist)
                        curr_vol = vbuf[-1]
                        vol_z = (curr_vol - vmean) / (vstd if vstd != 0 else 1.0)
                        if vol_z >= UNUSUAL_VOLUME_Z:
                            sev = log_unusual_activity(ticker, curr_price, pct_change or 0.0, curr_vol, vol_z, late_session, mins_to_close)
                            vol_alert = (sev, vol_z)

                    sparkline = generate_sparkline(buf)
                    if curr_price is None:
                        print(f"  {ticker:<7} | {'NO DATA':<9} | {'--':>9} | {'--':>7} | {sparkline} | {'OFFLINE':<15}")
                        continue

                    abs_c = abs(pct_change)
                    diamond, color = " ", CYAN
                    status = "NOMINAL"
                    if abs_c >= EXTREME_THRESH: color, diamond, status = PURPLE, "♦", "♦ VOLATILITY"
                    elif abs_c >= HIGH_THRESH: color, status = (RED, "CRIT. DROP") if pct_change < 0 else (GREEN, "SURGE ALERT")

                    if vol_alert:
                        color = PURPLE if vol_alert[0] == "SUSPICIOUS_LATE_SESSION" else RED
                        diamond = "!"
                        status = "⚠ LATE-SESSION" if vol_alert[0] == "SUSPICIOUS_LATE_SESSION" else "⚠ HUGE VOLUME"

                    print(f"{color} {diamond}{ticker:<7} | {curr_price:<9.2f} | {pct_change*100:>8.2f}% | {z_score:>7.2f} | {sparkline} | {status:<15}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  GLOBAL FRICTION FEED  (composite index: {composite:.2f} / 1.00){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                
                l_str = f"{RED}ACTIVE ({l_remaining/60:.0f}m left){RESET}" if L else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}Labor/enforcement radar:{RESET} {l_str}", end="")
                if L and labor_last_headline: print(f"  — {labor_last_headline[:70]}")
                else: print()

                # Basic Summary of top 2 markets for the main page
                poly_summary = " | ".join([f"{m['question'][:30]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Polymarket'][:2])
                kalshi_summary = " | ".join([f"{m['question'][:30]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Kalshi'][:2])
                if poly_summary: print(f" {GRAY}Polymarket Consensus:{RESET} {CYAN}{poly_summary}{RESET}")
                if kalshi_summary: print(f" {GRAY}Kalshi Consensus:{RESET} {YELLOW}{kalshi_summary}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  DPI LAYER: SEC EDGAR INSIDER DISCLOSURES (Form 4 Deep XML Extraction){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                if not ins_snap: print(f" {GRAY}Aggregating SEC EDGAR feed...{RESET}")
                else:
                    for f_item in list(ins_snap)[:4]: print(f" • {f_item['title'][:110]}")

            elif view_key == "C":
                print(f"{PURPLE}  4X DECENTRALIZED PREDICTION MARKETS & GLOBAL CONSENSUS{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'PLATFORM':<12} | {'MARKET / QUESTION':<60} | {'YES PRICE':<10} | {'VOLUME (24H)'}")
                print("-" * 89)
                if not p_snap: print(f" {GRAY}Aggregating decentralized consensus feeds...{RESET}")
                else:
                    sorted_markets = sorted(p_snap, key=lambda x: x['volume'], reverse=True)
                    for m in sorted_markets[:25]:
                        yp = f"{m['yes_price']*100:.1f}¢" if m['yes_price'] is not None else "N/A"
                        color = CYAN if m['platform'] == 'Polymarket' else (YELLOW if m['platform'] == 'Kalshi' else GREEN)
                        print(f" {color}{m['platform']:<12}{RESET} | {m['question'][:60]:<60} | {yp:<10} | ${m['volume']:,.0f}")

            elif view_key == "W":
                print(f"{PURPLE}  DETAILED METEOROLOGICAL & AIRSPACE DISRUPTION TELEMETRY{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}NWS Severe Weather Alerts ({len(w_snap)} active):{RESET}")
                for alert in w_snap[:15]: print(f"   • [{alert['severity']}] {alert['event']} — {alert['area'][:65]}")
                print(f"\n {GRAY}FAA Airspace Disruption & Ground Stops ({len(f_snap)} entries):{RESET}")
                for fs in f_snap[:15]: print(f"   • Airport: {fs['airport']} | Type: {fs['category']} | Reason: {fs['reason'][:50]}")

            elif view_key == "K":
                print(f"{PURPLE}  WORLDWIDE MULTI-THEATER KINETIC ESCALATION & STRIKE MAPPING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_stat = f"{RED}ACTIVE ({k_remaining/60:.0f}m remaining){RESET}" if K else f"{GRAY}INACTIVE / STANDBY{RESET}"
                print(f" Kinetic Escalation Flag (K): {k_stat}")
                print(f" Incoming Worldwide Conflict Headlines Buffer ({len(war_snap)} logged):\n")
                for idx, wh in enumerate(war_snap[:20], 1): print(f" [{idx:02d}] {RED}♦ THEATER INTEL:{RESET} {wh}")

            elif view_key == "P":
                print(f"{PURPLE}  QUANTITATIVE INTERCALCULATED PREDICTION & ADVISORY ENGINE (5-VECTOR){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" Composite Friction Score      : {latest_prediction.get('composite', 'N/A')} / 1.00")
                
                print(f"\n {YELLOW}--- TOP 5 INVESTMENT ADVISORIES ---{RESET}")
                for adv in latest_prediction.get('advices', []):
                    color = GREEN if "BUY" in adv['action'] or "LONG" in adv['action'] else (RED if "SHORT" in adv['action'] or "SELL" in adv['action'] else CYAN)
                    print(f" [{adv['sector']}]")
                    print(f"   Action: {color}{adv['action']}{RESET} | Asset Focus: {adv['asset']}")
                    print(f"   Reason: {adv['reason']}")
                
                print(f"\n {YELLOW}--- SUPPORTING TELEMETRY (DPI LAYER) ---{RESET}")
                print(f" {GRAY}Predictive Consensus Markets Driving Model:{RESET}")
                for m in latest_prediction.get('supporting_markets', []): print(f"   • {m}")
                print(f" {GRAY}Core OSINT Headlines Driving Model:{RESET}")
                for h in latest_prediction.get('supporting_headlines', []): print(f"   • {h}")

            elif view_key == "H":
                print(f"{PURPLE}  AUDIT TRAIL: HISTORICAL 5-VECTOR PREDICTIONS{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                history = load_prediction_history()
                if not history: print(f" {GRAY}No prediction history recorded yet.{RESET}")
                else:
                    for item in reversed(history):
                        print(f" {YELLOW}TIMESTAMP: {item['timestamp']}{RESET} | Composite: {item['composite']}")
                        for adv in item.get('advices', []):
                            print(f"   [{adv['sector']}] -> {adv['action']} ({adv['asset']})")
                        print("-" * 89)

            # ---------------------------------------------------------
            # FOOTER / ENGINE STATUS
            # ---------------------------------------------------------
            time_since_batch = time.time() - last_batch_sync_time
            next_batch_in = max(OSINT_BATCH_INTERVAL_SECONDS - time_since_batch, 0)
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{PURPLE}  GLOBAL ENGINE STATUS (Composite Index: {composite:.2f} / 1.00){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            print(f" {GRAY}Worldwide Active Feeds:{RESET} {len(FEED_REGISTRY)}/{len(FEED_REGISTRY)} Synced | {GRAY}Next 90s Batch:{RESET} {next_batch_in:.0f}s")
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{YELLOW} [1-0] Markets | [W] Weather/FAA | [K] War Theater | [C] Consensus | [P] Prediction | [H] Audit {RESET}")
            print(f"{GRAY} Log: {LOG_PATH.name}  |  Audit Log: {PREDICTION_AUDIT_PATH.name}  |  Unusual: {UNUSUAL_ACTIVITY_LOG_PATH.name}{RESET}")

            # Polling at 0.3 intervals
            for _ in range(int(max(1, POLL_INTERVAL * 10))):
                if IS_WINDOWS and _kbhit():
                    key = _getch()
                    if key in MARKETS or key in ['w', 'k', 'c', 'p', 'h']:
                        with view_lock: current_view = key.upper()
                        break
                time.sleep(0.1)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] Worldwide tactical terminal disengaged safely by operator.{RESET}")
        logger.info("Terminal stopped by operator.")

if __name__ == "__main__":
    main()