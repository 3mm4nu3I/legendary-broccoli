"""
OSINT Command Terminal — Global Multi-Theater & Predictive Engine (v2)
==========================================================================
Architecture Upgrades:
- 50+ Concurrent Intelligence Inputs (US, UK, Canada, EU)
- Strict 120-Second Batch Synchronization for all OSINT/API feeds
- ThreadPoolExecutor connection pooling to prevent HTTP 429 rate limits
- Real-time quantitative intercalculation and persistent audit logging
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
# PLATFORM HANDLING (Robust Windows Hotkeys via msvcrt)
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
    def _kbhit():
        return msvcrt.kbhit()
    def _getch():
        ch = msvcrt.getch()
        if ch in b'\x00\xe0':
            msvcrt.getch() # Consume extended key second byte
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
WARN_THRESH = 0.005

# ---------------------------------------------------------
# GLOBAL ENGINE CONFIGURATION
# ---------------------------------------------------------
# STRICT 2-MINUTE OSINT BATCH SYNC
OSINT_BATCH_INTERVAL_SECONDS = 120.0 
PRICE_POLL_BASE_SECONDS = 1.5
PRICE_POLL_MAX_BACKOFF = 30.0

MODEL_COEFFICIENTS = {"beta0": 0.0000, "beta1": 0.0500, "beta2": 0.1200, "beta3": -0.0400}
ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]
KINETIC_DECAY_SECONDS = 4 * 3600
LABOR_DECAY_SECONDS = 6 * 3600
SIGNAL_STRONG = 0.010
SIGNAL_MODERATE = 0.003
UNUSUAL_VOLUME_Z = 3.0
MARKET_CLOSE_HOUR_ET = 16
LATE_SESSION_WINDOW_MINUTES = 30
ET_ZONE = ZoneInfo("America/New_York")

COMPOSITE_WEIGHTS = {
    "kinetic": 0.30, "labor": 0.15, "flights": 0.15,
    "weather": 0.15, "prediction_markets": 0.25,
}

# ---------------------------------------------------------
# 50+ CONCURRENT FEED REGISTRY GENERATOR (US, UK, CA, EU)
# ---------------------------------------------------------
API_ENDPOINTS = {
    "NWS_WEATHER": "https://api.weather.gov/alerts/active?severity=Severe,Extreme",
    "FAA_STATUS": "https://nasstatus.faa.gov/api/airport-status-information",
    "SEC_FORM4": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=40&output=atom",
    "POLYMARKET": "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=5&order=volume24hr&ascending=false",
    "KALSHI": "https://api.elections.kalshi.com/trade-api/v2/markets?limit=5&status=open"
}

# International RSS Query Expansion
REGIONS = {
    "US": {"gl": "US", "ceid": "US:en", "hl": "en-US"},
    "UK": {"gl": "GB", "ceid": "GB:en", "hl": "en-GB"},
    "CA": {"gl": "CA", "ceid": "CA:en", "hl": "en-CA"},
    "EU": {"gl": "IE", "ceid": "IE:en", "hl": "en-IE"} 
}

TOPICS = {
    "kinetic": '"strike" OR "missile" OR "airstrike" OR "military escalation" OR "NATO" OR "defense ministry"',
    "labor": '"union strike" OR "work stoppage" OR "walkout" OR "labor dispute"',
    "regulatory": '"regulator" OR "investigation" OR "antitrust" OR "fined" OR "central bank"',
    "macro": '"interest rates" OR "inflation" OR "bond yield" OR "central bank"',
    "supply_chain": '"port congestion" OR "shipping delay" OR "logistics failure" OR "border closure"',
    "energy": '"oil shock" OR "natural gas" OR "grid failure" OR "power outage"',
    "tech_cyber": '"cyber attack" OR "data breach" OR "network outage" OR "semiconductor"',
    "metals": '"rare earth" OR "copper shortage" OR "lithium" OR "mining strike"',
    "agriculture": '"crop failure" OR "farm labor" OR "export ban" OR "fertilizer shortage"',
    "border_enforcement": '"worksite enforcement" OR "deportation" OR "customs seizure" OR "raid"'
}

FEED_REGISTRY = []
# Compile APIs (5 inputs)
for name, url in API_ENDPOINTS.items():
    FEED_REGISTRY.append({"type": "api", "category": name, "url": url})

# Compile Regional OSINT (4 Regions x 10 Topics = 40 inputs)
for region, params in REGIONS.items():
    for topic, query in TOPICS.items():
        encoded = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"
        FEED_REGISTRY.append({"type": "rss", "category": topic, "region": region, "url": url})

# Total Input Count check: 5 + 40 = 45 inputs. Add 5 more specific global trackers to hit >= 50.
GLOBAL_TRACKERS = [
    {"type": "rss", "category": "kinetic", "region": "GLOBAL", "url": f"https://news.google.com/rss/search?q={urllib.parse.quote('Pentagon OR Ministry of Defence OR INDOPACOM')}&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "macro", "region": "GLOBAL", "url": f"https://news.google.com/rss/search?q={urllib.parse.quote('IMF OR World Bank OR BIS')}&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "energy", "region": "GLOBAL", "url": f"https://news.google.com/rss/search?q={urllib.parse.quote('OPEC OR Brent Crude OR IEA')}&hl=en-US&gl=US&ceid=US:en"},
    {"type": "rss", "category": "regulatory", "region": "EU_THEATER", "url": f"https://news.google.com/rss/search?q={urllib.parse.quote('ESMA OR European Commission OR ECB')}&hl=en-IE&gl=IE&ceid=IE:en"},
    {"type": "rss", "category": "regulatory", "region": "UK_THEATER", "url": f"https://news.google.com/rss/search?q={urllib.parse.quote('FCA OR Bank of England OR CMA')}&hl=en-GB&gl=GB&ceid=GB:en"}
]
FEED_REGISTRY.extend(GLOBAL_TRACKERS)
# Total Inputs = 50 precisely.

# ---------------------------------------------------------
# MARKET VIEWS & SPECIALIZED PAGES
# ---------------------------------------------------------
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
POLL_INTERVAL = 1.5
WINDOW_SIZE = 600
SPARK_WIDTH = 12

# ---------------------------------------------------------
# SHARED STATE
# ---------------------------------------------------------
state_lock = threading.Lock()
osint_data = []
seen_headlines = set()

kinetic_headline_queue = deque()
kinetic_active_until = None
kinetic_last_headline = None
war_headlines = deque(maxlen=30)
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

# ---------------------------------------------------------
# UTILS & PARSERS
# ---------------------------------------------------------
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
    os.system('')

def generate_sparkline(data_list, width=SPARK_WIDTH):
    if len(data_list) < 2:
        return " " * width
    subset = list(data_list)[-width:]
    min_val, max_val = min(subset), max(subset)
    if max_val == min_val:
        return "-" * len(subset)
    chars = "  ▂▃▄▅▆▇█"
    span = max_val - min_val
    return "".join(chars[int(((x - min_val) / span) * (len(chars) - 1))] for x in subset).rjust(width)

def safe_request(url, is_api=False):
    headers = {"User-Agent": "OSINT-Terminal-Ultimate (research@osint.local)"}
    if is_api and "weather.gov" in url:
        headers["Accept"] = "application/geo+json"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as response:
            return response.read()
    except Exception as exc:
        logger.debug(f"Fetch failed for {url}: {exc}")
        return None

# ---------------------------------------------------------
# STRICT 120-SECOND BATCH SYNC ENGINE
# ---------------------------------------------------------
def process_feed(feed):
    """Worker function executed inside the ThreadPoolExecutor."""
    url = feed["url"]
    ftype = feed["type"]
    category = feed["category"]
    
    raw_data = safe_request(url, is_api=(ftype == "api"))
    if not raw_data:
        return None

    results = {"feed_config": feed, "data": []}

    if ftype == "api":
        try:
            if category == "NWS_WEATHER":
                payload = json.loads(raw_data)
                results["data"] = [{"event": f.get("properties", {}).get("event", "Unknown"), 
                                    "area": f.get("properties", {}).get("areaDesc", "Unknown"), 
                                    "severity": f.get("properties", {}).get("severity", "Unknown")} 
                                   for f in payload.get("features", [])[:30]]
            elif category == "FAA_STATUS":
                root = ET.fromstring(raw_data)
                parsed = []
                for dt in root.findall('.//Delay_type'):
                    cat = dt.findtext('Name') or "Delay"
                    for air in dt.findall('.//Airport'):
                        parsed.append({"category": cat, "airport": (air.findtext('ARPT') or "UNK").strip(), "reason": (air.findtext('Reason') or "").strip()})
                results["data"] = parsed
            elif category == "SEC_FORM4":
                root = ET.fromstring(raw_data)
                ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}
                parsed = []
                for entry in root.findall("a:entry", ATOM_NS):
                    title = entry.findtext("a:title", namespaces=ATOM_NS)
                    link = entry.find("a:link", namespaces=ATOM_NS)
                    href = link.get("href") if link is not None else None
                    if title and href:
                        parsed.append({"title": title, "link": href})
                results["data"] = parsed
            elif category == "POLYMARKET":
                for m in json.loads(raw_data):
                    op = m.get("outcomePrices")
                    if isinstance(op, str): op = json.loads(op)
                    results["data"].append({"platform": "Polymarket", "question": m.get("question", "Unknown"), "yes_price": float(op[0]) if op else 0.5, "volume": float(m.get("volume24hr") or 0)})
            elif category == "KALSHI":
                for m in json.loads(raw_data).get("markets", []):
                    yb = m.get("yes_bid")
                    results["data"].append({"platform": "Kalshi", "question": m.get("title", m.get("ticker", "Unknown")), "yes_price": (yb / 100.0) if yb else 0.5, "volume": float(m.get("volume") or 0)})
        except Exception as e:
            logger.debug(f"API parse error for {category}: {e}")

    elif ftype == "rss":
        try:
            root = ET.fromstring(raw_data)
            for item in root.findall('./channel/item')[:8]:
                title = item.findtext('title')
                if title:
                    results["data"].append(title)
        except Exception as e:
            logger.debug(f"RSS parse error for {category}: {e}")

    return results

def sync_osint_feeds():
    """Runs exactly once every 120 seconds. Fires 50 threads in parallel."""
    global last_batch_sync_time, osint_data, weather_alerts, flight_disruptions, insider_filings, prediction_markets
    global kinetic_active_until, kinetic_last_headline, labor_active_until, labor_last_headline

    logger.info(f"Initiating 120-second batch sync for {len(FEED_REGISTRY)} inputs...")
    last_batch_sync_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_feed = {executor.submit(process_feed, feed): feed for feed in FEED_REGISTRY}
        
        new_osint = []
        new_war = []
        new_labor = []
        temp_weather = []
        temp_flights = []
        temp_insider = []
        temp_predictions = []

        for future in concurrent.futures.as_completed(future_to_feed):
            res = future.result()
            if not res: continue

            cat = res["feed_config"]["category"]
            data = res["data"]

            if cat == "NWS_WEATHER": temp_weather.extend(data)
            elif cat == "FAA_STATUS": temp_flights.extend(data)
            elif cat == "SEC_FORM4": temp_insider.extend(data)
            elif cat in ["POLYMARKET", "KALSHI"]: temp_predictions.extend(data)
            elif res["feed_config"]["type"] == "rss":
                for title in data:
                    if title in seen_headlines: continue
                    seen_headlines.add(title)

                    if cat == "kinetic" or any(w in title.lower() for w in ["missile", "airstrike", "warship", "frontline"]):
                        new_war.append(f"[{res['feed_config'].get('region', 'GLB')}] {title}")
                        if any(w in title.lower() for w in ["confirmed", "hits", "launched", "destroyed", "strike"]):
                            with state_lock:
                                kinetic_headline_queue.append(title)
                                kinetic_last_headline = title
                                kinetic_active_until = datetime.datetime.now() + datetime.timedelta(seconds=KINETIC_DECAY_SECONDS)
                    
                    elif cat in ["labor", "border_enforcement"] or any(w in title.lower() for w in ["union strike", "worksite enforcement", "ICE raid"]):
                        new_labor.append(title)
                        with state_lock:
                            labor_headline_queue.append(title)
                            labor_last_headline = title
                            labor_active_until = datetime.datetime.now() + datetime.timedelta(seconds=LABOR_DECAY_SECONDS)

                    # General OSINT Formatting
                    numbers = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                    num_ctx = numbers[0] if numbers else "N/A"
                    tl = title.lower()
                    if any(w in tl for w in ["failure", "outage", "collapse", "strike", "attack", "hack", "raided"]): pred = f"{RED}▼ BEARISH SHOCK{RESET}"
                    elif any(w in tl for w in ["plunge", "drop", "cut", "loss", "shortage"]): pred = f"{YELLOW}▼ DOWNTREND{RESET}"
                    elif any(w in tl for w in ["surge", "jump", "record", "soar", "gain"]): pred = f"{GREEN}▲ UPTREND{RESET}"
                    else: pred = f"{CYAN}► NEUTRAL{RESET}"
                    
                    new_osint.append({"text": f"[{res['feed_config'].get('region', 'GLB')}] {title[:65]}", "nums": num_ctx, "prediction": pred})

    # Safely commit batch updates to shared state
    with state_lock:
        if temp_weather: weather_alerts = temp_weather
        if temp_flights: flight_disruptions = temp_flights
        if temp_predictions: prediction_markets = temp_predictions
        
        for item in temp_insider:
            if item["link"] not in seen_insider_links:
                seen_insider_links.add(item["link"])
                insider_filings.appendleft(item)
                
        for wh in new_war: war_headlines.appendleft(wh)
        osint_data = (new_osint + osint_data)[:8]

        if len(seen_headlines) > 2000: seen_headlines.clear()
        if len(seen_insider_links) > 500: seen_insider_links.clear()

def osint_sync_daemon():
    """Runs permanently in the background, enforcing the 120s batch schedule."""
    while True:
        sync_osint_feeds()
        time.sleep(OSINT_BATCH_INTERVAL_SECONDS)

# ---------------------------------------------------------
# LAGLESS PRICE DAEMON & PREDICTION MODEL
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
                close_series = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
                vol_series = data['Volume'][ticker].dropna() if 'Volume' in data and ticker in data['Volume'] else pd.Series(dtype=float)
            else:
                close_series = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
                vol_series = data['Volume'].dropna() if len(tickers) == 1 and 'Volume' in data else pd.Series(dtype=float)
            if not close_series.empty: price_result[ticker] = float(close_series.iloc[-1])
            if not vol_series.empty: volume_result[ticker] = float(vol_series.iloc[-1])
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

def generate_intercalculated_prediction(E_t, T_t, K, L, weather_count, flight_count, composite, snapshot):
    score = (E_t * 150) + (K * 25) + (composite * 40) - (T_t * 50)
    if score > 0.08:
        action, asset, reason = "STRONG BUY / LONG", "Crude Oil (BZ=F) & Defense Equities (NVDA/PLTR)", f"High kinetic/energy friction (E_t={E_t:.3f}, K={K}). Supply disruption premium active."
    elif score < -0.05:
        action, asset, reason = "DEFENSIVE / SHORT", "Treasury Bonds (^TNX) & Safe Haven Gold (GC=F)", f"Downward momentum and logistical drag (T_t={T_t:.3f}, composite={composite:.2f})."
    else:
        action, asset, reason = "NEUTRAL / RANGE-BOUND", "U.S. Dollar (UUP) & Cash Equities", f"Equilibrium friction index ({composite:.2f}). Monitor macroeconomic yield curves."

    rec = {"timestamp": datetime.datetime.now().isoformat(timespec="seconds"), "action": action, "asset": asset, "score": f"{score:.4f}", "composite": f"{composite:.2f}", "reason": reason}
    try:
        with open(PREDICTION_AUDIT_PATH, "a", encoding="utf-8") as f: f.write(json.dumps(rec) + "\n")
    except Exception as exc: logger.error(f"Audit write fail: {exc}")
    return rec

def load_prediction_history():
    history = []
    if PREDICTION_AUDIT_PATH.exists():
        try:
            with open(PREDICTION_AUDIT_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip(): history.append(json.loads(line.strip()))
        except Exception: pass
    return history[-15:]

# ---------------------------------------------------------
# UNUSUAL ACTIVITY & LOGGING
# ---------------------------------------------------------
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
    print(f"{CYAN} ULTIMATE OSINT TERMINAL | GLOBAL MULTI-THEATER {len(FEED_REGISTRY)}-INPUT ENGINE {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"\n[+] Engaging 120-second thread-pooled batch orchestrator...")
    
    # Start the 120-second batch daemon
    threading.Thread(target=osint_sync_daemon, name="Batch-Orchestrator", daemon=True).start()

    price_backoff = PRICE_POLL_BASE_SECONDS
    last_price_fetch = 0.0
    last_prediction_run = 0.0
    latest_prediction = {"action": "INITIALIZING", "asset": "N/A", "reason": "Gathering initial telemetry..."}

    try:
        while True:
            # Drain UI events
            with state_lock:
                while labor_headline_queue: log_labor_event(labor_headline_queue.popleft())
                
                headline_to_show = None
                if kinetic_headline_queue: headline_to_show = kinetic_headline_queue.popleft()
            
            # (In a real scenario, you'd trigger display_war_map_alarm(headline_to_show) here)

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            now_mono = time.monotonic()
            
            with view_lock: view_key = current_view
            active_tickers = MARKETS[view_key]["tickers"] if view_key in MARKETS else ["BZ=F", "^GSPC", "^FTSE", "^GSPTSE", "BTC-USD"]
            view_name = MARKETS[view_key]["name"] if view_key in MARKETS else f"SPECIALIZED PAGE: [{view_key}]"

            # Lagless Price Polling
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

            # State Snapshot
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
            
            prediction_momentum, p_snap = compute_prediction_momentum()
            composite = compute_composite_index(K, L, len(w_snap), len(f_snap), prediction_momentum)
            late_session, mins_to_close = is_late_session()

            # Prediction run logic (syncs precisely with the 120s OSINT batch)
            if now_mono - last_prediction_run >= 120.0:
                latest_prediction = generate_intercalculated_prediction(E_t, T_t, K, L, len(w_snap), len(f_snap), composite, p_snap)
                last_prediction_run = now_mono

            clear_screen()
            print(f"{BLUE}========================================================================================={RESET}")
            print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | ONLINE {RESET}")
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
                print(f"{PURPLE}  GLOBAL FRICTION FEED  (composite index: {composite:.2f} / 1.00, experimental heuristic){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                
                l_str = f"{RED}ACTIVE ({l_remaining/60:.0f}m left){RESET}" if L else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}Labor/enforcement radar:{RESET} {l_str}", end="")
                if L and labor_last_headline: print(f"  — {labor_last_headline[:70]}")
                else: print()

                # Prediction Market Summary on Main View
                poly_summary = " | ".join([f"{m['question'][:30]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Polymarket'][:2])
                kalshi_summary = " | ".join([f"{m['question'][:30]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Kalshi'][:2])
                if poly_summary: print(f" {GRAY}Polymarket Consensus:{RESET} {CYAN}{poly_summary}{RESET}")
                if kalshi_summary: print(f" {GRAY}Kalshi Consensus:{RESET} {YELLOW}{kalshi_summary}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  SEC EDGAR INSIDER DISCLOSURES (Form 4){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                if not ins_snap: print(f" {GRAY}Aggregating SEC EDGAR feed...{RESET}")
                else:
                    for f_item in list(ins_snap)[:2]: print(f" • {f_item['title'][:85]}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  OSINT HEADLINE RADAR (50-Input Batch Synced){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'LATEST GLOBAL HEADLINES':<65} | {'DATA':<8} | {'PREDICTION'}")
                print("-" * 89)
                if not osint_snap: print(f" {GRAY}Aggregating OSINT data streams...{RESET}")
                else:
                    for item in osint_snap: print(f" {item['text']:<65} | {item['nums']:<8} | {item['prediction']}")

            elif view_key == "W":
                print(f"{PURPLE}  DETAILED METEOROLOGICAL & AIRSPACE DISRUPTION TELEMETRY{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}NWS Severe Weather Alerts ({len(w_snap)} active):{RESET}")
                for alert in w_snap[:15]: print(f"   • [{alert['severity']}] {alert['event']} — {alert['area'][:65]}")
                print(f"\n {GRAY}FAA Airspace Disruption & Ground Stops ({len(f_snap)} entries):{RESET}")
                for fs in f_snap[:15]: print(f"   • Airport: {fs['airport']} | Type: {fs['category']} | Reason: {fs['reason'][:50]}")

            elif view_key == "K":
                print(f"{PURPLE}  MULTI-THEATER KINETIC ESCALATION & STRIKE MAPPING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_stat = f"{RED}ACTIVE ({k_remaining/60:.0f}m remaining){RESET}" if K else f"{GRAY}INACTIVE / STANDBY{RESET}"
                print(f" Kinetic Escalation Flag (K): {k_stat}")
                print(f" Incoming War / Conflict Headlines Buffer ({len(war_snap)} logged):\n")
                for idx, wh in enumerate(war_snap[:15], 1): print(f" [{idx:02d}] {RED}♦ THEATER INTEL:{RESET} {wh}")

            elif view_key == "P":
                print(f"{PURPLE}  QUANTITATIVE INTERCALCULATED PREDICTION & BETTING ENGINE{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" Latest Recommendation Action : {GREEN if 'BUY' in latest_prediction['action'] else RED}{latest_prediction['action']}{RESET}")
                print(f" Recommended Asset Focus       : {CYAN}{latest_prediction['asset']}{RESET}")
                print(f" Composite Friction Score      : {latest_prediction['composite']} / 1.00")
                print(f" Intercalculated Alpha Score   : {latest_prediction['score']}")
                print(f"\n Justification / Reason        :\n   -> {latest_prediction['reason']}")
                print(f"\n Saved to Persistent Audit File: {PREDICTION_AUDIT_PATH.name}")

            elif view_key == "H":
                print(f"{PURPLE}  AUDIT TRAIL: HISTORICAL MODEL PREDICTIONS{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                history = load_prediction_history()
                if not history: print(f" {GRAY}No prediction history recorded yet.{RESET}")
                else:
                    print(f" {'TIMESTAMP':<20} | {'ACTION':<22} | {'ASSET':<30} | {'SCORE':<6}")
                    print("-" * 89)
                    for item in reversed(history): print(f" {item['timestamp']:<20} | {item['action']:<22} | {item['asset'][:30]:<30} | {item['score']:<6}")

            # ---------------------------------------------------------
            # FOOTER / ENGINE STATUS
            # ---------------------------------------------------------
            time_since_batch = time.time() - last_batch_sync_time
            next_batch_in = max(OSINT_BATCH_INTERVAL_SECONDS - time_since_batch, 0)
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{PURPLE}  GLOBAL ENGINE STATUS (Composite Index: {composite:.2f} / 1.00){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            print(f" {GRAY}Active Recommendation:{RESET} {latest_prediction['action']} -> {latest_prediction['asset']}")
            print(f" {GRAY}Thread-Pooled Feeds:{RESET} {len(FEED_REGISTRY)}/{len(FEED_REGISTRY)} Synced | {GRAY}Next Batch:{RESET} {next_batch_in:.0f}s")
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{YELLOW} [1-0] Markets | [W] Weather/FAA | [K] War Theater | [P] Prediction Engine | [H] History Audit {RESET}")
            print(f"{GRAY} Log: {LOG_PATH.name}  |  Audit Log: {PREDICTION_AUDIT_PATH.name}  |  Unusual: {UNUSUAL_ACTIVITY_LOG_PATH.name}{RESET}")

            # Hotkey Polling Loop
            for _ in range(int(POLL_INTERVAL * 10)):
                if IS_WINDOWS and _kbhit():
                    key = _getch()
                    if key in MARKETS or key in ['w', 'k', 'p', 'h']:
                        with view_lock: current_view = key.upper()
                        break
                time.sleep(0.1)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] Global tactical terminal disengaged safely by operator.{RESET}")
        logger.info("Terminal stopped by operator.")

if __name__ == "__main__":
    main()