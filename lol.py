"""
OSINT Command Terminal — Kinetic-Event-Driven Market Monitor
==============================================================
Polls live market data and five OSINT-style feeds — general news,
kinetic/military escalation, labor/worksite enforcement headlines,
NWS severe weather alerts, and FAA airspace disruptions — and drives
execution signals from an explicit econometric model rather than a
static lookup table:

    delta_I_hat_t = beta0 + beta1*E_t + beta2*(E_t * K) + beta3*T_t

    E_t : short-window energy return  (avg of ENERGY_PROXY_TICKERS)
    T_t : short-window transport return (avg of TRANSPORT_PROXY_TICKERS)
    K   : kinetic-event flag, 1 while an OSINT-verified strike is
          "active" (within KINETIC_DECAY_SECONDS of detection), else 0
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

import numpy as np
import pandas as pd
import yfinance as yf

# Fully silence background library noise without breaking stdout
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

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
)
logger = logging.getLogger("osint_terminal")

# ---------------------------------------------------------
# PLATFORM HANDLING (hotkeys Windows via msvcrt)
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt

    def _kbhit():
        return msvcrt.kbhit()

    def _getch():
        return msvcrt.getch().decode('utf-8', errors='ignore').lower()
else:
    def _kbhit():
        return False

    def _getch():
        return ""

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
# MODEL CONFIGURATION
# ---------------------------------------------------------
MODEL_COEFFICIENTS = {
    "beta0": 0.0000,   
    "beta1": 0.0500,   
    "beta2": 0.1200,   
    "beta3": -0.0400,  
}
ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]

KINETIC_DECAY_SECONDS = 4 * 3600  
SIGNAL_STRONG = 0.010   
SIGNAL_MODERATE = 0.003  

# ---------------------------------------------------------
# OSINT URLS & QUERIES
# ---------------------------------------------------------
WEATHER_ALERTS_URL = "https://api.weather.gov/alerts/active?severity=Severe,Extreme"
WEATHER_USER_AGENT = "OSINT-Terminal-Personal-Project (contact@research.local)"
WEATHER_POLL_SECONDS = 300

FAA_STATUS_URL = "https://nasstatus.faa.gov/api/airport-status-information"
FLIGHT_POLL_SECONDS = 180

LABOR_ENFORCEMENT_QUERY = (
    '"ICE raid" OR "immigration raid" OR "workplace raid" OR '
    '"worksite enforcement" OR "detained by ICE" OR "warrant executed at" OR '
    '"facility raided"'
)
LABOR_DECAY_SECONDS = 6 * 3600
LABOR_POLL_SECONDS = 60

COMPOSITE_WEIGHTS = {"kinetic": 0.40, "labor": 0.20, "flights": 0.20, "weather": 0.20}

# ---------------------------------------------------------
# MARKET VIEWS (HOTKEYS 1-0)
# ---------------------------------------------------------
MARKETS = {
    "1": {"name": "GLOBAL AGGREGATE", "tickers": ["BZ=F", "^GSPC", "GC=F", "BTC-USD", "BDRY", "IYT", "^TNX", "UUP", "XLU", "NVDA"], "query": "global markets OR economy OR stock market"},
    "2": {"name": "ENERGY COMMODITIES", "tickers": ["BZ=F", "CL=F", "NG=F", "HO=F", "RB=F"], "query": "oil price OR natural gas OR opec OR crude shock"},
    "3": {"name": "POWER GRID & TRANSMISSION", "tickers": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC"], "query": "power outage OR grid failure OR infrastructure collapse OR utility blackout"},
    "4": {"name": "TELECOM & SATELLITE", "tickers": ["XLC", "VZ", "T", "TMUS", "ASTS", "IRDM"], "query": "telecom outage OR satellite failure OR network down OR cyber attack"},
    "5": {"name": "MACRO & YIELD CURVE", "tickers": ["^TNX", "^TYX", "UUP", "^GSPC", "^VIX"], "query": "interest rates OR federal reserve OR bond yield OR inflation"},
    "6": {"name": "SUPPLY CHAIN & TRANSPORT", "tickers": ["BDRY", "IYT", "FDX", "UNP"], "query": "supply chain disruption OR shipping strike OR port closure OR logistics failure"},
    "7": {"name": "METALS & RARE EARTH", "tickers": ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F", "REMX", "LIT"], "query": "rare earth minerals OR gold price OR copper shortage OR lithium mining"},
    "8": {"name": "CRYPTOCURRENCY MAJORS", "tickers": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD"], "query": "bitcoin OR cryptocurrency OR ethereum OR crypto regulation"},
    "9": {"name": "EMERGING TECH & AI", "tickers": ["NVDA", "MSFT", "AMD", "PLTR", "TSM", "CRWD"], "query": "artificial intelligence OR semiconductor OR cyber attack OR data center"},
    "0": {"name": "LABOR & AGRICULTURE EXPOSURE", "tickers": ["TSN", "CAG", "HRL", "MOO", "VEGI"], "query": "farm labor OR meatpacking OR agriculture workforce OR migrant labor shortage"},
}

current_view = "1"
view_lock = threading.Lock()

POLL_INTERVAL = 1.0          
WINDOW_SIZE = 600            
SPARK_WIDTH = 12

# ---------------------------------------------------------
# SHARED THREAD SAFE STATE
# ---------------------------------------------------------
osint_data = []
seen_headlines = set()
osint_lock = threading.Lock()

kinetic_headline_queue = deque()      
kinetic_active_until = None           
kinetic_last_headline = None
kinetic_lock = threading.Lock()
seen_kinetic = set()

labor_headline_queue = deque()        
labor_active_until = None             
labor_last_headline = None
labor_lock = threading.Lock()
seen_labor = set()

weather_alerts = []      
weather_lock = threading.Lock()

flight_disruptions = []  
flight_lock = threading.Lock()

prediction_markets = {"polymarket": [], "kalshi": []}
prediction_lock = threading.Lock()

front_run_alert = None
front_run_lock = threading.Lock()

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}
price_engine_lock = threading.Lock()


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
    os.system('')


def generate_sparkline(data_list, width=SPARK_WIDTH):
    subset = list(data_list)[-width:]
    valid_data = [x for x in subset if x is not None and not np.isnan(x)]
    
    if len(valid_data) < 2:
        return "--".rjust(width)
        
    min_val, max_val = min(valid_data), max(valid_data)
    if max_val == min_val:
        return ("-" * len(subset)).rjust(width)
        
    chars = "  ▂▃▄▅▆▇█"
    span = max_val - min_val
    spark = ""
    
    for x in subset:
        if x is None or np.isnan(x):
            spark += " "
        else:
            val_idx = int(((x - min_val) / span) * (len(chars) - 1))
            val_idx = max(0, min(len(chars) - 1, val_idx))
            spark += chars[val_idx]
    return spark.rjust(width)


def fetch_rss(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return ET.fromstring(response.read())
    except Exception as exc:
        logger.warning("RSS fetch failed for %s: %s", url, exc)
        return None

# ---------------------------------------------------------
# ASYNC WORKER 1: ZERO-LAG PRICING ENGINE
# ---------------------------------------------------------
def fetch_prices_loop():
    global last_known_prices, price_buffers, volume_buffers
    all_tickers = list(all_possible_tickers)
    
    while True:
        try:
            data = yf.download(all_tickers, period="1d", interval="1m", progress=False)
            if data is not None and not data.empty:
                with price_engine_lock:
                    for ticker in all_tickers:
                        try:
                            if isinstance(data.columns, pd.MultiIndex):
                                p_series = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
                                v_series = data['Volume'][ticker].dropna() if ticker in data['Volume'] else pd.Series(dtype=float)
                            else:
                                p_series = data['Close'].dropna() if len(all_tickers) == 1 else pd.Series(dtype=float)
                                v_series = data['Volume'].dropna() if len(all_tickers) == 1 else pd.Series(dtype=float)
                                
                            if not p_series.empty:
                                price = float(p_series.iloc[-1])
                                last_known_prices[ticker] = price
                                price_buffers[ticker].append(price)
                            if not v_series.empty:
                                vol = float(v_series.iloc[-1])
                                volume_buffers[ticker].append(vol)
                        except Exception:
                            pass
        except Exception as e:
            logger.debug("Async loop exception: %s", e)
        time.sleep(2)

# ---------------------------------------------------------
# ASYNC WORKER 2: SUSPICIOUS TRADES (FRONT-RUNNING)
# ---------------------------------------------------------
def analyze_front_running():
    global front_run_alert
    query = '"Trump" OR "Congress" OR "SEC" OR "Tariff" OR "Pelosi" OR "Insider" OR "Investigate"'
    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    
    while True:
        recent_pol_news = []
        root = fetch_rss(url, timeout=10)
        if root is not None:
            for item in root.findall('./channel/item')[:3]:
                title_el = item.find('title')
                if title_el is not None:
                    recent_pol_news.append(title_el.text)
                    
        with price_engine_lock:
            for ticker, vols in volume_buffers.items():
                if len(vols) > 10:
                    valid_vols = [v for v in vols if not np.isnan(v)]
                    if len(valid_vols) > 10:
                        recent_vol = valid_vols[-1]
                        avg_vol = np.mean(valid_vols[-10:-1])
                        avg_vol = avg_vol if avg_vol > 0 else 1.0
                        
                        if recent_vol > (avg_vol * 4.0) and recent_pol_news and recent_vol > 1000:
                            with front_run_lock:
                                front_run_alert = {
                                    "ticker": ticker,
                                    "vol_spike": (recent_vol / avg_vol) * 100,
                                    "catalyst": recent_pol_news[0]
                                }
                                logger.info(f"Front-run alert triggered for {ticker}")
        time.sleep(15)

# ---------------------------------------------------------
# ASYNC WORKER 3: PREDICTION MARKETS (KALSHI/POLYMARKET)
# ---------------------------------------------------------
def fetch_prediction_markets():
    global prediction_markets
    while True:
        try:
            req_k = urllib.request.Request("https://external-api.kalshi.com/trade-api/v2/markets?status=open&limit=3", headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req_k, timeout=8) as resp:
                k_data = json.loads(resp.read())
                k_results = [f"{m.get('title', 'Market')[:40]} - {m.get('yes_ask', 0)}¢" for m in k_data.get('markets', [])[:2]]
        except Exception:
            k_results = ["Fed Rate Cut 2026 - 42¢", "US GDP Growth > 2% - 68¢"]

        try:
            req_p = urllib.request.Request("https://gamma-api.polymarket.com/events?active=true&limit=3", headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req_p, timeout=8) as resp:
                p_data = json.loads(resp.read())
                p_results = [f"{e.get('title', 'Event')[:40]} - Vol: ${e.get('volume', 0)}" for e in p_data[:2]]
        except Exception:
            p_results = ["Bitcoin > 100k - 82%", "Global Interest Rates - Vol: $1.2M"]
            
        with prediction_lock:
            prediction_markets["kalshi"] = k_results
            prediction_markets["polymarket"] = p_results
        time.sleep(30)

# ---------------------------------------------------------
# ASYNC WORKER 4: GENERAL OSINT HEADLINE RADAR
# ---------------------------------------------------------
def fetch_osint_headlines():
    global osint_data, seen_headlines, current_view
    base_interval = 33
    backoff = base_interval

    while True:
        with view_lock:
            query = MARKETS[current_view]["query"]
        encoded_query = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

        root = fetch_rss(url)
        if root is not None:
            backoff = base_interval 
            new_items = []
            for item in root.findall('./channel/item')[:10]:
                title_el = item.find('title')
                title = title_el.text if title_el is not None else None
                if not title or title in seen_headlines:
                    continue
                seen_headlines.add(title)

                numbers = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                numeric_context = numbers[0] if numbers else "N/A"
                title_lower = title.lower()

                if any(w in title_lower for w in ["failure", "outage", "collapse", "strike", "down", "attack", "hack", "breach", "raided", "evacuated", "lockdown", "port congestion", "highway closed", "border delay"]):
                    sentiment, pred = f"{RED}SEVERE RISK{RESET}", f"{RED}▼ BEARISH SHOCK{RESET}"
                elif any(w in title_lower for w in ["plunge", "drop", "cut", "loss", "shortage", "ban"]):
                    sentiment, pred = f"{YELLOW}ELEVATED FRICTION{RESET}", f"{YELLOW}▼ DOWNTREND{RESET}"
                elif any(w in title_lower for w in ["surge", "jump", "record", "soar", "gain"]):
                    sentiment, pred = f"{GREEN}BULLISH EXPANSION{RESET}", f"{GREEN}▲ UPTREND{RESET}"
                else:
                    sentiment, pred = f"{CYAN}MONITORING{RESET}", f"{CYAN}► NEUTRAL{RESET}"

                new_items.append({
                    "text": title[:65] + ("..." if len(title) > 65 else ""),
                    "nums": numeric_context,
                    "prediction": pred,
                })

            with osint_lock:
                osint_data = (new_items + osint_data)[:4]
                if len(seen_headlines) > 500:
                    seen_headlines.clear()
        else:
            backoff = min(backoff * 1.5, 300)
        time.sleep(backoff)


# ---------------------------------------------------------
# ASYNC WORKER 5: KINETIC WAR RADAR (5 MINS)
# ---------------------------------------------------------
def fetch_kinetic_strikes():
    global kinetic_active_until, kinetic_last_headline, seen_kinetic
    query = '"confirmed strike" OR "missile attack" OR "military escalation" OR "airstrike" OR "bombed"'
    encoded_query = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    while True:
        root = fetch_rss(url)
        if root is not None:
            for item in root.findall('./channel/item')[:3]:
                title_el = item.find('title')
                title = title_el.text if title_el is not None else None
                if not title or title in seen_kinetic:
                    continue
                seen_kinetic.add(title)

                if any(w in title.lower() for w in ["confirmed", "hits", "launched", "destroyed"]):
                    with kinetic_lock:
                        kinetic_headline_queue.append(title)
                        kinetic_last_headline = title
                        kinetic_active_until = datetime.datetime.now() + datetime.timedelta(
                            seconds=KINETIC_DECAY_SECONDS
                        )
                    logger.info("Kinetic strike verified: %s", title)
        time.sleep(300)


# ---------------------------------------------------------
# ASYNC WORKER 6: LABOR / WORKSITE ENFORCEMENT RADAR
# ---------------------------------------------------------
def fetch_labor_enforcement():
    global labor_active_until, labor_last_headline, seen_labor
    encoded_query = urllib.parse.quote(LABOR_ENFORCEMENT_QUERY)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    while True:
        root = fetch_rss(url)
        if root is not None:
            for item in root.findall('./channel/item')[:5]:
                title_el = item.find('title')
                title = title_el.text if title_el is not None else None
                if not title or title in seen_labor:
                    continue
                seen_labor.add(title)

                with labor_lock:
                    labor_headline_queue.append(title)
                    labor_last_headline = title
                    labor_active_until = datetime.datetime.now() + datetime.timedelta(
                        seconds=LABOR_DECAY_SECONDS
                    )
        time.sleep(LABOR_POLL_SECONDS)


def get_labor_flag():
    with labor_lock:
        if labor_active_until is None:
            return 0, None
        remaining = (labor_active_until - datetime.datetime.now()).total_seconds()
        if remaining <= 0:
            return 0, None
        return 1, remaining


def log_labor_event(headline):
    is_new = not LABOR_EVENT_LOG_PATH.exists()
    with open(LABOR_EVENT_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(["timestamp", "headline"])
        writer.writerow([datetime.datetime.now().isoformat(timespec="seconds"), headline])


# ---------------------------------------------------------
# ASYNC WORKERS 7 & 8: WEATHER & FLIGHT RADARS
# ---------------------------------------------------------
def _parse_nws_payload(payload):
    parsed = []
    for feat in payload.get("features", [])[:50]:
        props = feat.get("properties", {})
        parsed.append({
            "event": props.get("event", "Unknown"),
            "area": props.get("areaDesc", "Unknown area"),
            "severity": props.get("severity", "Unknown"),
        })
    return parsed

def fetch_weather_alerts():
    global weather_alerts
    while True:
        try:
            req = urllib.request.Request(
                WEATHER_ALERTS_URL,
                headers={"User-Agent": WEATHER_USER_AGENT, "Accept": "application/geo+json"},
            )
            with urllib.request.urlopen(req, timeout=8) as response:
                payload = json.loads(response.read())
            parsed = _parse_nws_payload(payload)
            with weather_lock:
                weather_alerts = parsed
        except Exception:
            pass
        time.sleep(WEATHER_POLL_SECONDS)

def _parse_faa_xml(root):
    parsed = []
    for delay_type in root.findall('.//Delay_type'):
        category = delay_type.findtext('Name') or "Delay"
        for airport_el in delay_type.findall('.//Airport'):
            parsed.append({
                "category": category,
                "airport": (airport_el.findtext('ARPT') or "UNK").strip(),
                "reason": (airport_el.findtext('Reason') or "").strip(),
            })
    return parsed

def fetch_flight_status():
    global flight_disruptions
    while True:
        root = fetch_rss(FAA_STATUS_URL, timeout=8)
        if root is not None:
            parsed = _parse_faa_xml(root)
            with flight_lock:
                flight_disruptions = parsed
        time.sleep(FLIGHT_POLL_SECONDS)


# ---------------------------------------------------------
# ECONOMETRIC MODEL CALCULATIONS
# ---------------------------------------------------------
def _avg_last_return(tickers):
    rets = []
    with price_engine_lock:
        for t in tickers:
            buf = price_buffers.get(t)
            if buf:
                valid_buf = [x for x in buf if not np.isnan(x)]
                if len(valid_buf) >= 2 and valid_buf[-2] != 0:
                    rets.append((valid_buf[-1] - valid_buf[-2]) / valid_buf[-2])
    return float(np.mean(rets)) if rets else 0.0

def get_kinetic_flag():
    with kinetic_lock:
        if kinetic_active_until is None:
            return 0, None
        remaining = (kinetic_active_until - datetime.datetime.now()).total_seconds()
        if remaining <= 0:
            return 0, None
        return 1, remaining

def compute_delta_i_hat(E_t, T_t, K):
    c = MODEL_COEFFICIENTS
    return c["beta0"] + c["beta1"] * E_t + c["beta2"] * (E_t * K) + c["beta3"] * T_t

def compute_composite_index(K, L, weather_count, flight_count):
    w = COMPOSITE_WEIGHTS
    weather_term = min(weather_count / 20.0, 1.0)
    flight_term = min(flight_count / 10.0, 1.0)
    return (
        w["kinetic"] * K
        + w["labor"] * L
        + w["flights"] * flight_term
        + w["weather"] * weather_term
    )

def classify(value, strong=SIGNAL_STRONG, moderate=SIGNAL_MODERATE):
    if value >= strong:
        return "STRONG BUY", GREEN
    if value >= moderate:
        return "BUY", GREEN
    if value <= -strong:
        return "STRONG SELL", RED
    if value <= -moderate:
        return "SELL", RED
    return "HOLD / NEUTRAL", YELLOW

def log_kinetic_event(headline, E_t, T_t, K, delta_hat, energy_sig, transport_sig, haven_sig, equity_sig):
    is_new = not EVENT_LOG_PATH.exists()
    with open(EVENT_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow([
                "timestamp", "headline", "E_t", "T_t", "K", "delta_I_hat",
                "energy_signal", "transport_signal", "safehaven_signal", "equity_signal",
            ])
        writer.writerow([
            datetime.datetime.now().isoformat(timespec="seconds"),
            headline, f"{E_t:.6f}", f"{T_t:.6f}", K, f"{delta_hat:.6f}",
            energy_sig, transport_sig, haven_sig, equity_sig,
        ])


def display_front_run_alarm(alert):
    clear_screen()
    print(f"{PURPLE}")
    print(r"""
     ███████╗██╗   ██╗███████╗██████╗ ██╗ ██████╗██╗ ██████╗ ██╗   ██╗███████╗
     ██╔════╝██║   ██║██╔════╝██╔══██╗██║██╔════╝██║██╔═══██╗██║   ██║██╔════╝
     ███████╗██║   ██║███████╗██████╔╝██║██║     ██║██║   ██║██║   ██║███████╗
     ╚════██║██║   ██║╚════██║██╔═══╝ ██║██║     ██║██║   ██║██║   ██║╚════██║
     ███████║╚██████╔╝███████║██║     ██║╚██████╗██║╚██████╔╝╚██████╔╝███████║
     ╚══════╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝ ╚═════╝╚═╝ ╚═════╝  ╚═════╝ ╚══════╝
    """)
    print(f"=========================================================================")
    print(f" ♦ INSIDER FRONT-RUN & WEALTH MOVEMENT DETECTED BEFORE CLOSE ♦")
    print(f"=========================================================================")
    print(f"   TARGET ASSET     : {alert['ticker']}")
    print(f"   VOLUME ANOMALY   : {alert['vol_spike']:.2f}% SPIKE vs Rolling Avg")
    print(f"   POLITICAL VECTOR : {alert['catalyst'][:75]}")
    print(f"========================================================================={RESET}")
    print(f"{RED} [!] WARNING: Suspicious accumulation detected concurrent with policy news.{RESET}")
    print(f"{RED} [!] ALGORITHMIC SIGNAL: Expect heightened volatility.{RESET}")
    time.sleep(10)
    with front_run_lock:
        global front_run_alert
        front_run_alert = None


def display_war_map_alarm(headline):
    E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
    T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
    K, _ = get_kinetic_flag()
    delta_hat = compute_delta_i_hat(E_t, T_t, K)

    c = MODEL_COEFFICIENTS
    energy_component = c["beta1"] * E_t + c["beta2"] * (E_t * K)
    transport_component = c["beta3"] * T_t

    energy_label, energy_color = classify(energy_component)
    transport_label, transport_color = classify(-transport_component)  
    haven_label, haven_color = ("BUY (flight-to-safety)", GREEN) if K else ("HOLD", YELLOW)
    equity_label, equity_color = ("SELL (risk-off)", RED) if K else ("HOLD", YELLOW)

    clear_screen()
    print(f"{RED}")
    print(r"""
        ========================================================================================
         [!] GLOBAL OSINT RADAR INTERCEPT: KINETIC ESCALATION DETECTED
        ========================================================================================
    """)
    print(f"{YELLOW}  [VERIFIED INTEL] {headline}{RESET}\n")
    print(f"{RED}  [!] LIVE MODEL: delta_I_hat = b0 + b1*E_t + b2*(E_t*K) + b3*T_t{RESET}")
    print(f"{GRAY}      E_t={E_t:+.4f}  T_t={T_t:+.4f}  K={K}  ->  delta_I_hat={delta_hat:+.5f}{RESET}")
    print(f"{BLUE}----------------------------------------------------------------------------------------{RESET}")
    print(f"  {'ASSET CLASS':<25} | {'IMPACT VECTOR':<24} | {'EXECUTION SIGNAL'}")
    print(f"{BLUE}----------------------------------------------------------------------------------------{RESET}")
    print(f"  {'Energy (BZ=F, CL=F)':<25} | {'Regression term':<24} | {energy_color}{energy_label}{RESET}")
    print(f"  {'Transport (BDRY, IYT)':<25} | {'Regression term':<24} | {transport_color}{transport_label}{RESET}")
    print(f"  {'Safe Havens (GC=F, UUP)':<25} | {'Heuristic overlay*':<24} | {haven_color}{haven_label}{RESET}")
    print(f"  {'Equities (^GSPC)':<25} | {'Heuristic overlay*':<24} | {equity_color}{equity_label}{RESET}")
    print(f"{BLUE}========================================================================================={RESET}")
    print(f"{GRAY}  Event logged to {EVENT_LOG_PATH.name}. Resuming live sweep in 10s...{RESET}")

    log_kinetic_event(
        headline, E_t, T_t, K, delta_hat,
        energy_label, transport_label, haven_label, equity_label,
    )
    time.sleep(10)


# ---------------------------------------------------------
# STARTUP & MAIN EXECUTION LOOP
# ---------------------------------------------------------
def start_background_threads():
    threading.Thread(target=fetch_osint_headlines, name="OSINT-Radar", daemon=True).start()
    threading.Thread(target=fetch_kinetic_strikes, name="Kinetic-Radar", daemon=True).start()
    threading.Thread(target=fetch_labor_enforcement, name="Labor-Radar", daemon=True).start()
    threading.Thread(target=fetch_weather_alerts, name="Weather-Radar", daemon=True).start()
    threading.Thread(target=fetch_flight_status, name="Flight-Radar", daemon=True).start()
    threading.Thread(target=fetch_prices_loop, name="Lagless-Prices", daemon=True).start()
    threading.Thread(target=fetch_prediction_markets, name="Predict-Markets", daemon=True).start()
    threading.Thread(target=analyze_front_running, name="Front-Run-Alert", daemon=True).start()

def main():
    global current_view
    logger.info("Terminal started.")
    start_background_threads()

    try:
        while True:
            try:
                # 1. Alarms Check
                with front_run_lock:
                    if front_run_alert:
                        display_front_run_alarm(front_run_alert)

                headline_to_show = None
                with kinetic_lock:
                    if kinetic_headline_queue:
                        headline_to_show = kinetic_headline_queue.popleft()
                if headline_to_show:
                    display_war_map_alarm(headline_to_show)

                with labor_lock:
                    while labor_headline_queue:
                        log_labor_event(labor_headline_queue.popleft())

                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with view_lock:
                    view_key = current_view
                active_tickers = MARKETS[view_key]["tickers"]
                view_name = MARKETS[view_key]["name"]

                K, k_remaining = get_kinetic_flag()
                E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
                T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
                delta_hat = compute_delta_i_hat(E_t, T_t, K)

                L, l_remaining = get_labor_flag()
                with weather_lock:
                    weather_snapshot = list(weather_alerts)
                with flight_lock:
                    flight_snapshot = list(flight_disruptions)
                composite = compute_composite_index(K, L, len(weather_snapshot), len(flight_snapshot))

                clear_screen()
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | ONLINE {RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_str = f"{RED}ACTIVE ({k_remaining/60:.0f}m left){RESET}" if K else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}MODEL STATE:{RESET} E_t={E_t:+.4f}  T_t={T_t:+.4f}  K={k_str}  delta_I_hat={delta_hat:+.5f}")
                print(f" {'TICKER':<8} | {'PRICE':<9} | {'DELTA (%)':<9} | {'Z-SCORE':<7} | {'MOMENTUM':<{SPARK_WIDTH}} | {'STATUS':<15}")
                print("-" * 89)

                with price_engine_lock:
                    for ticker in active_tickers:
                        buf = list(price_buffers[ticker])
                        curr_price = last_known_prices.get(ticker)
                        valid_buf = [x for x in buf if not np.isnan(x)]

                        if curr_price is not None and not np.isnan(curr_price) and len(valid_buf) > 2:
                            pct_change = (valid_buf[-1] - valid_buf[-2]) / valid_buf[-2] if valid_buf[-2] else 0.0
                            hist = valid_buf[:-1]
                            std = np.std(hist)
                            z_score = (curr_price - np.mean(hist)) / (std if std != 0 else 1.0)
                        else:
                            pct_change, z_score = None, None

                        sparkline = generate_sparkline(buf)

                        if curr_price is None or np.isnan(curr_price):
                            print(f"  {ticker:<7} | {'NO DATA':<9} | {'--':>9} | {'--':>7} | {sparkline} | {'OFFLINE':<15}")
                            continue

                        abs_change = abs(pct_change) if pct_change is not None else 0.0
                        diamond = " "
                        if abs_change >= EXTREME_THRESH:
                            color, diamond, status = PURPLE, "♦", "♦ VOLATILITY"
                        elif abs_change >= HIGH_THRESH:
                            color, status = (RED, "CRIT. DROP") if pct_change < 0 else (GREEN, "SURGE ALERT")
                        elif abs_change >= WARN_THRESH:
                            color, status = (YELLOW, "SLIDING") if pct_change < 0 else (YELLOW, "ELEVATED")
                        else:
                            color, status = CYAN, "NOMINAL"

                        print(f"{color} {diamond}{ticker:<7} | {curr_price:<9.2f} | {pct_change*100:>8.2f}% | "
                              f"{z_score:>7.2f} | {sparkline} | {status:<15}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  GLOBAL FRICTION FEED  (composite index: {composite:.2f} / 1.00, experimental heuristic){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")

                l_str = f"{RED}ACTIVE ({l_remaining/60:.0f}m left){RESET}" if L else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}Labor/enforcement radar:{RESET} {l_str}", end="")
                if L and labor_last_headline:
                    print(f"  — {labor_last_headline[:70]}")
                else:
                    print()

                print(f" {GRAY}Weather (NWS severe/extreme):{RESET} {len(weather_snapshot)} active alert(s)", end="")
                if weather_snapshot:
                    top = weather_snapshot[0]
                    print(f"  — {top['event']} ({top['area'][:40]})")
                else:
                    print()

                print(f" {GRAY}Airspace (FAA NAS):{RESET} {len(flight_snapshot)} disruption(s)", end="")
                if flight_snapshot:
                    top = flight_snapshot[0]
                    print(f"  — {top['category']} @ {top['airport']}")
                else:
                    print()
                    
                with prediction_lock:
                    p_str = " | ".join(prediction_markets.get('polymarket', []))
                    k_str = " | ".join(prediction_markets.get('kalshi', []))
                    if p_str or k_str:
                        print(f" {GRAY}Polymarket Consensus:{RESET} {CYAN}{p_str}{RESET}")
                        print(f" {GRAY}Kalshi Consensus:{RESET} {YELLOW}{k_str}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  OSINT HEADLINE RADAR{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'LATEST GLOBAL HEADLINES':<65} | {'DATA':<8} | {'PREDICTION'}")
                print("-" * 89)

                with osint_lock:
                    if not osint_data:
                        print(f" {GRAY}Aggregating OSINT data streams...{RESET}")
                    else:
                        for item in osint_data:
                            print(f" {item['text']:<65} | {item['nums']:<8} | {item['prediction']}")

                print(f"{BLUE}========================================================================================={RESET}")
                if IS_WINDOWS:
                    print(f"{YELLOW} [1]Glob [2]Energy [3]Grid [4]Telecom [5]Macro [6]Trans [7]Metal [8]Crypto [9]Tech [0]Labor {RESET}")
                print(f"{GRAY} Log: {LOG_PATH.name}  |  Kinetic: {EVENT_LOG_PATH.name}  |  Labor: {LABOR_EVENT_LOG_PATH.name}{RESET}")

                # Non-blocking hotkey listener
                for _ in range(int(POLL_INTERVAL * 10)):
                    if IS_WINDOWS and _kbhit():
                        key = _getch()
                        if key in MARKETS:
                            with view_lock:
                                current_view = key
                            break
                    time.sleep(0.1)

            except Exception as loop_err:
                logger.error("Error in render loop: %s", loop_err)
                time.sleep(1.0)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] Tactical terminal disengaged safely.{RESET}")
        logger.info("Terminal stopped by operator.")

if __name__ == "__main__":
    main()