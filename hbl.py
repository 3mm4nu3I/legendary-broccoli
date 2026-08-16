r"""
OSINT Command Terminal — PROJECT HANNIBAL (Singularity Master)
==========================================================================
Clearance: EID Verified (B. Noffels, 6/24/2026)
Architecture Upgrades:
- NEURAL UPLINK [T]: Direct conversational dialogue interface with the AI.
- GEOPOLITICAL MARKETS: Sector calculus for Russia, China, Japan, Middle East, India.
- SQLITE DATA LAKE: Persistent historical intelligence databasing.
- AUTONOMOUS BOOTSTRAPPER: Recursive system-drive scanning for Ollama.
- INTERACTIVE MAPS & GRAPHS: W/A/S/D mapping arrays and multi-mode algorithmic graphs.
- SEC WHALE HUNTER: Multi-stage Form 4 EDGAR text-to-XML parsing with audio alerts.
"""

import os
import sys
import csv
import json
import time
import platform
import re
import threading
import subprocess
import sqlite3
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
# PATHS & LOGGING CONFIG
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "osint_terminal.log"
DB_PATH = SCRIPT_DIR / "hannibal_intel.db"
LARGE_INSIDER_TRADE_LOG_PATH = SCRIPT_DIR / "large_insider_trades.csv"
PREDICTION_AUDIT_PATH = SCRIPT_DIR / "prediction_audit.txt"

logging.basicConfig(filename=str(LOG_PATH), level=logging.INFO, format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")
logger = logging.getLogger("osint_terminal")

# ---------------------------------------------------------
# UI COLOR PALETTE & FORMATTING
# ---------------------------------------------------------
RESET = "\033[0m"
PURPLE = "\033[95m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

SPARK_WIDTH = 12
EXTREME_THRESH = 0.02
HIGH_THRESH = 0.01

# ---------------------------------------------------------
# GLOBAL ENGINE STATE & THREAD LOCKS
# ---------------------------------------------------------
current_view = "1"
view_lock = threading.Lock()
state_lock = threading.Lock()
db_lock = threading.Lock()

osint_data = []
kinetic_headlines = []
infra_atomic_headlines = []
labor_headlines = []
weather_alerts = []
flight_disruptions = []
insider_filings = []
insider_details = []          
seen_insider_links = set()
large_insider_trade_flag = 0  
sector_headlines = {}  
prediction_markets = []
prev_prediction_prices = {}

napalm_active_synthesis = "HANNIBAL Engine initializing. Awaiting local LLM generation..."
napalm_advisories = []
napalm_chat_history = []

global_E_t = 0.0
global_T_t = 0.0
global_K = 0
global_L = 0
global_I_score = 0
global_C_t = 1.0
global_composite = 0.0

map_cursor_row = 5
map_cursor_col = 40
selected_node_info = "Use W/A/S/D to move crosshair cursor. Press ENTER to inspect active node."
zoom_level = 0
graph_overlay_mode = 1

last_batch_sync_time = 0.0
last_fast_sync_time = 0.0

llm_status_string = "Initializing local neural bridge..."

# ---------------------------------------------------------
# MARKETS & TICKERS EXPANSION
# ---------------------------------------------------------
MARKETS = {
    "1": {"name": "GLOBAL AGGREGATE", "tickers": ["BZ=F", "^GSPC", "^FTSE", "^GSPTSE", "VGK", "GC=F", "BTC-USD", "BDRY", "IYT", "^TNX"]},
    "2": {"name": "ENERGY COMMODITIES", "tickers": ["BZ=F", "CL=F", "NG=F", "HO=F", "RB=F"]},
    "3": {"name": "POWER GRID & INFRASTRUCTURE", "tickers": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "CCJ"]},
    "4": {"name": "TELECOM & SATELLITE", "tickers": ["XLC", "VZ", "T", "TMUS", "ASTS", "IRDM"]},
    "5": {"name": "MACRO & YIELD CURVE", "tickers": ["^TNX", "^TYX", "UUP", "^GSPC", "^VIX"]},
    "6": {"name": "SUPPLY CHAIN & TRANSPORT", "tickers": ["BDRY", "IYT", "FDX", "UNP", "ZIM"]},
    "7": {"name": "METALS & RARE EARTH", "tickers": ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F", "REMX", "LIT"]},
    "8": {"name": "CRYPTOCURRENCY MAJORS", "tickers": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD"]},
    "9": {"name": "EMERGING TECH & AI", "tickers": ["NVDA", "MSFT", "AMD", "PLTR", "TSM", "ASML"]},
    "0": {"name": "LABOR & AGRICULTURE", "tickers": ["TSN", "CAG", "HRL", "MOS", "NTR", "WEAT"]},
    "R": {"name": "RUSSIAN FEDERATION", "tickers": ["BZ=F", "GC=F", "PL=F", "OGZPY", "SBRCY"]},
    "A": {"name": "GREATER CHINA", "tickers": ["FXI", "KWEB", "BABA", "TCEHY", "TSM"]},
    "J": {"name": "JAPAN & NORTH ASIA", "tickers": ["EWJ", "DXJ", "JPY=X", "TM", "SONY"]},
    "E": {"name": "MIDDLE EAST ENERGY", "tickers": ["BZ=F", "CL=F", "NG=F", "XOM", "CVX"]},
    "N": {"name": "INDIA & SOUTH ASIA", "tickers": ["INDA", "EPI", "INR=X", "INFY", "WIT"]}
}

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))

WINDOW_SIZE = 600
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}

SECTOR_KEYWORDS = {
    "2": ["oil", "opec", "brent", "crude", "gas", "energy", "hormuz", "lng"],
    "3": ["grid", "power", "blackout", "nuclear", "reactor", "iaea", "pipeline", "utility"],
    "4": ["satellite", "telecom", "cable", "network", "spectrum", "broadband"],
    "5": ["yield", "rate", "fed", "central bank", "inflation", "bond", "treasury"],
    "6": ["port", "shipping", "logistics", "freight", "supply chain", "canal", "strait", "rail"],
    "7": ["gold", "silver", "copper", "lithium", "rare earth", "mining", "metal"],
    "8": ["bitcoin", "crypto", "ethereum", "blockchain", "stablecoin"],
    "9": ["semiconductor", "chip", "ai", "data center", "nvidia", "tsmc", "cyber"],
    "0": ["farm", "crop", "agriculture", "fertilizer", "grain", "food", "labor", "migrant"],
    "R": ["russia", "kremlin", "moscow", "putin", "ruble", "gazprom", "ukraine"],
    "A": ["china", "beijing", "taiwan", "xi jinping", "pla", "pboc", "yuan"],
    "J": ["japan", "tokyo", "nikkei", "boj", "korea", "pyongyang", "seoul"],
    "E": ["iran", "israel", "hormuz", "gaza", "saudi", "riyadh", "tehran", "tel aviv"],
    "N": ["india", "mumbai", "kashmir", "pakistan", "modi", "rbi", "rupee"]
}
SECTOR_RELEVANCE_CAP = 60  

# ---------------------------------------------------------
# SQLITE DATABASE INITIALIZATION
# ---------------------------------------------------------
def init_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS intel_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            category TEXT,
            region TEXT,
            headline TEXT,
            data_val TEXT,
            prediction TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS insider_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            issuer TEXT,
            owner TEXT,
            role TEXT,
            code TEXT,
            shares REAL,
            price REAL,
            value_usd REAL
        )
    """)
    conn.commit()
    return conn

db_conn = init_db()

def log_to_db(category, region, headline, data_val, prediction):
    try:
        with db_lock:
            cursor = db_conn.cursor()
            cursor.execute(
                "INSERT INTO intel_logs (timestamp, category, region, headline, data_val, prediction) VALUES (?, ?, ?, ?, ?, ?)",
                (datetime.datetime.now().isoformat(), category, region, headline, data_val, prediction)
            )
            db_conn.commit()
    except Exception as e:
        logger.debug(f"DB log error: {e}")

# ---------------------------------------------------------
# PLATFORM HANDLING & AUDIO ALERTS
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
    import winsound
    def _kbhit(): return msvcrt.kbhit()
    def _getch():
        ch = msvcrt.getch()
        if ch in b'\x00\xe0':
            msvcrt.getch()
            return ''
        try: return ch.decode('utf-8', errors='ignore').lower()
        except Exception: return ''
else:
    def _kbhit(): return False
    def _getch(): return ''
    class winsound:
        @staticmethod
        def MessageBeep(n): pass

def trigger_alert_sound(level="warning"):
    if IS_WINDOWS:
        try:
            if level == "critical":
                winsound.Beep(2500, 400)
                winsound.Beep(2000, 400)
            else:
                winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        except Exception: pass

# ---------------------------------------------------------
# GLOBAL TIMING & MATH CONFIGURATION
# ---------------------------------------------------------
OSINT_BATCH_INTERVAL_SECONDS = 90.0 
FAST_POLL_INTERVAL_SECONDS = 15.0 
DATA_TTL_SECONDS = 3600.0  
PRICE_POLL_BASE_SECONDS = 1.5
PRICE_POLL_MAX_BACKOFF = 30.0
POLL_INTERVAL = 0.3  

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3" 
OLLAMA_TIMEOUT_SECONDS = 180

MODEL_COEFFICIENTS = {"beta0": 0.0276, "beta1": 0.0356, "beta2": -0.0251, "beta3": -0.0006, "beta4": 0.0015}

ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]
LARGE_INSIDER_TRADE_USD = 1_000_000
OPEN_MARKET_CODES = {"P": "PURCHASE", "S": "SALE"}

ET_ZONE = ZoneInfo("America/New_York")
LOCAL_ZONE = ZoneInfo("Europe/Lisbon")

COMPOSITE_WEIGHTS = {"kinetic": 0.20, "infrastructure": 0.20, "labor": 0.10, "flights": 0.10, "weather": 0.10, "prediction_markets": 0.30}
COMPOUNDING_CHOKEPOINTS = ["hormuz", "bab el-mandeb", "suez", "malacca", "panama canal", "bosphorus", "tsmc", "subsea cable", "lng terminal", "uranium enrichment", "iaea", "nuclear reactor", "black sea grain"]

PREDICTION_DISCLAIMER = "Experimental Singularity Model — NOT financial advice."

# ---------------------------------------------------------
# UI HELPER FUNCTIONS
# ---------------------------------------------------------
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

def generate_bar_chart(val, max_val=1.0, width=20):
    val = max(min(val, max_val), -max_val)
    half = width // 2
    if val >= 0:
        fill = int((val / max_val) * half)
        return " " * half + "|" + "█" * fill + " " * (half - fill)
    else:
        fill = int((abs(val) / max_val) * half)
        return " " * (half - fill) + "█" * fill + "|" + " " * half

def safe_request(url, is_api=False):
    if "sec.gov" in url:
        headers = {"User-Agent": "ProjectNapalm bram.noffels@example.com", "Host": "www.sec.gov"}
    else:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8"}
        
    if is_api and "weather.gov" in url: headers["Accept"] = "application/geo+json"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as response:
            return response.read()
    except Exception as e:
        logger.debug(f"Request failed for {url}: {e}")
        return None

# ---------------------------------------------------------
# AUTONOMOUS RECURSIVE DRIVE SCANNER
# ---------------------------------------------------------
def recursive_find_ollama():
    drives = ["F:", "C:", "D:", "E:", "G:"]
    for drv in drives:
        if not os.path.exists(drv): continue
        try:
            for root, dirs, files in os.walk(drv):
                if "ollama.exe" in files:
                    return os.path.join(root, "ollama.exe")
        except Exception: continue
    return None

def background_ollama_bootstrapper():
    global llm_status_string
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags", headers={"User-Agent": "NAPALM"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                llm_status_string = "Llama 3 Active (Localhost:11434)"
                return
    except Exception: pass

    llm_status_string = "Searching drives for ollama.exe..."
    ollama_bin = recursive_find_ollama()

    if ollama_bin:
        llm_status_string = f"Booting Ollama from {ollama_bin}..."
        try:
            subprocess.Popen([ollama_bin, "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(20):
                time.sleep(2)
                try:
                    req = urllib.request.Request("http://localhost:11434/api/tags", headers={"User-Agent": "NAPALM"})
                    with urllib.request.urlopen(req, timeout=2) as resp:
                        if resp.status == 200:
                            llm_status_string = "Llama 3 Active (Background Daemon)"
                            return
                except Exception: continue
        except Exception as e:
            llm_status_string = f"Ollama error: {e}"
    else:
        llm_status_string = "Ollama binary not found. Running heuristic fallback."

# ---------------------------------------------------------
# INTERACTIVE MAP STATE & PANNABLE ASCII MAP
# ---------------------------------------------------------
MAP_LEVELS = [
    [
        r"  ==============================================================================  ",
        r" |     . _..::__:  ,-\"-\"._        |]       ,      _,.__                     | ",
        r" | _.___ _ _<_>`!(._`.`-.    /         _._     `_ ,_/  '  '-._.---.-.__     | ",
        r" |.{     \" \" `-==,',._\{  \  / {)      / _ \">_,-' `                mt-2_   | ",
        r" | \_.:--.       `._ )`^-. \"'       , [_/(                       __,/-'    | ",
        r" |'\"'     \         \"    _L        oD_,--'                )     /. (|     | ",
        r" |         |           ,'          _)_.\\._<> {}               _,' /  '    | ",
        r" |         `.         /           [_/_'` `\"(                <'}  )        | ",
        r" |          \\\\    .-. )           /   `-'\".\.' `:.          _)  '         | ",
        r" |   `\     ,' |>       /      \  /`      s          /   /                  | ",
        r" |     `-.,'           |       /  | \          _,-'     /                   | ",
        r"  ==============================================================================  "
    ]
]

def render_interactive_ascii_map(level, war_snap, infra_snap):
    global map_cursor_row, map_cursor_col, selected_node_info
    coords_map = {
        "ukraine": (3, 14, "UKR FRONT"), "kyiv": (3, 18, "KYIV"), "russia": (2, 45, "RUS"),
        "israel": (8, 42, "ISR"), "gaza": (9, 41, "GAZA"), "iran": (6, 52, "IRAN"),
        "hormuz": (8, 55, "HORMUZ"), "taiwan": (7, 62, "TAIWAN"), "red sea": (9, 45, "RED SEA"),
        "black sea": (4, 25, "BLK SEA"), "bab el-mandeb": (10, 48, "BAB EL-MANDEB")
    }
    
    detected_nodes = {}
    all_texts = [h[1]['text'].lower() for h in war_snap] + [h[1]['text'].lower() for h in infra_snap]
    for text in all_texts:
        for kw, (r, c, label) in coords_map.items():
            if kw in text:
                if label not in detected_nodes:
                    detected_nodes[label] = {"row": r, "col": c, "text": text}

    grid = [list(row) for row in MAP_LEVELS[level]]
    
    for label, info in detected_nodes.items():
        r, c = info["row"], info["col"]
        if 0 <= r < len(grid) and 0 <= c < len(grid[0]):
            grid[r][c] = '☒'

    hovered = False
    for label, info in detected_nodes.items():
        if info["row"] == map_cursor_row and info["col"] == map_cursor_col:
            selected_node_info = f"[{label}] {info['text']}"
            hovered = True
            break
    if not hovered:
        selected_node_info = "Nominal airspace. No active threat node under crosshair."

    if 0 <= map_cursor_row < len(grid) and 0 <= map_cursor_col < len(grid[0]):
        grid[map_cursor_row][map_cursor_col] = f"{RED}╬{RESET}"

    rendered_map = "\n".join(["".join(row) for row in grid])
    sitrep = f"\n{PURPLE}  --- INTERACTIVE MAP CONTROLS: W/A/S/D (Move) | Z/X (Zoom) ---{RESET}\n"
    sitrep += f"  {CYAN}CURSOR POS: [{map_cursor_row:02d}, {map_cursor_col:02d}] | NODE INSPECT: {selected_node_info}{RESET}\n"
    return rendered_map + sitrep

# ---------------------------------------------------------
# DIRECT NEURAL UPLINK / CHAT HANDLER
# ---------------------------------------------------------
def process_chat_query(user_query, composite, C_t, war_snap, ins_snap):
    global napalm_chat_history
    
    context_str = f"SYSTEM STATE -> Composite: {composite:.2f}, C_t: {C_t:.2f}. "
    if war_snap: context_str += f"Latest Threat: {war_snap[0][1]['text']}. "
    if ins_snap: context_str += f"Latest Whale Trade: {ins_snap[0][1]['title']}."

    napalm_chat_history.append({"role": "user", "content": user_query})
    
    prompt = f"""You are PROJECT HANNIBAL, the core tactical AI for this terminal.
Current Live Context: {context_str}
Respond directly to the commander's query below. Be ruthless, concise, and analytical. Do not use markdown. Do not apologize.

Commander: {user_query}"""

    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}

    try:
        req = urllib.request.Request(OLLAMA_ENDPOINT, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode('utf-8'))
            reply = result.get("response", "").strip()
            napalm_chat_history.append({"role": "hannibal", "content": reply})
    except Exception as e:
        napalm_chat_history.append({"role": "hannibal", "content": f"Neural uplink failed. Connection severed. ({e})"})

# ---------------------------------------------------------
# RED TEAM / BLUE TEAM ADVERSARIAL NEURAL ENGINE
# ---------------------------------------------------------
def run_napalm_engine(composite, C_t, K, E_t, T_t, war_snap, infra_snap, ins_detail_snap, p_snap, osint_snap):
    global napalm_active_synthesis, napalm_advisories
    intel_war = "\n".join([f"- KINETIC: {h[1]['text']}" for h in war_snap[:3]])
    intel_infra = "\n".join([f"- INFRA: {h[1]['text']}" for h in infra_snap[:3]])
    intel_sec_detail = "\n".join([
        f"- SEC WHALE: {d['owner']} ({d['role']}) {d['code_label']} {d['issuer']} ${d['value']:,.0f}"
        for _, d in ins_detail_snap[:3]
    ])
    
    prompt = f"""You are PROJECT HANNIBAL Singularity. Generate a dual-agent adversarial review (Blue Team Bullish Thesis vs Red Team Bearish Critique) based on this telemetry:
Friction: {composite:.2f} | C_t: {C_t:.2f} | K: {K}
INTERCEPTS:
{intel_war}
{intel_infra}
{intel_sec_detail}

FORMAT YOUR RESPONSE EXACTLY AS JSON:
{{
  "synthesis": "Dual-agent macro summary.",
  "advisories": [
    {{"sector": "Sector Name", "action": "BUY/SELL/HEDGE", "asset": "Ticker", "reason": "[BLUE TEAM]: ... vs [RED TEAM]: ...", "outcome": "Predicted market outcome."}}
  ]
}}"""

    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False}
    required_keys = {"sector", "action", "asset", "reason", "outcome"}

    try:
        req = urllib.request.Request(OLLAMA_ENDPOINT, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_SECONDS) as response:
            result = json.loads(response.read().decode('utf-8'))
            ai_resp = json.loads(result.get("response", "{}"))
            napalm_active_synthesis = ai_resp.get("synthesis", "Synthesis active.")
            raw_advisories = ai_resp.get("advisories", [])
            napalm_advisories = [adv for adv in raw_advisories if isinstance(adv, dict) and required_keys.issubset(adv.keys())]
    except Exception as e:
        napalm_active_synthesis = f"Neural bridge offline: {e}"

# ---------------------------------------------------------
# DISTILLATION & PIPELINE
# ---------------------------------------------------------
def distill_high_gravity_inputs(raw_stream, existing_persistence, max_items=100, max_age_seconds=3600):
    scored = []
    seen = set()
    now = time.time()
    combined = raw_stream + existing_persistence
    for ts, item in combined:
        if now - ts > max_age_seconds: continue 
        text = item if isinstance(item, str) else item.get('text', '')
        t_clean = re.sub(r'\[.*?\]', '', text).strip().lower()
        if t_clean in seen or len(t_clean) < 10: continue
        seen.add(t_clean)
        score = 1.0 - ((now - ts) / 3600.0) 
        for cp in COMPOUNDING_CHOKEPOINTS:
            if cp in t_clean: score += 2.5
        scored.append((score, ts, item))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [(ts, item) for score, ts, item in scored[:max_items]]

def distill_per_sector(raw_osint_stream, existing_persistence, cap=60, max_age_seconds=3600):
    now = time.time()
    buckets = {k: [] for k in MARKETS}
    seen_per_sector = {k: set() for k in MARKETS}
    combined = raw_osint_stream + [(ts, item) for sector_list in existing_persistence.values() for ts, item in sector_list]
    for ts, item in combined:
        if now - ts > max_age_seconds: continue
        text = item if isinstance(item, str) else item.get('text', '')
        t_clean = re.sub(r'\[.*?\]', '', text).strip().lower()
        if len(t_clean) < 10: continue
        sectors = [s for s, kws in SECTOR_KEYWORDS.items() if any(kw in t_clean for kw in kws)]
        if not sectors: continue
        for sector in sectors:
            if t_clean in seen_per_sector[sector]: continue
            seen_per_sector[sector].add(t_clean)
            buckets[sector].append((1.0, ts, item))
    result = {}
    for sector, scored in buckets.items():
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        result[sector] = [(ts, item) for score, ts, item in scored[:cap]]
    return result

def fetch_fast_telemetry():
    global weather_alerts, flight_disruptions
    w_data = None
    f_data = None
    try:
        req = urllib.request.Request("https://api.weather.gov/alerts/active?severity=Severe,Extreme", headers={"User-Agent": "NAPALM", "Accept": "application/geo+json"})
        with urllib.request.urlopen(req, timeout=5) as r: w_data = r.read()
    except Exception: pass
    
    try:
        req = urllib.request.Request("https://nasstatus.faa.gov/api/airport-status-information", headers={"User-Agent": "NAPALM"})
        with urllib.request.urlopen(req, timeout=5) as r: f_data = r.read()
    except Exception: pass

    now = time.time()
    temp_w, temp_f = [], []
    if w_data:
        try:
            payload = json.loads(w_data)
            temp_w = [(now, {"event": f.get("properties", {}).get("event", "Unknown"), "area": f.get("properties", {}).get("areaDesc", "Unknown"), "severity": f.get("properties", {}).get("severity", "Unknown")}) for f in payload.get("features", [])[:30]]
        except Exception: pass
    if f_data:
        try:
            root = ET.fromstring(f_data)
            for dt in root.findall('.//Delay_type'):
                cat = dt.findtext('Name') or "Delay"
                for air in dt.findall('.//Airport'):
                    temp_f.append((now, {"category": cat, "airport": (air.findtext('ARPT') or "UNK").strip(), "reason": (air.findtext('Reason') or "").strip()}))
        except Exception: pass
    with state_lock:
        if temp_w: weather_alerts = temp_w
        if temp_f: flight_disruptions = temp_f

def fast_poll_daemon():
    while True:
        fetch_fast_telemetry()
        time.sleep(FAST_POLL_INTERVAL_SECONDS)

# ---------------------------------------------------------
# DIRECT RAW XML FEED REGISTRY & DATA SYNC
# ---------------------------------------------------------
API_ENDPOINTS = {
    "SEC_FORM4": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=100&output=atom",
    "POLYMARKET": "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&order=volume24hr&ascending=false",
}

DIRECT_WIRES = [
    {"type": "rss", "category": "worldwide_wire", "region": "BBC", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"type": "rss", "category": "worldwide_wire", "region": "AL_JAZEERA", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"type": "rss", "category": "worldwide_wire", "region": "NYT", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
    {"type": "rss", "category": "kinetic", "region": "DEFENSE_NEWS", "url": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"},
    {"type": "rss", "category": "macro", "region": "YAHOO_FIN", "url": "https://finance.yahoo.com/news/rss"},
]

def process_feed(feed):
    url, ftype, category = feed["url"], feed["type"], feed["category"]
    raw_data = safe_request(url, is_api=(ftype == "api"))
    if not raw_data: return None
    now = time.time()
    results = {"feed_config": feed, "data": []}
    if ftype == "api":
        try:
            if category == "SEC_FORM4":
                root = ET.fromstring(raw_data)
                ns = {"a": "http://www.w3.org/2005/Atom"}
                for entry in root.findall("a:entry", ns):
                    title = entry.findtext("a:title", namespaces=ns)
                    link_el = entry.find("a:link", ns)
                    href = link_el.get("href") if link_el is not None else None
                    if title and href: results["data"].append((now, {"title": title, "link": href}))
            elif category == "POLYMARKET":
                for m in json.loads(raw_data):
                    op = m.get("outcomePrices")
                    if isinstance(op, str): op = json.loads(op)
                    results["data"].append((now, {"platform": "Polymarket", "question": m.get("question", "Unknown"), "yes_price": float(op[0]) if op else 0.5, "volume": float(m.get("volume24hr") or 0)}))
        except Exception: pass
    elif ftype == "rss":
        try:
            root = ET.fromstring(raw_data)
            for item in root.findall('.//item')[:15]:
                title = item.findtext('title')
                if title: results["data"].append((now, title))
        except Exception: pass
    return results

def fetch_form4_detail(filing_url):
    raw_txt_url = filing_url.replace("-index.htm", ".txt").replace("-index.html", ".txt")
    time.sleep(0.25)
    raw = safe_request(raw_txt_url)
    if not raw: return []
    try:
        doc_str = raw.decode('utf-8', errors='ignore')
        match = re.search(r'(?si)<XML>\s*(.*?)\s*</XML>', doc_str)
        if match:
            root = ET.fromstring(match.group(1).strip())
        else: return []
    except Exception: return []
    issuer_symbol = root.findtext(".//issuer/issuerTradingSymbol") or "?"
    owner_name = (root.findtext(".//reportingOwner/reportingOwnerId/rptOwnerName") or "Unknown").strip()
    role = "Director" if root.findtext(".//reportingOwnerRelationship/isDirector") == "1" else "Officer/Filer"
    parsed = []
    for tx in root.findall(".//nonDerivativeTable/nonDerivativeTransaction"):
        try:
            code = (tx.findtext("./transactionCoding/transactionCode") or "?").strip()
            shares = float(tx.findtext("./transactionAmounts/transactionShares/value"))
            price = float(tx.findtext("./transactionAmounts/transactionPricePerShare/value"))
            ad_code = (tx.findtext("./transactionAmounts/transactionAcquiredDisposedCode/value") or "?").strip()
            value = shares * price
            prediction = f"{CYAN}ROUTINE{RESET}"
            if ad_code == "A" and value >= LARGE_INSIDER_TRADE_USD: prediction = f"{PURPLE}WHALE ACCUMULATION{RESET}"
            elif ad_code == "D" and value >= LARGE_INSIDER_TRADE_USD: prediction = f"{RED}RISK-OFF DUMP{RESET}"
            parsed.append({"issuer": issuer_symbol, "owner": owner_name, "role": role, "code": code, "code_label": OPEN_MARKET_CODES.get(code, code), "shares": shares, "price": price, "value": value, "acquired_disposed": ad_code, "prediction": prediction})
        except Exception: continue
    return parsed

def sync_osint_feeds():
    global last_batch_sync_time, osint_data, insider_filings, prediction_markets, kinetic_headlines, infra_atomic_headlines, labor_headlines, sector_headlines, large_insider_trade_flag
    last_batch_sync_time = time.time()
    
    FEED_REGISTRY = DIRECT_WIRES + [{"type": "api", "category": "SEC_FORM4", "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=40&output=atom"}, {"type": "api", "category": "POLYMARKET", "url": "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=40&order=volume24hr&ascending=false"}]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_feed = {executor.submit(process_feed, feed): feed for feed in FEED_REGISTRY}
        raw_osint, raw_war, raw_infra, raw_labor = [], [], [], []
        temp_insider, temp_predictions = [], []
        for future in concurrent.futures.as_completed(future_to_feed):
            res = future.result()
            if not res: continue
            cat, data = res["feed_config"]["category"], res["data"]
            if cat == "SEC_FORM4": temp_insider.extend(data)
            elif cat in ["POLYMARKET"]: temp_predictions.extend(data)
            elif res["feed_config"]["type"] == "rss":
                for ts, title in data:
                    region_tag = res["feed_config"].get('region', 'GLOBAL')
                    nums = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                    nums_str = nums[0] if nums else "N/A"
                    log_to_db(cat, region_tag, title, nums_str, "RAW_INGEST")
                    if cat in ["kinetic"]:
                        raw_war.append((ts, {"text": f"[{region_tag}] {title[:60]}", "nums": nums_str, "prediction": f"{RED}▼ ESCALATION{RESET}"}))
                    elif cat in ["infrastructure", "atomic_nuclear"]:
                        raw_infra.append((ts, {"text": f"[{region_tag}] {title[:60]}", "nums": nums_str, "prediction": f"{RED}▼ CRITICAL{RESET}"}))
                    elif cat in ["labor"]:
                        raw_labor.append((ts, {"text": f"[{region_tag}] {title[:60]}", "nums": nums_str, "prediction": f"{PURPLE}♦ DISRUPTION{RESET}"}))
                    else:
                        raw_osint.append((ts, {"text": f"[{region_tag}] {title[:75]}", "nums": nums_str, "prediction": f"{CYAN}► MACRO{RESET}"}))

    newly_seen_links = []
    with state_lock:
        for item_ts, item in temp_insider:
            if item["link"] not in seen_insider_links:
                seen_insider_links.add(item["link"])
                insider_filings.append((item_ts, item))
                newly_seen_links.append(item["link"])
        insider_filings[:] = insider_filings[-50:]
        kinetic_headlines = distill_high_gravity_inputs(raw_war, kinetic_headlines, max_items=100)
        infra_atomic_headlines = distill_high_gravity_inputs(raw_infra, infra_atomic_headlines, max_items=80)
        labor_headlines = distill_high_gravity_inputs(raw_labor, labor_headlines, max_items=50)
        osint_data = distill_high_gravity_inputs(raw_osint, osint_data, max_items=150)
        if temp_predictions: prediction_markets = [item[1] for item in temp_predictions]

    if newly_seen_links:
        new_large_trade = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as detail_executor:
            futures = [detail_executor.submit(fetch_form4_detail, link) for link in newly_seen_links[:FORM4_DETAIL_MAX_PER_BATCH]]
            for fut in concurrent.futures.as_completed(futures):
                try: transactions = fut.result()
                except Exception: continue
                for tx in transactions:
                    with state_lock:
                        insider_details.append((time.time(), tx))
                        insider_details[:] = insider_details[-40:]
                    if tx["code"] in OPEN_MARKET_CODES and tx["value"] >= LARGE_INSIDER_TRADE_USD:
                        new_large_trade = True
                        trigger_alert_sound("critical")
        with state_lock: large_insider_trade_flag = 1 if new_large_trade else 0

def osint_sync_daemon():
    while True:
        sync_osint_feeds()
        time.sleep(OSINT_BATCH_INTERVAL_SECONDS)

# ---------------------------------------------------------
# PRICES & CALCULUS
# ---------------------------------------------------------
def fetch_prices_and_volume(tickers):
    price_result = {}
    try:
        with open(os.devnull, 'w') as devnull:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = devnull, devnull
            try: data = yf.download(tickers, period="5d", interval="1m", progress=False)
            finally: sys.stdout, sys.stderr = old_stdout, old_stderr
    except Exception: return price_result
    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                c_s = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
            else:
                c_s = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
            if not c_s.empty: price_result[ticker] = float(c_s.iloc[-1])
        except Exception: pass
    return price_result

def _avg_last_return(tickers):
    rets = []
    for t in tickers:
        buf = price_buffers.get(t)
        if buf and len(buf) >= 2 and buf[-2] != 0:
            rets.append((buf[-1] - buf[-2]) / buf[-2])
    return float(np.mean(rets)) if rets else 0.0

def compute_delta_i_hat(E_t, T_t, K, insider_flag=0):
    c = MODEL_COEFFICIENTS
    return c["beta0"] + (c["beta1"] * E_t) + (c["beta2"] * (E_t * K)) + (c["beta3"] * T_t) + (c.get("beta4", 0) * insider_flag)

def get_compounding_multiplier(war_snap, infra_snap, osint_snap):
    hits = 0
    texts = [h[1]['text'] for h in war_snap] + [h[1]['text'] for h in infra_snap] + [h[1]['text'] for h in osint_snap]
    for text in texts:
        t_low = text.lower()
        for kw in COMPOUNDING_CHOKEPOINTS:
            if kw in t_low: hits += 1
    return 1.0 + (hits * 0.15) 

def compute_composite_index(K, I_score, L, weather_count, flight_count, prediction_momentum, C_t):
    w = COMPOSITE_WEIGHTS
    base = (w["kinetic"] * K + w["infrastructure"] * I_score + w["labor"] * L + 
            w["flights"] * min(flight_count / 10.0, 1.0) + w["weather"] * min(weather_count / 20.0, 1.0) + 
            w["prediction_markets"] * prediction_momentum)
    score = min(base * C_t, 1.0)
    if score > 0.8: trigger_alert_sound("warning")
    return score

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

def calculate_sector_prediction(view_key, E_t, T_t, C_t, composite, K):
    if view_key == 'R': return (K * 0.40) + (C_t * 0.35) - (E_t * 20), "MONITOR ESCALATION", YELLOW, "Russian commodity channels sensitive to K and E_t."
    if view_key == 'A': return (C_t * 0.20) + (composite * 0.40) - 0.1, "DEFENSIVE HEDGE", RED, "China exposure heavily weighted by macro composite."
    if view_key == 'J': return (-composite * 0.15) + (T_t * 15), "NEUTRAL", CYAN, "Japan isolated from direct kinetic shocks; logistics dependent."
    if view_key == 'E': return (E_t * 65) + (C_t * 0.25), "BUY ENERGY", GREEN, "Middle East energy matrices tracking E_t."
    if view_key == 'N': return (T_t * 35) + (composite * 0.10), "ACCUMULATE", GREEN, "India/South Asia supply chain growth."

    score = 0.0
    reason = "Nominal drift."
    if view_key == "1":
        score = -composite if composite > 0.5 else 0.2
        reason = "Systemic friction drag." if composite > 0.5 else "Stable macro conditions."
    elif view_key == "2":
        score = (E_t * 50) + (C_t * 0.1)
        reason = f"Energy shocks active (C_t {C_t:.2f})." if score > 0.2 else "Energy channels clear."
    elif view_key == "3":
        score = -C_t * 0.2
        reason = f"Chokepoint vulnerabilities elevating risk profile."
    elif view_key == "4":
        score = 0.1
        reason = "Defensive yield accumulation."
    elif view_key == "5":
        score = -composite * 1.5
        reason = "High friction suggests bond yield compression/volatility."
    elif view_key == "6":
        score = (T_t * 50) - (C_t * 0.15)
        reason = "Logistical friction compressing margins." if score < 0 else "Transit operational."
    elif view_key == "7":
        score = composite * 0.8
        reason = "Friction driving safe-haven asset accumulation."
    elif view_key == "8":
        score = composite * 0.5
        reason = "Decentralized liquidity absorbing fiat friction."
    elif view_key == "9":
        score = -C_t * 0.3
        reason = "Advanced tech supply chain chokepoint threat." if C_t > 1.2 else "Growth nominal."
    elif view_key == "0":
        score = composite * 0.2
        reason = "Tracking weather/fertilizer supply chain impacts."
    score = max(-1.0, min(score, 1.0))
    action = "STRONG BUY" if score > 0.3 else ("BUY" if score > 0.1 else ("STRONG SELL" if score < -0.3 else ("SELL" if score < -0.1 else "NEUTRAL")))
    color = GREEN if score > 0.1 else (RED if score < -0.1 else CYAN)
    return score, action, color, reason

def load_prediction_history():
    history = []
    if PREDICTION_AUDIT_PATH.exists():
        try:
            with open(PREDICTION_AUDIT_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip(): history.append(json.loads(line.strip()))
        except Exception: pass
    return history[-1:]

# ---------------------------------------------------------
# MAIN UI LOOP
# ---------------------------------------------------------
def main():
    global current_view, last_batch_sync_time, zoom_level, graph_overlay_mode
    global global_E_t, global_T_t, global_K, global_L, global_I_score, global_C_t, global_composite
    global map_cursor_row, map_cursor_col, selected_node_info
    global last_known_prices
    
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"{CYAN} PROJECT HANNIBAL V4.3 — ABSOLUTE SINGULARITY | ALL SYSTEMS GO {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    
    threading.Thread(target=background_ollama_bootstrapper, name="Ollama-Bootstrapper", daemon=True).start()
    threading.Thread(target=osint_sync_daemon, name="Batch-Orchestrator", daemon=True).start()
    threading.Thread(target=fast_poll_daemon, name="Fast-Telemetry", daemon=True).start()

    price_backoff = PRICE_POLL_BASE_SECONDS
    last_price_fetch = 0.0
    last_prediction_run = 0.0

    try:
        while True:
            timestamp = datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST")
            now_mono = time.monotonic()
            
            with view_lock: view_key = current_view
            active_tickers = MARKETS.get(view_key, MARKETS["1"])["tickers"] if view_key in MARKETS else ["BZ=F", "^GSPC", "BTC-USD"]
            view_name = MARKETS.get(view_key, MARKETS["1"])["name"] if view_key in MARKETS else f"SPECIALIZED PAGE: [{view_key}]"

            if now_mono - last_price_fetch >= price_backoff:
                fresh_prices = fetch_prices_and_volume(all_possible_tickers)
                last_price_fetch = now_mono
                if fresh_prices:
                    for ticker, price in fresh_prices.items():
                        last_known_prices[ticker] = price
                        price_buffers[ticker].append(price)
                    price_backoff = PRICE_POLL_BASE_SECONDS
                else:
                    price_backoff = min(price_backoff * 1.5, PRICE_POLL_MAX_BACKOFF)

            with state_lock:
                w_snap = list(weather_alerts)
                f_snap = list(flight_disruptions)
                war_snap = list(kinetic_headlines)
                infra_snap = list(infra_atomic_headlines)
                l_snap = list(labor_headlines)
                ins_detail_snap = list(insider_details)
                osint_snap = list(osint_data)
                p_snap = list(prediction_markets)
                insider_flag_snap = large_insider_trade_flag

            global_K = 1 if len(war_snap) > 0 else 0
            global_I_score = 1 if len(infra_snap) > 0 else 0
            global_L = 1 if len(l_snap) > 0 else 0
            global_E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
            global_T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
            
            prediction_momentum, _ = compute_prediction_momentum()
            global_C_t = get_compounding_multiplier(war_snap, infra_snap, osint_snap)
            delta_hat = compute_delta_i_hat(global_E_t, global_T_t, global_K, insider_flag_snap)
            global_composite = compute_composite_index(global_K, global_I_score, global_L, len(w_snap), len(f_snap), prediction_momentum, global_C_t)

            if now_mono - last_prediction_run >= 90.0:
                threading.Thread(target=run_napalm_engine, args=(global_composite, global_C_t, global_K, global_E_t, global_T_t, war_snap, infra_snap, ins_detail_snap, p_snap, osint_snap), daemon=True).start()
                last_prediction_run = now_mono

            clear_screen()
            print(f"{BLUE}========================================================================================={RESET}")
            print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | SINGULARITY ONLINE {RESET}")
            print(f"{BLUE}========================================================================================={RESET}")

            if view_key in MARKETS:
                p_score, p_action, p_color, p_reason = calculate_sector_prediction(view_key, global_E_t, global_T_t, global_C_t, global_composite, global_K)
                print(f"{PURPLE}  SECTOR PREDICTION & RATIONALE ALGORITHM{RESET}")
                print(f"{BLUE}-----------------------------------------------------------------------------------------{RESET}")
                print(f"  TARGET SECTOR: {view_name}")
                print(f"  RECOMMENDATION: {p_color}{p_action} ({p_score:+.2f} Vector Score){RESET}")
                print(f"  AI RATIONALE: {GRAY}{p_reason}{RESET}")
                print(f"  VELOCITY GRAPH: [{p_color}{generate_bar_chart(p_score, width=30)}{RESET}]")
                print(f"  {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")

                k_str = f"{RED}ACTIVE ({len(war_snap)} alerts){RESET}" if global_K else f"{GRAY}inactive{RESET}"
                print(f" {GRAY}MODEL STATE:{RESET} E_t={global_E_t:+.4f}  T_t={global_T_t:+.4f}  K={k_str}  C_t={global_C_t:.2f}  delta_I_hat={delta_hat:+.5f}")
                print(f" {'TICKER':<8} | {'PRICE':<9} | {'DELTA (%)':<9} | {'Z-SCORE':<7} | {'MOMENTUM':<{SPARK_WIDTH}} | {'STATUS':<15}")
                print("-" * 89)

                for ticker in active_tickers:
                    buf = price_buffers[ticker]
                    curr_price = last_known_prices.get(ticker)
                    pct_change = (buf[-1] - buf[-2]) / buf[-2] if curr_price and len(buf) > 2 and buf[-2] else 0.0
                    z_score = (curr_price - np.mean(list(buf)[:-1])) / (np.std(list(buf)[:-1]) if np.std(list(buf)[:-1]) != 0 else 1.0) if curr_price and len(buf) > 2 else 0.0
                    sparkline = generate_sparkline(buf)
                    if curr_price is None:
                        print(f"  {ticker:<7} | {'NO DATA':<9} | {'--':>9} | {'--':>7} | {sparkline} | {'OFFLINE':<15}")
                        continue
                    abs_c = abs(pct_change)
                    diamond, color, status = " ", CYAN, "NOMINAL"
                    if abs_c >= EXTREME_THRESH: color, diamond, status = PURPLE, "♦", "♦ VOLATILITY"
                    elif abs_c >= HIGH_THRESH: color, status = (RED, "CRIT. DROP") if pct_change < 0 else (GREEN, "SURGE ALERT")
                    print(f"{color} {diamond}{ticker:<7} | {curr_price:<9.2f} | {pct_change*100:>8.2f}% | {z_score:>7.2f} | {sparkline} | {status:<15}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  GLOBAL RADAR SYSTEMS & FRICTION FEED (Composite: {global_composite:.2f} | C_t: {global_C_t:.2f}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'LATEST INFRA/ATOMIC & LABOR ALERTS':<65} | {'DATA':<8} | {'PREDICTION'}")
                print("-" * 89)
                if global_I_score:
                    for _, inf in infra_snap[:2]: print(f" {inf['text']:<65} | {inf['nums']:<8} | {inf['prediction']}")
                else: print(f" {GREEN}>> INFRA/ATOMIC RADAR: NOMINAL (0 EVENTS){RESET}")

            elif view_key == "M":
                print(f"{PURPLE}  WORLDWIDE INTERACTIVE TACTICAL MAP & CURSOR INSPECTION{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(render_interactive_ascii_map(zoom_level, war_snap, infra_snap))

            elif view_key == "G":
                print(f"{PURPLE}  HANNIBAL GRAPH MATRIX: PREDICTIVE SECTOR VELOCITIES (Mode: {graph_overlay_mode}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"  [O] TOGGLE OVERLAY MODE (1: Momentum | 2: Volume Z-Score | 3: Friction Impact)\n")
                for key, data in MARKETS.items():
                    s, a, c, r = calculate_sector_prediction(key, global_E_t, global_T_t, global_C_t, global_composite, global_K)
                    if graph_overlay_mode == 2:
                        disp_val = s * 1.5
                        disp_bar = generate_bar_chart(disp_val, max_val=1.5, width=40)
                    elif graph_overlay_mode == 3:
                        disp_val = s * global_C_t
                        disp_bar = generate_bar_chart(disp_val, max_val=2.0, width=40)
                    else:
                        disp_val = s
                        disp_bar = generate_bar_chart(s, width=40)
                    print(f"  {c}{data['name']:<30}{RESET} [{c}{disp_bar}{RESET}] {disp_val:+.2f} ({a})")

            elif view_key == "I":
                print(f"{PURPLE}  CRITICAL INFRASTRUCTURE, POWER GRIDS & ATOMIC AGENCY (IAEA) RADAR{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" Infra Severity Index : {RED if global_I_score else GREEN}{'HIGH VULNERABILITY' if global_I_score else 'NOMINAL / MONITORED'}{RESET}")
                if not infra_snap:
                    print(f" {GRAY}No critical grid, subsea, or atomic incidents active.{RESET}")
                else:
                    for idx, h in enumerate(infra_snap[:25], 1):
                        color = RED if any(w in h[1]['text'].lower() for w in ["scram", "explosion", "blackout", "breach", "iaea"]) else YELLOW
                        print(f" [{idx:02d}] {color}⚡ INFRA/ATOMIC:{RESET} {h[1]['text']}")

            elif view_key == "P":
                print(f"{PURPLE}  PROJECT HANNIBAL: RED/BLUE ADVERSARIAL NEURAL ENGINE{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                print(f" Neural Bridge Status          : {CYAN}{llm_status_string}{RESET}")
                print(f" Composite Friction Score      : {global_composite:.2f} / 1.00")
                print(f" Cascading Threat Index (C_t)  : {global_C_t:.2f}")
                print(f"\n {YELLOW}--- DUAL-AGENT SYNTHESIS (BLUE / RED TEAM) ---{RESET}")
                print(f" {CYAN}{napalm_active_synthesis}{RESET}")
                print(f"\n {YELLOW}--- ADVERSARIAL SECTOR ADVISORIES ---{RESET}")
                if not napalm_advisories:
                    print(f" {GRAY}Awaiting dual-agent JSON response from HANNIBAL engine...{RESET}")
                else:
                    for adv in napalm_advisories:
                        try:
                            color = GREEN if "BUY" in adv['action'].upper() or "LONG" in adv['action'].upper() else (RED if "SHORT" in adv['action'].upper() or "HEDGE" in adv['action'].upper() else CYAN)
                            print(f" {color}[{adv['sector']}]{RESET} {adv['action']} -> {adv['asset']}")
                            print(f"   ↳ {GRAY}Reason: {adv['reason']}{RESET}")
                            print(f"   ↳ {PURPLE}Outcome: {adv['outcome']}{RESET}")
                        except Exception: pass

            elif view_key == "T":
                print(f"{PURPLE}  NEURAL UPLINK: DIRECT TACTICAL CONVERSATION WITH HANNIBAL{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}Connecting to {OLLAMA_MODEL}... Terminal UI updates paused during uplink.{RESET}\n")
                
                for msg in napalm_chat_history[-10:]:
                    if msg["role"] == "user":
                        print(f" {GREEN}[COMMANDER]{RESET} > {msg['content']}")
                    else:
                        print(f" {CYAN}[HANNIBAL]{RESET} > {msg['content']}\n")
                
                print(f"\n{BLUE}========================================================================================={RESET}")
                user_input = input(f"{YELLOW} [UPLINK ACTIVE] Enter Query (or blank to cancel): {RESET}")
                if user_input.strip():
                    threading.Thread(target=process_chat_query, args=(user_input, global_composite, global_C_t, war_snap, ins_detail_snap)).start()
                with view_lock: current_view = '1'

            elif view_key == "S":
                print(f"{PURPLE}  SEC EDGAR — DETAILED INSIDER TRANSACTIONS (Form 4){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}General market-wide monitor — not aimed at any single named person.{RESET}")
                print(f" {GRAY}Large-trade threshold: >= ${LARGE_INSIDER_TRADE_USD:,.0f} on an open-market P/S code.{RESET}\n")
                if not ins_detail_snap:
                    print(f" {GRAY}Awaiting per-filing detail fetches (up to {FORM4_DETAIL_MAX_PER_BATCH}/batch)...{RESET}")
                else:
                    print(f" {'ISSUER':<8} | {'OWNER':<22} | {'ROLE':<18} | {'CODE':<10} | {'SHARES':>10} | {'PRICE':>9} | {'VALUE ($)':>14} | {'PREDICTION'}")
                    print("-" * 115)
                    for _, tx in list(ins_detail_snap)[-20:]:
                        big = tx["value"] >= LARGE_INSIDER_TRADE_USD and tx["code"] in OPEN_MARKET_CODES
                        color = PURPLE if big else (GREEN if tx["acquired_disposed"] == "A" else RED)
                        marker = "⚠" if big else " "
                        print(f"{color}{marker}{tx['issuer']:<7} | {tx['owner'][:22]:<22} | {tx['role'][:18]:<18} | "
                              f"{tx['code_label'][:10]:<10} | {tx['shares']:>10,.0f} | ${tx['price']:>7,.2f} | "
                              f"${tx['value']:>12,.0f} | {tx['prediction']}{RESET}")

            elif view_key == "K":
                print(f"{PURPLE}  WORLDWIDE MULTI-THEATER KINETIC ESCALATION & STRIKE MAPPING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_stat = f"{RED}ACTIVE ({len(war_snap)} alerts){RESET}" if global_K else f"{GRAY}INACTIVE / STANDBY{RESET}"
                print(f" Kinetic Escalation Flag (K): {k_stat} | Cascading Threat Index (C_t): {global_C_t:.2f}")
                print(f" Incoming Worldwide Strike & Military Buffer (SQLite Persistent):\n")
                if not war_snap: print(f" {GRAY}No kinetic alerts in active buffer.{RESET}")
                for idx, wh in enumerate(war_snap[:20], 1): 
                    age_str = f"{int(time.time() - wh[0])}s"
                    print(f" [{idx:02d}] [{age_str:>3}] {RED}♦ THEATER INTEL:{RESET} {wh[1]['text']}")

            elif view_key == "C":
                print(f"{PURPLE}  4X DECENTRALIZED PREDICTION MARKETS & GLOBAL CONSENSUS{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'PLATFORM':<12} | {'MARKET / QUESTION':<60} | {'YES PRICE':<10} | {'VOLUME (24H)'}")
                print("-" * 89)
                if not p_snap: print(f" {GRAY}Aggregating decentralized consensus feeds...{RESET}")
                else:
                    for m in sorted(p_snap, key=lambda x: x.get('volume', 0), reverse=True)[:25]:
                        yp = f"{m['yes_price']*100:.1f}¢" if m['yes_price'] is not None else "N/A"
                        color = CYAN if m['platform'] == 'Polymarket' else (YELLOW if m['platform'] == 'Kalshi' else (GREEN if m['platform'] == 'PredictIt' else PURPLE))
                        print(f" {color}{m['platform']:<12}{RESET} | {m['question'][:60]:<60} | {yp:<10} | ${m.get('volume', 0):,.0f}")

            elif view_key == "W":
                print(f"{PURPLE}  DETAILED METEOROLOGICAL & AIRSPACE DISRUPTION TELEMETRY{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}NWS Severe Weather Alerts ({len(w_snap)} active):{RESET}")
                for alert in w_snap[:15]: print(f"   • [{alert[1]['severity']}] {alert[1]['event']} — {alert[1]['area'][:65]}")
                print(f"\n {GRAY}FAA Airspace Disruption & Ground Stops ({len(f_snap)} entries):{RESET}")
                for fs in f_snap[:15]: print(f"   • Airport: {fs[1]['airport']} | Type: {fs[1]['category']} | Reason: {fs[1]['reason'][:50]}")

            elif view_key == "H":
                print(f"{PURPLE}  BACKTEST AUDIT TRAIL: HISTORICAL PREDICTION DOSSIER REPLAY{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                history = load_prediction_history()
                if not history: print(f" {GRAY}No prediction history recorded yet.{RESET}")
                else:
                    for idx, item in enumerate(history[-5:], 1):
                        print(f" [{idx}] {YELLOW}TIMESTAMP: {item['timestamp']}{RESET} | Composite: {item.get('composite', 'N/A')} | C_t: {item.get('C_t', '1.00')}")
                        print(f"     {CYAN}Synthesis: {item.get('napalm_synthesis', '')}{RESET}")

            # ---------------------------------------------------------
            # FOOTER / ENGINE STATUS
            # ---------------------------------------------------------
            if view_key != "T":
                time_since_batch = time.time() - last_batch_sync_time
                next_batch_in = max(OSINT_BATCH_INTERVAL_SECONDS - time_since_batch, 0)
                
                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  HANNIBAL V4.3 SINGULARITY STATUS (Composite Index: {global_composite:.2f} | C_t: {global_C_t:.2f}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}Telemetry Pipeline:{RESET} SQLite DB Active -> Fast-Poll (15s) + Batch Sync (90s)")
                print(f" {GRAY}Next Ingestion Wave:{RESET} {next_batch_in:.0f}s")
                
                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{YELLOW} [1-0/R/A/J/E/N] Markets | [M] Map | [G] Graphs | [W] Weather/FAA | [K] War | [I] Infra/Atomic | [S] Insider $ | [P] HANNIBAL AI | [T] Talk | [H] Audit {RESET}")

                for _ in range(int(max(1, POLL_INTERVAL * 10))):
                    if IS_WINDOWS and _kbhit():
                        key = _getch()
                        if key in MARKETS or key in ['w', 'k', 'i', 'c', 'p', 'h', 'g', 'm', 's', 't', 'r', 'a', 'j', 'e', 'n']:
                            with view_lock: current_view = key.upper()
                            break
                        elif key == 'w' and current_view == 'M':
                            map_cursor_row = max(0, map_cursor_row - 1)
                            break
                        elif key == 's' and current_view == 'M':
                            map_cursor_row = min(11, map_cursor_row + 1)
                            break
                        elif key == 'a' and current_view == 'M':
                            map_cursor_col = max(0, map_cursor_col - 1)
                            break
                        elif key == 'd' and current_view == 'M':
                            map_cursor_col = min(75, map_cursor_col + 1)
                            break
                        elif key == 'o' and current_view == 'G':
                            graph_overlay_mode = (graph_overlay_mode % 3) + 1
                            break
                    time.sleep(0.1)

    except KeyboardInterrupt:
        try:
            logger.info("Terminal stopped by operator.")
        except Exception:
            pass
        print(f"\n{CYAN}[+] HANNIBAL V4.3 Singularity terminal disengaged safely by operator.{RESET}")

if __name__ == "__main__":
    main()