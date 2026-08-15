"""
OSINT Command Terminal — PROJECT NAPALM V2.3 (Autonomous Deep-Scan)
==========================================================================
Clearance: EID Verified (B. Noffels, 6/24/2026)
Architecture Upgrades:
- DEEP DRIVE SCANNER: Recursively walks F:\ and system drives to locate ollama.exe.
- NON-BLOCKING ASYNC BOOTSTRAP: Terminal UI loads instantly while LLM spins up in background.
- DYNAMIC ALL-POINTS TACTICAL MAP [M]: Real-time plotting of active telemetry nodes.
- INTERACTIVE GRAPH MATRIX [G]: Multi-option overlays (momentum, volume, friction).
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
LARGE_INSIDER_TRADE_LOG_PATH = SCRIPT_DIR / "large_insider_trades.csv"
PREDICTION_AUDIT_PATH = SCRIPT_DIR / "prediction_audit.txt"

logging.basicConfig(filename=str(LOG_PATH), level=logging.INFO, format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")
logger = logging.getLogger("osint_terminal")

# ---------------------------------------------------------
# PLATFORM HANDLING
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
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

# ---------------------------------------------------------
# UI COLOR PALETTE
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
UNUSUAL_VOLUME_Z = 3.0
MARKET_CLOSE_HOUR_ET = 16
LATE_SESSION_WINDOW_MINUTES = 30

ET_ZONE = ZoneInfo("America/New_York")
LOCAL_ZONE = ZoneInfo("Europe/Lisbon")

COMPOSITE_WEIGHTS = {"kinetic": 0.20, "infrastructure": 0.20, "labor": 0.10, "flights": 0.10, "weather": 0.10, "prediction_markets": 0.30}
COMPOUNDING_CHOKEPOINTS = ["hormuz", "bab el-mandeb", "suez", "malacca", "panama canal", "bosphorus", "tsmc", "subsea cable", "lng terminal", "uranium enrichment", "iaea", "nuclear reactor", "black sea grain"]

FORM4_DETAIL_MAX_PER_BATCH = 10
LARGE_INSIDER_TRADE_USD = 1_000_000
OPEN_MARKET_CODES = {"P": "PURCHASE", "S": "SALE"}

PREDICTION_DISCLAIMER = "Experimental model output under hand-picked, unfit weights — NOT financial advice."

llm_status_string = "Initializing local neural bridge..."

# ---------------------------------------------------------
# AUTONOMOUS RECURSIVE F-DRIVE & SYSTEM OLLAMA SCANNER
# ---------------------------------------------------------
def recursive_find_ollama():
    """Recursively walks system drives (prioritizing F: and C:) to locate ollama.exe."""
    drives = ["F:", "C:", "D:", "E:", "G:"]
    for drv in drives:
        if not os.path.exists(drv):
            continue
        logger.info(f"Scanning drive {drv} for ollama.exe...")
        try:
            for root, dirs, files in os.walk(drv):
                if "ollama.exe" in files:
                    found_path = os.path.join(root, "ollama.exe")
                    return found_path
        except Exception:
            continue
    return None

def background_ollama_bootstrapper():
    global llm_status_string
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags", headers={"User-Agent": "NAPALM"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                llm_status_string = "Llama 3 Active (Localhost:11434)"
                return
    except Exception:
        pass

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
                except Exception:
                    continue
        except Exception as e:
            llm_status_string = f"Ollama launch error: {e}"
    else:
        llm_status_string = "Ollama binary not found on drives. Running heuristic bypass."

# ---------------------------------------------------------
# INTERACTIVE ASCII MAPS WITH DYNAMIC NODE PLOTTING
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
    ],
    [
        r" +----------------------------------------------------------------------------+ ",
        r" |      .---.      _..._      .-.      .-..-..-.      _..._      .-.        | ",
        r" |    .'     '.  .'     '.  .'   '.  .'        '.  .'     '.  .'   '.       | ",
        r" |   /   FR    \/   GER   \/   UKR \/     RUS    \/         \/       \      | ",
        r" |  |    IT    |    ITA   | BLK SEA|     CASP   |           |         |     | ",
        r" |   \  ESP    /\ MEDITERR/\       /\            /\         /\        /     | ",
        r" |    '.     .'  '.     .'  '.   .'  '.        .'  '.     .'  '.    .'      | ",
        r" |      '---'      '---'      '-'      '-..-..-'      '---'      '-'        | ",
        r" |         \       ALG         /          |         ISR  |                  | ",
        r" |          '-.,__,.-'~'-.,__,'           |     RED SEA  |                  | ",
        r" |                                        |    YEM       |                  | ",
        r" +----------------------------------------------------------------------------+ "
    ],
    [
        r" +========================================================================+ ",
        r" |  [TACTICAL GRID: INDO-PACIFIC / EASTERN EUROPE / MIDDLE EAST CHOKEPOINTS]| ",
        r" |                                                                        | ",
        r" |    [N 50°]   UKR FRONT                 |               RUS FAR EAST    | ",
        r" |                                        |                               | ",
        r" |    [N 35°]          BLK SEA            |                               | ",
        r" |                                        |      EAST CHINA SEA           | ",
        r" |                                        |                               | ",
        r" |    [N 20°]    RED SEA     HORMUZ       |                               | ",
        r" |                                        |         TAIWAN STRAIT         | ",
        r" |                                        |                               | ",
        r" |    [N 05°]        BAB EL-MANDEB        |     SOUTH CHINA SEA           | ",
        r" +========================================================================+ "
    ]
]
zoom_level = 0
graph_overlay_mode = 1 

# ---------------------------------------------------------
# DIRECT RAW XML FEED REGISTRY
# ---------------------------------------------------------
API_ENDPOINTS = {
    "NWS_WEATHER": "https://api.weather.gov/alerts/active?severity=Severe,Extreme",
    "FAA_STATUS": "https://nasstatus.faa.gov/api/airport-status-information",
    "SEC_FORM4": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=100&output=atom",
    "POLYMARKET": "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&order=volume24hr&ascending=false",
    "KALSHI": "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&status=open",
    "PREDICTIT": "https://www.predictit.org/api/marketdata/all/",
    "MANIFOLD": "https://api.manifold.markets/v0/markets?limit=100"
}

DIRECT_WIRES = [
    {"type": "rss", "category": "worldwide_wire", "region": "BBC", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"type": "rss", "category": "worldwide_wire", "region": "AL_JAZEERA", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"type": "rss", "category": "worldwide_wire", "region": "NYT", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
    {"type": "rss", "category": "worldwide_wire", "region": "UN_NEWS", "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml"},
    {"type": "rss", "category": "kinetic", "region": "DEFENSE_NEWS", "url": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"},
    {"type": "rss", "category": "kinetic", "region": "RT", "url": "https://www.rt.com/rss/news/"},
    {"type": "rss", "category": "macro", "region": "YAHOO_FIN", "url": "https://finance.yahoo.com/news/rss"},
    {"type": "rss", "category": "macro", "region": "CNBC", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?id=100727362"},
    {"type": "rss", "category": "infrastructure", "region": "IAEA", "url": "https://www.iaea.org/feeds/topnews"},
]

REGIONS = {"US": {"gl": "US", "ceid": "US:en", "hl": "en-US"}, "UK": {"gl": "GB", "ceid": "GB:en", "hl": "en-GB"}}
TOPICS = {
    "kinetic": '"strike" OR "missile" OR "airstrike" OR "military escalation" OR "NATO"',
    "infrastructure": '"power grid" OR "pipeline" OR "subsea cable" OR "nuclear reactor" OR "IAEA"',
    "labor": '"union strike" OR "work stoppage" OR "walkout" OR "labor dispute"',
    "macro": '"interest rates" OR "inflation" OR "bond yield" OR "central bank"',
    "supply_chain": '"port congestion" OR "shipping delay" OR "logistics failure" OR "border closure"'
}

REGIONAL_TOPIC_FEEDS = []
for region, p in REGIONS.items():
    for topic, query in TOPICS.items():
        encoded = urllib.parse.quote(query)
        REGIONAL_TOPIC_FEEDS.append({"type": "rss", "category": topic, "region": region, "url": f"https://www.bing.com/news/search?q={encoded}&format=rss"})
        REGIONAL_TOPIC_FEEDS.append({"type": "rss", "category": topic, "region": region, "url": f"https://news.search.yahoo.com/news/rss?p={encoded}"})

FEED_REGISTRY = [{"type": "api", "category": name, "url": url} for name, url in API_ENDPOINTS.items()] + DIRECT_WIRES + REGIONAL_TOPIC_FEEDS

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
}

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
}
SECTOR_RELEVANCE_CAP = 60  

current_view = "1"
view_lock = threading.Lock()
WINDOW_SIZE = 600
SPARK_WIDTH = 12

state_lock = threading.Lock()
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
sector_headlines = {k: [] for k in MARKETS}  
prediction_markets = []
prev_prediction_prices = {}

napalm_active_synthesis = "NAPALM Engine initializing. Awaiting local LLM generation..."
napalm_advisories = []

global_E_t, global_T_t, global_K, global_L, global_I_score, global_C_t, global_composite = 0.0, 0.0, 0, 0, 0, 1.0, 0.0

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}
last_batch_sync_time = 0.0
last_fast_sync_time = 0.0

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
# DYNAMIC ALL-POINTS TACTICAL MAP RENDERER
# ---------------------------------------------------------
def render_ascii_map(level, war_snap, infra_snap, weather_snap):
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
                detected_nodes[label] = {"row": r, "col": c, "text": text}

    grid = [list(row) for row in MAP_LEVELS[level]]
    for label, info in detected_nodes.items():
        r, c = info["row"], info["col"]
        if 0 <= r < len(grid) and 0 <= c < len(grid[0]):
            marker = '☒' if level == 0 else 'X'
            for idx, char in enumerate(marker):
                if c + idx < len(grid[r]):
                    grid[r][c + idx] = char

    rendered_map = "\n".join(["".join(row) for row in grid])
    sitrep = f"\n{PURPLE}  --- DYNAMIC TACTICAL SITREP & ACTIVE TELEMETRY NODES ---{RESET}\n"
    if not detected_nodes:
        sitrep += f"  {GRAY}Scanning global telemetry streams for geographic coordinates...{RESET}\n"
    else:
        for label, info in list(detected_nodes.items())[:8]:
            sitrep += f"  {RED}[ACTIVE NODE: {label}]{RESET} -> {info['text'][:75]}\n"
    return rendered_map + sitrep

# ---------------------------------------------------------
# OLLAMA AI: FULL NEURAL OVERRIDE (NAPALM)
# ---------------------------------------------------------
def run_napalm_engine(composite, C_t, K, E_t, T_t, war_snap, infra_snap, ins_snap, ins_detail_snap, p_snap, osint_snap):
    global napalm_active_synthesis, napalm_advisories
    intel_war = "\n".join([f"- KINETIC: {h[1]['text']}" for h in war_snap[:3]])
    intel_infra = "\n".join([f"- INFRA: {h[1]['text']}" for h in infra_snap[:3]])
    intel_osint = "\n".join([f"- MACRO: {h[1]['text']}" for h in osint_snap[:3]])
    intel_sec_detail = "\n".join([
        f"- SEC TRANSACTION: {d['owner']} ({d['role']}) {d['code_label']} {d['issuer']} "
        f"{d['shares']:,.0f} sh @ ${d['price']:,.2f} = ${d['value']:,.0f}"
        for _, d in ins_detail_snap[:5]
    ])
    intel_pred = "\n".join([f"- CONSENSUS: [{m['platform']}] {m['question']} (YES: {m['yes_price']*100:.0f}%)" for m in sorted(p_snap, key=lambda x: x.get('volume', 0), reverse=True)[:3]])
    
    prompt = f"""You are PROJECT NAPALM, a geopolitical/macro risk-signal AI.
Evaluate this LIVE telemetry:
Friction: {composite:.2f} | C_t Scalar: {C_t:.2f} | K: {K} | E_t: {E_t:.4f}

INTERCEPTS:
{intel_war}
{intel_infra}
{intel_osint}
{intel_sec_detail}
{intel_pred}

YOUR DIRECTIVE:
1. Provide a 2-sentence synthesis of the global landscape.
2. Generate exactly 5 Investment Advisories based ONLY on the text intercepts above.

FORMAT YOUR RESPONSE EXACTLY LIKE THIS JSON:
{{
  "synthesis": "Macro synthesis.",
  "advisories": [
    {{"sector": "Sector Name", "action": "BUY/SELL/HEDGE", "asset": "Asset/Ticker", "reason": "Detailed reasoning.", "outcome": "Predicted market outcome."}}
  ]
}}"""

    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False}
    required_keys = {"sector", "action", "asset", "reason", "outcome"}

    try:
        req = urllib.request.Request(OLLAMA_ENDPOINT, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_SECONDS) as response:
            result = json.loads(response.read().decode('utf-8'))
            ai_resp = json.loads(result.get("response", "{}"))
            napalm_active_synthesis = ai_resp.get("synthesis", "Synthesis generation active.")
            raw_advisories = ai_resp.get("advisories", [])
            napalm_advisories = [adv for adv in raw_advisories if isinstance(adv, dict) and required_keys.issubset(adv.keys())]
    except Exception as e:
        napalm_active_synthesis = f"NAPALM Engine offline or processing (Localhost:11434). Detail: {e}"

# ---------------------------------------------------------
# HIGH-GRAVITY DISTILLATION FILTER
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
        if any(w in t_clean for w in ["explosion", "missile", "strike", "breach", "scram", "outage", "whale", "war"]): score += 2.0
            
        scored.append((score, ts, item))
        
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [(ts, item) for score, ts, item in scored[:max_items]]

def distill_per_sector(raw_osint_stream, existing_persistence, cap=SECTOR_RELEVANCE_CAP, max_age_seconds=3600):
    now = time.time()
    buckets = {k: [] for k in MARKETS}
    seen_per_sector = {k: set() for k in MARKETS}

    combined = raw_osint_stream + [(ts, item) for sector_list in existing_persistence.values() for ts, item in sector_list]
    for ts, item in combined:
        if now - ts > max_age_seconds: continue
        text = item if isinstance(item, str) else item.get('text', '')
        t_clean = re.sub(r'\[.*?\]', '', text).strip().lower()
        if len(t_clean) < 10: continue
        sectors = tag_sectors(t_clean)
        if not sectors: continue
        score = 1.0 - ((now - ts) / 3600.0)
        for cp in COMPOUNDING_CHOKEPOINTS:
            if cp in t_clean: score += 2.5
        if any(w in t_clean for w in ["explosion", "missile", "strike", "breach", "scram", "outage", "war"]): score += 2.0
        for sector in sectors:
            if t_clean in seen_per_sector[sector]: continue
            seen_per_sector[sector].add(t_clean)
            buckets[sector].append((score, ts, item))

    result = {}
    for sector, scored in buckets.items():
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        result[sector] = [(ts, item) for score, ts, item in scored[:cap]]
    return result

# ---------------------------------------------------------
# DPI LAYER: INSTANT FAST-POLL (WEATHER & FAA)
# ---------------------------------------------------------
def fetch_fast_telemetry():
    global weather_alerts, flight_disruptions
    w_data = safe_request(API_ENDPOINTS["NWS_WEATHER"], is_api=True)
    f_data = safe_request(API_ENDPOINTS["FAA_STATUS"], is_api=False)
    
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
# DPI LAYER: BATCH SYNC ENGINE
# ---------------------------------------------------------
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
            elif category == "KALSHI":
                for m in json.loads(raw_data).get("markets", []):
                    yb = m.get("yes_bid")
                    results["data"].append((now, {"platform": "Kalshi", "question": m.get("title", m.get("ticker", "Unknown")), "yes_price": (yb / 100.0) if yb else 0.5, "volume": float(m.get("volume") or 0)}))
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
        else:
            return []
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

def tag_sectors(text):
    t = text.lower()
    return [sector for sector, kws in SECTOR_KEYWORDS.items() if any(kw in t for kw in kws)]

def sync_osint_feeds():
    global last_batch_sync_time, osint_data, insider_filings, prediction_markets, kinetic_headlines, infra_atomic_headlines, labor_headlines, sector_headlines, large_insider_trade_flag

    last_batch_sync_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        future_to_feed = {executor.submit(process_feed, feed): feed for feed in FEED_REGISTRY}
        
        raw_osint, raw_war, raw_infra, raw_labor = [], [], [], []
        temp_insider, temp_predictions = [], []

        for future in concurrent.futures.as_completed(future_to_feed):
            res = future.result()
            if not res: continue
            cat, data = res["feed_config"]["category"], res["data"]
            if cat == "SEC_FORM4": temp_insider.extend(data)
            elif cat in ["POLYMARKET", "KALSHI"]: temp_predictions.extend(data)
            elif res["feed_config"]["type"] == "rss":
                for ts, title in data:
                    region_tag = res["feed_config"].get('region', 'GLOBAL')
                    nums = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                    nums_str = nums[0] if nums else "N/A"
                    
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
        sector_headlines = distill_per_sector(raw_osint, sector_headlines)
        if temp_predictions: prediction_markets = [item[1] for item in temp_predictions]

    if newly_seen_links:
        new_large_trade = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as detail_executor:
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
        with state_lock: large_insider_trade_flag = 1 if new_large_trade else 0

def osint_sync_daemon():
    while True:
        sync_osint_feeds()
        time.sleep(OSINT_BATCH_INTERVAL_SECONDS)

# ---------------------------------------------------------
# PRICES & CALCULUS
# ---------------------------------------------------------
def fetch_prices_and_volume(tickers):
    price_result, volume_result = {}, {}
    try:
        with open(os.devnull, 'w') as devnull:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = devnull, devnull
            try: data = yf.download(tickers, period="5d", interval="1m", progress=False)
            finally: sys.stdout, sys.stderr = old_stdout, old_stderr
    except Exception: return price_result, volume_result

    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                c_s = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
            else:
                c_s = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
            if not c_s.empty: price_result[ticker] = float(c_s.iloc[-1])
        except Exception: pass
    return price_result, volume_result

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
    return min(base * C_t, 1.0)

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

def calculate_sector_prediction(view_key, E_t, T_t, C_t, composite):
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
    
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"{CYAN} PROJECT NAPALM V2.3 | AUTONOMOUS DRIVE SCANNER | INSTANT UI BOOT {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    
    # Launch Ollama bootstrapper asynchronously so UI loads instantly
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
                fresh_prices, _ = fetch_prices_and_volume(all_possible_tickers)
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
                sector_snap = dict(sector_headlines)
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
                threading.Thread(target=run_napalm_engine, args=(global_composite, global_C_t, global_K, global_E_t, global_T_t, war_snap, infra_snap, [], ins_detail_snap, p_snap, osint_snap), daemon=True).start()
                last_prediction_run = now_mono

            clear_screen()
            print(f"{BLUE}========================================================================================={RESET}")
            print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | ONLINE {RESET}")
            print(f"{BLUE}========================================================================================={RESET}")

            # ---------------------------------------------------------
            # MULTI-PAGE RENDER
            # ---------------------------------------------------------
            if view_key in MARKETS:
                p_score, p_action, p_color, p_reason = calculate_sector_prediction(view_key, global_E_t, global_T_t, global_C_t, global_composite)
                print(f"{PURPLE}  SECTOR PREDICTION & RATIONALE ALGORITHM{RESET}")
                print(f"{BLUE}-----------------------------------------------------------------------------------------{RESET}")
                print(f"  TARGET SECTOR: {view_name}")
                print(f"  RECOMMENDATION: {p_color}{p_action} ({p_score:+.2f} Vector Score){RESET}")
                print(f"  AI RATIONALE: {GRAY}{p_reason}{RESET}")
                print(f"  VELOCITY GRAPH: [{p_color}{generate_bar_chart(p_score, width=30)}{RESET}]")
                print(f"  {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                sector_news = sector_snap.get(view_key, [])
                if sector_news:
                    top_ts, top_item = sector_news[0]
                    print(f"  {GRAY}Top relevant headline ({len(sector_news)} ranked this sector):{RESET} {top_item['text'][:75]}")
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

                if global_L:
                    for _, lab in l_snap[:2]: print(f" {lab['text']:<65} | {lab['nums']:<8} | {lab['prediction']}")
                else: print(f" {GREEN}>> LABOR/UNION RADAR: INACTIVE (0 EVENTS){RESET}")

                poly_summary = " | ".join([f"{m['question'][:25]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Polymarket'][:2])
                kalshi_summary = " | ".join([f"{m['question'][:25]} - {int(m['yes_price']*100)}¢" for m in p_snap if m['platform'] == 'Kalshi'][:2])
                if poly_summary: print(f"\n {GRAY}Polymarket Momentum:{RESET} {CYAN}{poly_summary}{RESET}")
                if kalshi_summary: print(f" {GRAY}Kalshi Momentum:{RESET} {YELLOW}{kalshi_summary}{RESET}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  DPI LAYER: SEC EDGAR INSIDER DISCLOSURES (Form 4 Deep XML Extraction){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                if not ins_detail_snap: print(f" {GRAY}Aggregating SEC EDGAR XML payloads...{RESET}")
                else:
                    for _, d in list(ins_detail_snap)[-3:]:
                        title = f"{d['owner'][:20]} ({d['role'][:15]}) - {d['issuer']}"
                        print(f" {title:<65} | ${d['value']:<7,.0f} | {d['prediction']}")

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  WORLDWIDE OSINT HEADLINE RADAR (Persistent Rolling Buffer){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'LATEST WORLDWIDE HEADLINES':<65} | {'DATA':<8} | {'PREDICTION'}")
                print("-" * 89)
                if not osint_snap: print(f" {GRAY}Aggregating worldwide OSINT streams...{RESET}")
                else:
                    for item in osint_snap[:6]:
                        age_str = f"{int(time.time() - item[0])}s"
                        print(f" [{age_str:>3}] {item[1]['text']:<60} | {item[1]['nums']:<8} | {item[1]['prediction']}")

            elif view_key == "M":
                print(f"{PURPLE}  WORLDWIDE DYNAMIC TACTICAL MAP & KINETIC NODE PLOTTING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"  [Z] ZOOM IN | [X] ZOOM OUT | CURRENT ZOOM LEVEL: {zoom_level}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(render_ascii_map(zoom_level, war_snap, infra_snap, w_snap))

            elif view_key == "G":
                print(f"{PURPLE}  NAPALM GRAPH MATRIX: PREDICTIVE SECTOR VELOCITIES (Mode: {graph_overlay_mode}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"  [O] TOGGLE OVERLAY MODE (1: Momentum | 2: Volume Z-Score | 3: Friction Impact)\n")
                
                for key, data in MARKETS.items():
                    s, a, c, r = calculate_sector_prediction(key, global_E_t, global_T_t, global_C_t, global_composite)
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
                
                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"  {GRAY}Matrix weights governed by C_t ({global_C_t:.2f}), K ({global_K}), E_t ({global_E_t:.3f}){RESET}")

            elif view_key == "I":
                print(f"{PURPLE}  CRITICAL INFRASTRUCTURE, POWER GRIDS & ATOMIC AGENCY (IAEA) RADAR{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" Infra Severity Index : {RED if global_I_score else GREEN}{'HIGH VULNERABILITY' if global_I_score else 'NOMINAL / MONITORED'}{RESET}")
                print(f" Active Alerts        : {len(infra_snap)} verified events in persistent buffer.\n")
                if not infra_snap:
                    print(f" {GRAY}No critical grid, subsea, or atomic incidents active.{RESET}")
                else:
                    for idx, h in enumerate(infra_snap[:25], 1):
                        color = RED if any(w in h[1]['text'].lower() for w in ["scram", "explosion", "blackout", "breach", "iaea"]) else YELLOW
                        age_str = f"{int(time.time() - h[0])}s"
                        print(f" [{idx:02d}] [{age_str:>3}] {color}⚡ INFRA/ATOMIC:{RESET} {h[1]['text']}")

            elif view_key == "P":
                print(f"{PURPLE}  PROJECT NAPALM: AUTONOMOUS NEURAL ADVISORY ENGINE{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                print(f" Neural Bridge Status          : {CYAN}{llm_status_string}{RESET}")
                print(f" Composite Friction Score      : {global_composite:.2f} / 1.00")
                print(f" Cascading Threat Index (C_t)  : {global_C_t:.2f}")
                
                print(f"\n {YELLOW}--- NAPALM SYNTHESIS ---{RESET}")
                print(f" {CYAN}{napalm_active_synthesis}{RESET}")

                print(f"\n {YELLOW}--- LIVE AI SECTOR ADVISORIES ---{RESET}")
                if not napalm_advisories:
                    print(f" {GRAY}Awaiting structured JSON response from NAPALM engine...{RESET}")
                else:
                    for adv in napalm_advisories:
                        try:
                            color = GREEN if "BUY" in adv['action'].upper() or "LONG" in adv['action'].upper() else (RED if "SHORT" in adv['action'].upper() or "HEDGE" in adv['action'].upper() else CYAN)
                            print(f" {color}[{adv['sector']}]{RESET} {adv['action']} -> {adv['asset']}")
                            print(f"   ↳ {GRAY}Reason: {adv['reason']}{RESET}")
                            print(f"   ↳ {PURPLE}Outcome: {adv['outcome']}{RESET}")
                        except Exception: pass

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
                print(f"\n {GRAY}Large trades also logged to {LARGE_INSIDER_TRADE_LOG_PATH.name}{RESET}")

            elif view_key == "K":
                print(f"{PURPLE}  WORLDWIDE MULTI-THEATER KINETIC ESCALATION & STRIKE MAPPING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_stat = f"{RED}ACTIVE ({len(war_snap)} alerts){RESET}" if global_K else f"{GRAY}INACTIVE / STANDBY{RESET}"
                print(f" Kinetic Escalation Flag (K): {k_stat} | Cascading Threat Index (C_t): {global_C_t:.2f}")
                print(f" Incoming Worldwide Strike & Military Buffer (Persistent):\n")
                if not war_snap: print(f" {GRAY}No kinetic alerts in the active buffer.{RESET}")
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
                print(f"{PURPLE}  AUDIT TRAIL: LATEST PROJECT NAPALM PREDICTION DOSSIER SNAPSHOT{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                history = load_prediction_history()
                if not history: print(f" {GRAY}No prediction history recorded yet.{RESET}")
                else:
                    item = history[0]
                    print(f" {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                    print(f" {YELLOW}TIMESTAMP: {item['timestamp']}{RESET} | Composite: {item.get('composite', 'N/A')} | C_t: {item.get('C_t', '1.00')}")
                    print(f" {CYAN}Synthesis: {item.get('napalm_synthesis', '')}{RESET}")
                    for adv in item.get('napalm_advisories', []):
                        try: print(f"   [{adv['sector']}] -> {adv['action']} ({adv['asset']})")
                        except Exception: pass

            # ---------------------------------------------------------
            # FOOTER / ENGINE STATUS
            # ---------------------------------------------------------
            time_since_batch = time.time() - last_batch_sync_time
            next_batch_in = max(OSINT_BATCH_INTERVAL_SECONDS - time_since_batch, 0)
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{PURPLE}  NAPALM V2.3 STATUS (Composite Index: {global_composite:.2f} | C_t: {global_C_t:.2f}){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            print(f" {GRAY}Telemetry Pipeline:{RESET} Fast-Poll (15s) + Batch Sync (90s) -> Active")
            print(f" {GRAY}Next Ingestion Wave:{RESET} {next_batch_in:.0f}s | {GRAY}Feeds:{RESET} {len(FEED_REGISTRY)}")
            
            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{YELLOW} [1-0] Markets | [M] Map | [G] Graphs | [W] Weather/FAA | [K] War | [I] Infra/Atomic | [S] Insider $ | [C] Consensus | [P] NAPALM AI | [H] Audit {RESET}")

            for _ in range(int(max(1, POLL_INTERVAL * 10))):
                if IS_WINDOWS and _kbhit():
                    key = _getch()
                    if key in MARKETS or key in ['w', 'k', 'i', 'c', 'p', 'h', 'g', 'm', 's']:
                        with view_lock: current_view = key.upper()
                        break
                    elif key == 'z':
                        zoom_level = min(2, zoom_level + 1)
                        break
                    elif key == 'x':
                        zoom_level = max(0, zoom_level - 1)
                        break
                    elif key == 'o' and current_view == 'G':
                        graph_overlay_mode = (graph_overlay_mode % 3) + 1
                        break
                time.sleep(0.1)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] NAPALM V2.3 terminal disengaged safely by operator.{RESET}")
        logger.info("Terminal stopped by operator.")

if __name__ == "__main__":
    main()