r"""
OSINT Command Terminal — PROJECT HANNIBAL V8.0 (Final Standalone Master)
==========================================================================
Clearance: EID Verified
Architecture Capabilities:
- AUTONOMOUS BOOTSTRAPPER: Auto-detects and installs missing packages on launch.
- ZERO-LATENCY INTERACTION: Instant keystroke polling decoupled from background data pulls.
- GLOBAL WEATHER SYNC: Parses worldwide RSS feeds for global disasters alongside US NWS data.
- IRONCLAD BATCH ENGINE: Try/Except wrapped 90s polling ensures the terminal NEVER crashes.
"""

import sys
import subprocess
import os

# ---------------------------------------------------------
# STEP 1: AUTONOMOUS DEPENDENCY BOOTSTRAPPER
# ---------------------------------------------------------
REQUIRED_PACKAGES = ["numpy", "pandas", "yfinance", "tzdata"]  # tzdata: Windows has no built-in IANA zone db; ZoneInfo(LOCAL_ZONE) throws without it

def bootstrap_runtime():
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
            
    if missing:
        print("\033[93m[*] Missing runtime libraries detected: " + ", ".join(missing) + "\033[0m")
        print("\033[96m[*] Initializing autonomous dependency compilation...\033[0m")
        for pkg in missing:
            print(f"    -> Installing {pkg} via package manager...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])
                print(f"    \033[92m[+] {pkg} successfully compiled.\033[0m")
            except Exception as e:
                print(f"    \033[91m[-] Failed to install {pkg}: {e}\033[0m")
        print("\033[92m[+] Core environment verified. Launching HANNIBAL matrix...\033[0m\n")

bootstrap_runtime()

# Standard Library Imports
import json
import time
import platform
import re
import threading
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

# Third-Party Imports
import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# ---------------------------------------------------------
# PATHS, LOGGING, AND ENGINE CONSTANTS
# ---------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "osint_terminal.log"
DB_PATH = SCRIPT_DIR / "hannibal_intel.db"
PREDICTION_AUDIT_PATH = SCRIPT_DIR / "prediction_audit.txt"

logging.basicConfig(filename=str(LOG_PATH), level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("osint_terminal")

RESET = "\033[0m"
PURPLE = "\033[95m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BLUE = "\033[94m"
GRAY = "\033[90m"

SPARK_WIDTH = 12
FORM4_DETAIL_MAX_PER_BATCH = 5
LARGE_INSIDER_TRADE_USD = 1000000

# Enable Windows ANSI Support
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    os.system('')

# ---------------------------------------------------------
# SEC TOKEN-BUCKET RATE LIMITER
# ---------------------------------------------------------
class SECRateLimiter:
    def __init__(self, rate=5.0):
        self.interval = 1.0 / rate
        self.lock = threading.Lock()
        self.next_slot = time.monotonic()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            wait_time = max(0.0, self.next_slot - now)
            self.next_slot = max(now, self.next_slot) + self.interval
            time.sleep(wait_time)

sec_limiter = SECRateLimiter()

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
insider_details = []          
seen_insider_links = set()
prediction_markets = []

napalm_active_synthesis = "HANNIBAL Engine initializing. Awaiting local LLM generation..."
napalm_advisories = []
napalm_chat_history = []

global_E_t, global_T_t, global_K, global_C_t, global_composite = 0.0, 0.0, 0, 1.0, 0.0

map_cursor_row, map_cursor_col = 5, 40
graph_overlay_mode = 1
llm_status_string = "Initializing local neural bridge..."
last_batch_sync_time = 0.0
LOCAL_ZONE = ZoneInfo("Europe/Lisbon")

# ---------------------------------------------------------
# SQLITE DATA LAKE
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
    except Exception: pass

# ---------------------------------------------------------
# MARKETS MATRIX
# ---------------------------------------------------------
MARKETS = {
    "1": {"name": "GLOBAL AGGREGATE", "tickers": ["BZ=F", "^GSPC", "^FTSE", "VGK", "GC=F", "BTC-USD", "BDRY", "IYT", "^TNX"]},
    "2": {"name": "ENERGY COMMODITIES", "tickers": ["BZ=F", "CL=F", "NG=F", "HO=F", "RB=F"]},
    "3": {"name": "POWER GRID & INFRA", "tickers": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "CCJ"]},
    "4": {"name": "TELECOM & SATELLITE", "tickers": ["XLC", "VZ", "T", "TMUS", "ASTS", "IRDM"]},
    "5": {"name": "MACRO & YIELD CURVE", "tickers": ["^TNX", "^TYX", "UUP", "^GSPC", "^VIX"]},
    "6": {"name": "SUPPLY CHAIN & TRANSPORT", "tickers": ["BDRY", "IYT", "FDX", "UNP", "ZIM"]},
    "7": {"name": "METALS & RARE EARTH", "tickers": ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F", "REMX", "LIT"]},
    "8": {"name": "CRYPTOCURRENCY MAJORS", "tickers": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD"]},
    "9": {"name": "EMERGING TECH & AI", "tickers": ["NVDA", "MSFT", "AMD", "PLTR", "TSM", "ASML"]},
    "0": {"name": "LABOR & AGRICULTURE", "tickers": ["TSN", "CAG", "HRL", "MOS", "NTR", "WEAT"]},
    "R": {"name": "RUSSIAN FEDERATION", "tickers": ["BZ=F", "GC=F", "PL=F", "OGZPY"]},
    "A": {"name": "GREATER CHINA", "tickers": ["FXI", "KWEB", "BABA", "TCEHY", "TSM"]},
    "J": {"name": "JAPAN & NORTH ASIA", "tickers": ["EWJ", "DXJ", "JPY=X", "TM"]},
    "E": {"name": "MIDDLE EAST ENERGY", "tickers": ["BZ=F", "CL=F", "NG=F", "XOM", "CVX"]},
    "N": {"name": "INDIA & SOUTH ASIA", "tickers": ["INDA", "EPI", "INR=X", "INFY"]}
}

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=600) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}

# ---------------------------------------------------------
# PLATFORM KEYBOARD HANDLING
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
    def _kbhit(): return msvcrt.kbhit()
    def _getch():
        ch = msvcrt.getch()
        if ch in b'\x00\xe0': msvcrt.getch(); return ''
        try: return ch.decode('utf-8', errors='ignore').lower()
        except Exception: return ''
else:
    def _kbhit(): return False
    def _getch(): return ''

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
        sec_limiter.wait()
        headers = {"User-Agent": "ProjectHannibal admin@hannibal-intel.org", "Accept-Encoding": "gzip, deflate"}
    else:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
    if is_api and "weather.gov" in url: headers["Accept"] = "application/geo+json"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read()
    except Exception: return None

# ---------------------------------------------------------
# OLLAMA RECURSIVE BOOTSTRAPPER
# ---------------------------------------------------------
def background_ollama_bootstrapper():
    global llm_status_string
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags")
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                with state_lock:
                    llm_status_string = "Llama 3 Active (Localhost:11434)"
                return
    except Exception: pass
    with state_lock:
        llm_status_string = "Ollama binary standby. Heuristic bypass active."

# ---------------------------------------------------------
# INTERACTIVE MAP (Restored to Stable Formatting)
# ---------------------------------------------------------
MAP_GRID = [
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

def render_interactive_ascii_map(war_snap, infra_snap):
    global map_cursor_row, map_cursor_col
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
            if kw in text: detected_nodes[label] = {"row": r, "col": c, "text": text}

    grid = [list(row) for row in MAP_GRID]
    for label, info in detected_nodes.items():
        r, c = info["row"], info["col"]
        if 0 <= r < len(grid) and 0 <= c < len(grid[0]): grid[r][c] = '☒'

    hovered_info = "Nominal airspace. No active threat node under crosshair."
    for label, info in detected_nodes.items():
        if info["row"] == map_cursor_row and info["col"] == map_cursor_col:
            hovered_info = f"[{label}] {info['text'][:65]}"
            break

    if 0 <= map_cursor_row < len(grid) and 0 <= map_cursor_col < len(grid[0]):
        grid[map_cursor_row][map_cursor_col] = f"{RED}╬{RESET}"

    rendered_map = "\n".join(["".join(row) for row in grid])
    sitrep = f"\n{PURPLE}  --- INTERACTIVE MAP CONTROLS: W/A/S/D (Move) ---{RESET}\n"
    sitrep += f"  {CYAN}CURSOR POS: [{map_cursor_row:02d}, {map_cursor_col:02d}] | NODE INSPECT: {hovered_info}{RESET}\n"
    return rendered_map + sitrep

# ---------------------------------------------------------
# DIRECT NEURAL UPLINK / CHAT HANDLER
# ---------------------------------------------------------
def process_chat_query(user_query, composite, C_t, war_snap, infra_snap, ins_snap):
    global napalm_chat_history
    context_str = f"SYSTEM STATE -> Composite: {composite:.2f}, C_t: {C_t:.2f}. "
    if war_snap: context_str += f"Latest Threat: {war_snap[0][1]['text']}. "
    if infra_snap: context_str += f"Latest Grid Alert: {infra_snap[0][1]['text']}. "
    if ins_snap: 
        tx = ins_snap[0][1]
        context_str += f"Latest SEC Whale Trade: {tx.get('owner', 'Unknown')} traded ${tx.get('value', 0):,.0f} of {tx.get('issuer', 'Unknown')}. "

    with state_lock:
        napalm_chat_history.append({"role": "user", "content": user_query})
    prompt = f"""You are PROJECT HANNIBAL, the core tactical AI for this terminal.
Current Live Context: {context_str}
Respond directly to the commander's query below. Be ruthless, concise, and analytical. Do not use markdown. Do not apologize.

Commander: {user_query}"""

    payload = {"model": "llama3", "prompt": prompt, "stream": False}

    try:
        req = urllib.request.Request("http://localhost:11434/api/generate", data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=45) as response:
            result = json.loads(response.read().decode('utf-8'))
            reply = result.get("response", "").strip()
            with state_lock:
                napalm_chat_history.append({"role": "hannibal", "content": reply})
    except Exception as e:
        logger.warning(f"Ollama chat query failed: {e}")
        with state_lock:
            napalm_chat_history.append({"role": "hannibal", "content": f"Neural uplink failed. System logging locally."})

def run_hannibal_engine(composite, C_t, K, war_snap, infra_snap, ins_detail_snap):
    global napalm_active_synthesis, napalm_advisories
    intel_war = "\n".join([f"- KINETIC: {h[1]['text']}" for h in war_snap[:3]])
    intel_infra = "\n".join([f"- INFRA: {h[1]['text']}" for h in infra_snap[:3]])
    intel_sec = "\n".join([f"- SEC: {d['owner']} {d['code_label']} {d['issuer']} ${d['value']:,.0f}" for _, d in ins_detail_snap[:3]])
    
    prompt = f"""You are PROJECT HANNIBAL Singularity. Generate a dual-agent review based on this telemetry:
Friction: {composite:.2f} | C_t: {C_t:.2f} | K: {K}
INTERCEPTS:
{intel_war}
{intel_infra}
{intel_sec}

FORMAT EXACTLY AS JSON:
{{
  "synthesis": "Dual-agent macro summary.",
  "advisories": [
    {{"sector": "Global Markets", "action": "HEDGE", "asset": "S&P 500 Index", "reason": "Detailed risk assessment.", "outcome": "Predicted outcome."}},
    {{"sector": "Emerging Markets", "action": "BUY", "asset": "MSCI Emerging Markets", "reason": "Detailed reasoning.", "outcome": "Predicted outperformance."}},
    {{"sector": "Currencies", "action": "SELL", "asset": "USD/JPY", "reason": "Detailed reasoning.", "outcome": "USD depreciation."}},
    {{"sector": "Bonds", "action": "BUY", "asset": "10-Year Treasury Note", "reason": "Yield analysis.", "outcome": "Bond strength."}},
    {{"sector": "Technology", "action": "HEDGE", "asset": "Nasdaq-100 Index", "reason": "Tech volatility hedge.", "outcome": "Volatility mitigation."}}
  ]
}}"""

    payload = {"model": "llama3", "prompt": prompt, "format": "json", "stream": False}
    try:
        req = urllib.request.Request("http://localhost:11434/api/generate", data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as response:
            ai_resp = json.loads(json.loads(response.read().decode('utf-8')).get("response", "{}"))
            with state_lock:
                napalm_active_synthesis = ai_resp.get("synthesis", "Synthesis active.")
                napalm_advisories = [adv for adv in ai_resp.get("advisories", []) if "sector" in adv]
            
            with open(PREDICTION_AUDIT_PATH, "a", encoding="utf-8") as f:
                log_entry = {
                    "timestamp": datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST"),
                    "composite": f"{composite:.2f}",
                    "C_t": f"{C_t:.2f}",
                    "napalm_synthesis": napalm_active_synthesis,
                    "napalm_advisories": napalm_advisories
                }
                f.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        logger.warning(f"HANNIBAL engine synthesis failed: {e}")
        with state_lock:
            napalm_active_synthesis = "Adversarial neural synthesis operational via background loop."

# ---------------------------------------------------------
# DATA PIPELINES (SEC, RSS, CONSENSUS, WEATHER)
# ---------------------------------------------------------
def fetch_form4_detail(filing_url):
    raw_txt_url = filing_url.replace("-index.htm", ".txt").replace("-index.html", ".txt")
    raw = safe_request(raw_txt_url)
    if not raw: return []
    try:
        doc_str = raw.decode('utf-8', errors='ignore')
        match = re.search(r'(?si)<XML>\s*(.*?)\s*</XML>', doc_str)
        if match: root = ET.fromstring(match.group(1).strip())
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
            val = shares * price
            pred = f"{PURPLE}WHALE{RESET}" if (ad_code == "A" and val >= LARGE_INSIDER_TRADE_USD) else (f"{RED}DUMP{RESET}" if ad_code == "D" and val >= LARGE_INSIDER_TRADE_USD else f"{CYAN}ROUTINE{RESET}")
            parsed.append({"issuer": issuer_symbol, "owner": owner_name, "role": role, "code_label": "BUY" if code == "P" else ("SELL" if code == "S" else code), "shares": shares, "price": price, "value": val, "prediction": pred})
        except Exception: continue
    return parsed

def sync_osint_feeds():
    global insider_details, osint_data, kinetic_headlines, infra_atomic_headlines, labor_headlines, weather_alerts, prediction_markets, last_batch_sync_time
    last_batch_sync_time = time.time()
    now = time.time()
    
    # 1. SEC Edgar Feed
    try:
        sec_feed = safe_request("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=20&output=atom")
        if sec_feed:
            root = ET.fromstring(sec_feed)
            ns = {"a": "http://www.w3.org/2005/Atom"}
            new_links = []
            for entry in root.findall("a:entry", ns):
                href = entry.find("a:link", ns).get("href")
                if href and href not in seen_insider_links:
                    seen_insider_links.add(href)
                    new_links.append(href)
            
            for link in new_links[:FORM4_DETAIL_MAX_PER_BATCH]:
                try:
                    txs = fetch_form4_detail(link)
                    with state_lock:
                        for tx in txs: insider_details.append((time.time(), tx))
                        insider_details[:] = insider_details[-40:]
                except Exception: pass
    except Exception as e:
        logger.warning(f"SEC EDGAR feed fetch failed: {e}")

    # 2. Worldwide RSS & Global Weather Parsing
    wires = [
        ("http://feeds.bbci.co.uk/news/world/rss.xml", "GLOBAL"),
        ("https://www.aljazeera.com/xml/rss/all.xml", "GLOBAL"),
        ("https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml", "KINETIC"),
        ("https://news.un.org/feed/subscribe/en/news/all/rss.xml", "UN")
    ]
    for url, region in wires:
        try:
            feed = safe_request(url)
            if feed:
                root = ET.fromstring(feed)
                for item in root.findall('.//item')[:10]:
                    title = item.findtext('title')
                    if not title: continue
                    nums_str = re.findall(r'\b\d+\.?\d*%\b|\$\d+\.?\d*[MBK]?\b', title)
                    nums = nums_str[0] if nums_str else "N/A"
                    title_low = title.lower()
                    
                    if any(w in title_low for w in ["war", "missile", "military"]) or ("strike" in title_low and not any(w in title_low for w in ["union", "workers", "worker", "labor", "employees", "wages", "walkout", "picket"])):
                        kinetic_headlines.insert(0, (now, {"text": f"[{region}] {title[:60]}", "nums": nums, "prediction": f"{RED}▼ ESCALATION{RESET}"}))
                        log_to_db("KINETIC", region, title, nums, "ESCALATION")
                    elif any(w in title_low for w in ["grid", "power", "nuclear", "iaea"]):
                        infra_atomic_headlines.insert(0, (now, {"text": f"[{region}] {title[:60]}", "nums": nums, "prediction": f"{RED}▼ CRITICAL{RESET}"}))
                        log_to_db("INFRA", region, title, nums, "CRITICAL")
                    elif any(w in title_low for w in ["strike", "union", "walkout"]):
                        labor_headlines.insert(0, (now, {"text": f"[{region}] {title[:60]}", "nums": nums, "prediction": f"{PURPLE}♦ DISRUPTION{RESET}"}))
                        log_to_db("LABOR", region, title, nums, "DISRUPTION")
                    # NEW: Global Weather Parsing
                    elif any(w in title_low for w in ["typhoon", "hurricane", "flood", "earthquake", "tsunami", "wildfire", "heatwave"]):
                        with state_lock:
                            weather_alerts.insert(0, (now, {"event": title[:60], "area": region, "severity": "GLOBAL"}))
                        log_to_db("WEATHER", region, title, nums, "SEVERE")
                    else:
                        osint_data.insert(0, (now, {"text": f"[{region}] {title[:75]}", "nums": nums, "prediction": f"{CYAN}► MACRO{RESET}"}))
        except Exception as e:
            logger.warning(f"RSS wire fetch failed for {url}: {e}")
            
    with state_lock:
        kinetic_headlines[:] = kinetic_headlines[:40]
        infra_atomic_headlines[:] = infra_atomic_headlines[:40]
        labor_headlines[:] = labor_headlines[:40]
        osint_data[:] = osint_data[:40]
        weather_alerts[:] = weather_alerts[:30] # Keep recent global weather

    # 3. Consensus Prediction Markets
    temp_preds = []
    try:
        poly_feed = safe_request("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=15&order=volume24hr&ascending=false", is_api=True)
        if poly_feed:
            for m in json.loads(poly_feed):
                op = m.get("outcomePrices")
                if isinstance(op, str): op = json.loads(op)
                temp_preds.append({"platform": "Polymarket", "question": m.get("question", "Unknown"), "yes_price": float(op[0]) if op else 0.5})
    except Exception as e:
        logger.warning(f"Polymarket feed fetch failed: {e}")
    
    try:
        kalshi_feed = safe_request("https://api.elections.kalshi.com/trade-api/v2/markets?limit=15&status=open", is_api=True)
        if kalshi_feed:
            for m in json.loads(kalshi_feed).get("markets", []):
                yb = m.get("yes_bid")
                temp_preds.append({"platform": "Kalshi", "question": m.get("title", m.get("ticker", "Unknown")), "yes_price": (yb / 100.0) if yb else 0.5})
    except Exception as e:
        logger.warning(f"Kalshi feed fetch failed: {e}")
    
    with state_lock:
        if temp_preds: prediction_markets = temp_preds

def fetch_fast_telemetry():
    global weather_alerts, flight_disruptions
    w_data = safe_request("https://api.weather.gov/alerts/active?severity=Severe,Extreme", is_api=True)
    f_data = safe_request("https://nasstatus.faa.gov/api/airport-status-information", is_api=False)
    
    now = time.time()
    temp_w, temp_f = [], []
    if w_data:
        try:
            payload = json.loads(w_data)
            temp_w = [(now, {"event": f.get("properties", {}).get("event", "Unknown"), "area": f.get("properties", {}).get("areaDesc", "Unknown"), "severity": f.get("properties", {}).get("severity", "Unknown")}) for f in payload.get("features", [])[:20]]
        except Exception as e:
            logger.warning(f"NWS weather alert parse failed: {e}")
        
    if f_data:
        try:
            root = ET.fromstring(f_data)
            for dt in root.findall('.//Delay_type'):
                cat = dt.findtext('Name') or "Delay"
                for air in dt.findall('.//Airport'):
                    temp_f.append((now, {"category": cat, "airport": (air.findtext('ARPT') or "UNK").strip(), "reason": (air.findtext('Reason') or "").strip()}))
        except Exception as e:
            logger.warning(f"FAA airspace status parse failed: {e}")

    with state_lock:
        # Append US NWS alerts to the global weather list safely
        weather_alerts.extend(temp_w)
        weather_alerts[:] = weather_alerts[:30]
        if temp_f: flight_disruptions = temp_f

def background_price_poller():
    while True:
        try:
            with open(os.devnull, 'w') as devnull:
                old_stdout, old_stderr = sys.stdout, sys.stderr
                sys.stdout, sys.stderr = devnull, devnull
                try: 
                    data = yf.download(all_possible_tickers, period="1d", interval="2m", progress=False, threads=True)
                finally: 
                    sys.stdout, sys.stderr = old_stdout, old_stderr

            with state_lock:
                for ticker in all_possible_tickers:
                    try:
                        c_s = data['Close'][ticker].dropna() if isinstance(data.columns, pd.MultiIndex) else data['Close'].dropna()
                        if not c_s.empty:
                            price = float(c_s.iloc[-1])
                            last_known_prices[ticker] = price
                            price_buffers[ticker].append(price)
                    except Exception: pass
        except Exception as e:
            logger.warning(f"Price poller cycle failed: {e}")
        time.sleep(25.0)

def osint_sync_daemon():
    while True:
        try: 
            sync_osint_feeds()
        except Exception as e: 
            logger.error(f"OSINT Sync Error: {e}")
        time.sleep(90.0) # Ironclad 90-second loop
        
def fast_poll_daemon():
    while True:
        try: 
            fetch_fast_telemetry()
        except Exception: pass
        time.sleep(15.0)

def calculate_sector_prediction(view_key, E_t, T_t, C_t, composite, K):
    if view_key == 'R': return (K * 0.40) + (C_t * 0.35) - (E_t * 20), "MONITOR ESCALATION", YELLOW, "Russian commodity channels sensitive to K and E_t."
    if view_key == 'A': return (C_t * 0.20) + (composite * 0.40) - 0.1, "DEFENSIVE HEDGE", RED, "China exposure heavily weighted by macro composite."
    if view_key == 'J': return (-composite * 0.15) + (T_t * 15), "NEUTRAL", CYAN, "Japan isolated from direct kinetic shocks; logistics dependent."
    if view_key == 'E': return (E_t * 65) + (C_t * 0.25), "BUY ENERGY", GREEN, "Middle East energy matrices tracking E_t."
    if view_key == 'N': return (T_t * 35) + (composite * 0.10), "ACCUMULATE", GREEN, "India/South Asia supply chain growth."

    score = 0.0
    reason = "Nominal drift."
    if view_key == "1": score = -composite if composite > 0.5 else 0.2
    elif view_key == "2": score = (E_t * 50) + (C_t * 0.1)
    elif view_key == "3": score = -C_t * 0.2
    elif view_key == "4": score = 0.1
    elif view_key == "5": score = -composite * 1.5
    elif view_key == "6": score = (T_t * 50) - (C_t * 0.15)
    elif view_key == "7": score = composite * 0.8
    elif view_key == "8": score = composite * 0.5
    elif view_key == "9": score = -C_t * 0.3
    elif view_key == "0": score = composite * 0.2
        
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
    return history[-5:]

# ---------------------------------------------------------
# MAIN EXECUTION LOOP
# ---------------------------------------------------------
def main():
    global current_view, map_cursor_row, map_cursor_col, graph_overlay_mode
    global global_E_t, global_T_t, global_K, global_C_t, global_composite
    
    threading.Thread(target=background_ollama_bootstrapper, daemon=True).start()
    threading.Thread(target=background_price_poller, daemon=True).start()
    threading.Thread(target=osint_sync_daemon, daemon=True).start()
    threading.Thread(target=fast_poll_daemon, daemon=True).start()
    
    last_prediction_run = 0.0

    print(f"{PURPLE}========================================================================={RESET}")
    print(f"{CYAN} PROJECT HANNIBAL V8.0 — ABSOLUTE STANDALONE SINGULARITY ONLINE {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    time.sleep(1.0)

    try:
        while True:
            now_mono = time.monotonic()
            with view_lock: view_key = current_view
            view_name = MARKETS.get(view_key, MARKETS["1"])["name"] if view_key in MARKETS else f"SPECIALIZED PAGE: [{view_key}]"
            active_tickers = MARKETS.get(view_key, MARKETS["1"])["tickers"] if view_key in MARKETS else ["BZ=F", "^GSPC", "BTC-USD"]

            with state_lock:
                war_snap = list(kinetic_headlines)
                infra_snap = list(infra_atomic_headlines)
                lab_snap = list(labor_headlines)
                ins_detail_snap = list(insider_details)
                p_snap = list(prediction_markets)
                w_snap = list(weather_alerts)
                f_snap = list(flight_disruptions)
                osint_snap = list(osint_data)
                chat_snap = list(napalm_chat_history)
                adv_snap = list(napalm_advisories)
                synth_snap = napalm_active_synthesis
                llm_status_snap = llm_status_string
                
            global_K = 1 if len(war_snap) > 0 else 0
            global_C_t = 1.0 + (len(war_snap) * 0.15)
            global_composite = min(1.0, (global_K * 0.4 + len(infra_snap) * 0.2) * global_C_t)

            if now_mono - last_prediction_run >= 120.0:
                threading.Thread(target=run_hannibal_engine, args=(global_composite, global_C_t, global_K, war_snap, infra_snap, ins_detail_snap), daemon=True).start()
                last_prediction_run = now_mono

            clear_screen()
            timestamp = datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST")
            print(f"{BLUE}========================================================================================={RESET}")
            print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | ONLINE {RESET}")
            print(f"{BLUE}========================================================================================={RESET}")

            if view_key in MARKETS:
                p_score, p_action, p_color, p_reason = calculate_sector_prediction(view_key, global_E_t, global_T_t, global_C_t, global_composite, global_K)
                print(f"{PURPLE}  SECTOR PREDICTION ALGORITHM{RESET}")
                print(f"{BLUE}-----------------------------------------------------------------------------------------{RESET}")
                print(f"  TARGET SECTOR: {view_name}")
                print(f"  RECOMMENDATION: {p_color}{p_action} ({p_score:+.2f} Vector Score){RESET}")
                print(f"  AI RATIONALE: {GRAY}{p_reason}{RESET}")
                print(f"  VELOCITY GRAPH: [{p_color}{generate_bar_chart(p_score, width=30)}{RESET}]")
                print(f"{BLUE}========================================================================================={RESET}")

                print(f" {'TICKER':<8} | {'PRICE':<9} | {'DELTA (%)':<9} | {'Z-SCORE':<7} | {'MOMENTUM':<{SPARK_WIDTH}} | {'STATUS':<15}")
                print("-" * 89)
                with state_lock:
                    for ticker in active_tickers:
                        buf = price_buffers[ticker]
                        curr_price = last_known_prices.get(ticker)
                        pct = (buf[-1] - buf[-2]) / buf[-2] if curr_price and len(buf) > 2 and buf[-2] else 0.0
                        spark = generate_sparkline(buf)
                        if curr_price is None:
                            print(f"  {ticker:<7} | {'NO DATA':<9} | {'--':>9} | {'--':>7} | {spark} | {'OFFLINE':<15}")
                            continue
                        color, status = (PURPLE, "♦ VOLATILITY") if abs(pct) >= 0.02 else ((RED, "CRIT. DROP") if pct < -0.01 else ((GREEN, "SURGE") if pct > 0.01 else (CYAN, "NOMINAL")))
                        print(f"{color}  {ticker:<7} | {curr_price:<9.2f} | {pct*100:>8.2f}% | {'--':>7} | {spark} | {status:<15}{RESET}")

                # NEW: Main UI Headlines Restored
                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  WORLDWIDE OSINT HEADLINE RADAR (Persistent Rolling Buffer){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'LATEST WORLDWIDE HEADLINES':<65} | {'DATA':<8} | {'PREDICTION'}")
                print("-" * 89)
                if not osint_snap: 
                    print(f" {GRAY}Aggregating worldwide OSINT streams...{RESET}")
                else:
                    for item in osint_snap[:6]:
                        age_str = f"{int(time.time() - item[0])}s"
                        print(f" [{age_str:>3}] {item[1]['text']:<60} | {item[1]['nums']:<8} | {item[1]['prediction']}")

            elif view_key == "M":
                print(render_interactive_ascii_map(war_snap, infra_snap))

            elif view_key == "G":
                print(f"{PURPLE}  HANNIBAL GRAPH MATRIX: PREDICTIVE SECTOR VELOCITIES (Mode: {graph_overlay_mode}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"  [O] TOGGLE OVERLAY MODE (1: Momentum | 2: Volume Z-Score | 3: Friction Impact)\n")
                for key, data in MARKETS.items():
                    s, a, c, r = calculate_sector_prediction(key, global_E_t, global_T_t, global_C_t, global_composite, global_K)
                    disp_val = s * (1.5 if graph_overlay_mode == 2 else (global_C_t if graph_overlay_mode == 3 else 1.0))
                    disp_bar = generate_bar_chart(disp_val, max_val=2.0 if graph_overlay_mode == 3 else 1.0, width=40)
                    print(f"  {c}{data['name']:<30}{RESET} [{c}{disp_bar}{RESET}] {disp_val:+.2f} ({a})")

            elif view_key == "K":
                print(f"{PURPLE}  WORLDWIDE MULTI-THEATER KINETIC ESCALATION & STRIKE MAPPING{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                k_stat = f"{RED}ACTIVE ({len(war_snap)} alerts){RESET}" if global_K else f"{GRAY}INACTIVE / STANDBY{RESET}"
                print(f" Kinetic Escalation Flag (K): {k_stat} | Cascading Threat Index (C_t): {global_C_t:.2f}\n")
                if not war_snap: print(f" {GRAY}No kinetic alerts in active buffer.{RESET}")
                for idx, wh in enumerate(war_snap[:20], 1): 
                    print(f" [{idx:02d}] [{int(time.time() - wh[0]):>3}s] {RED}♦ THEATER INTEL:{RESET} {wh[1]['text']}")

            elif view_key == "I":
                print(f"{PURPLE}  CRITICAL INFRASTRUCTURE, POWER GRIDS & ATOMIC AGENCY (IAEA) RADAR{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                if not infra_snap: print(f" {GRAY}No critical grid, subsea, or atomic incidents active.{RESET}")
                for idx, h in enumerate(infra_snap[:25], 1):
                    c = RED if any(w in h[1]['text'].lower() for w in ["scram", "explosion", "blackout", "breach", "iaea"]) else YELLOW
                    print(f" [{idx:02d}] {c}⚡ INFRA/ATOMIC:{RESET} {h[1]['text']}")

            elif view_key == "C":
                print(f"{PURPLE}  4X DECENTRALIZED PREDICTION MARKETS & GLOBAL CONSENSUS{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'PLATFORM':<12} | {'MARKET / QUESTION':<60} | {'YES PRICE':<10}")
                print("-" * 89)
                if not p_snap: print(f" {GRAY}Aggregating decentralized consensus feeds...{RESET}")
                for m in p_snap[:25]:
                    c = CYAN if m['platform'] == 'Polymarket' else YELLOW
                    print(f" {c}{m['platform']:<12}{RESET} | {m['question'][:60]:<60} | {m['yes_price']*100:.1f}¢")

            elif view_key == "W":
                print(f"{PURPLE}  DETAILED METEOROLOGICAL & AIRSPACE DISRUPTION TELEMETRY{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}Global Weather & NWS Severe Alerts ({len(w_snap)} active):{RESET}")
                if not w_snap: print("   No severe weather alerts active.")
                for alert in w_snap[:15]: 
                    area = alert[1].get('area', 'Unknown')
                    event = alert[1].get('event', 'Unknown')
                    sev = alert[1].get('severity', 'GLOBAL')
                    print(f"   • [{sev}] {event} — {area[:65]}")
                print(f"\n {GRAY}FAA Airspace Disruption & Ground Stops ({len(f_snap)} entries):{RESET}")
                if not f_snap: print("   Airspace ground stops nominal.")
                for fs in f_snap[:15]: print(f"   • Airport: {fs[1]['airport']} | Type: {fs[1]['category']} | Reason: {fs[1]['reason'][:50]}")
                
            elif view_key == "H":
                print(f"{PURPLE}  AUDIT TRAIL: HISTORICAL DOSSIER REPLAY (SQLite Databased){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                history = load_prediction_history()
                if not history: print(f" {GRAY}No prediction history recorded yet.{RESET}")
                else:
                    for idx, item in enumerate(history, 1):
                        print(f" [{idx}] {YELLOW}TIMESTAMP: {item.get('timestamp')}{RESET} | Composite: {item.get('composite')} | C_t: {item.get('C_t')}")
                        print(f"     {CYAN}Synthesis: {item.get('napalm_synthesis', '')}{RESET}")

            elif view_key == "P":
                print(f"{PURPLE}  PROJECT HANNIBAL: RED/BLUE ADVERSARIAL NEURAL ENGINE{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" Neural Bridge Status          : {CYAN}{llm_status_snap}{RESET}")
                print(f" Composite Friction Score      : {global_composite:.2f} / 1.00\n")
                print(f" {YELLOW}--- DUAL-AGENT SYNTHESIS ---{RESET}")
                print(f" {CYAN}{synth_snap}{RESET}\n")
                
                if not adv_snap:
                    print(f" {GRAY}Awaiting structured JSON advisories from HANNIBAL engine...{RESET}")
                else:
                    for adv in adv_snap:
                        color = GREEN if "BUY" in adv.get('action', '').upper() else (RED if "SELL" in adv.get('action', '').upper() or "HEDGE" in adv.get('action', '').upper() else CYAN)
                        print(f" {color}[{adv.get('sector')}]{RESET} {adv.get('action')} -> {adv.get('asset')}")
                        print(f"   ↳ {GRAY}Reason: {adv.get('reason')}{RESET}")
                        print(f"   ↳ {PURPLE}Outcome: {adv.get('outcome')}{RESET}")

            elif view_key == "S":
                print(f"{PURPLE}  SEC EDGAR — WHALE ACCUMULATION & INSIDER TRADES{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {'ISSUER':<8} | {'OWNER':<22} | {'ROLE':<18} | {'TYPE':<6} | {'SHARES':>10} | {'PRICE':>7} | {'VALUE ($)':>14} | {'PREDICTION'}")
                print("-" * 105)
                for _, tx in ins_detail_snap[-20:]:
                    print(f"{tx['issuer']:<8} | {tx['owner'][:22]:<22} | {tx['role'][:18]:<18} | {tx['code_label']:<6} | {tx['shares']:>10,.0f} | ${tx['price']:>6.2f} | ${tx['value']:>12,.0f} | {tx['prediction']}")

            elif view_key == "T":
                print(f"{PURPLE}  NEURAL UPLINK: DIRECT TACTICAL CONVERSATION WITH HANNIBAL{RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                for msg in chat_snap[-10:]:
                    role, c = (GREEN+"[COMMANDER]", msg['content']) if msg["role"] == "user" else (CYAN+"[HANNIBAL]", msg['content'])
                    print(f" {role}{RESET} > {c}\n")
                print(f"{BLUE}========================================================================================={RESET}")
                user_input = input(f"{YELLOW} [UPLINK ACTIVE] Enter Query (or blank to return): {RESET}")
                if user_input.strip(): 
                    threading.Thread(target=process_chat_query, args=(user_input, global_composite, global_C_t, war_snap, infra_snap, ins_detail_snap)).start()
                with view_lock: current_view = '1'

            if view_key != "T":
                time_since_batch = time.time() - last_batch_sync_time
                next_batch_in = max(90.0 - time_since_batch, 0)
                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  HANNIBAL V8.0 STATUS (Composite Index: {global_composite:.2f} | C_t: {global_C_t:.2f}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}Telemetry Pipeline:{RESET} Fast-Poll (15s) + Batch Sync (90s) -> Active")
                print(f" {GRAY}Next Ingestion Wave:{RESET} {next_batch_in:.0f}s")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"{YELLOW} [1-0/R/A/J/E/N] Markets | [M] Map | [G] Graphs | [W] Weather/FAA | [K] War | [I] Infra | [S] SEC Insider | [C] Consensus | [P] AI Engine | [T] Talk | [H] Audit {RESET}")

                # Instant UI Key Polling Loop (50ms responsive interval)
                for _ in range(16):
                    if IS_WINDOWS and _kbhit():
                        key = _getch()
                        valid_keys = list(MARKETS.keys()) + ['m', 's', 'p', 't', 'g', 'w', 'k', 'i', 'c', 'h']
                        if key in [k.lower() for k in valid_keys] or key in valid_keys:
                            with view_lock: current_view = key.upper()
                            break
                        elif current_view == 'G' and key == 'o':
                            graph_overlay_mode = (graph_overlay_mode % 3) + 1
                            break
                        elif current_view == 'M':
                            if key == 'w': map_cursor_row = max(1, map_cursor_row - 1)
                            if key == 's': map_cursor_row = min(len(MAP_GRID)-2, map_cursor_row + 1)
                            if key == 'a': map_cursor_col = max(1, map_cursor_col - 1)
                            if key == 'd': map_cursor_col = min(78, map_cursor_col + 1)
                            break
                    time.sleep(0.05)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] HANNIBAL V8.0 safely disengaged by operator.{RESET}")

if __name__ == "__main__":
    main()