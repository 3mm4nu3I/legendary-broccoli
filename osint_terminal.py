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

IMPORTANT — coefficient status:
    beta0..beta3 below are PROVISIONAL DEFAULTS, not empirically fitted.
    Fitting them for real requires a historical panel of verified
    kinetic events matched against E_t/T_t/inflation outcomes, which
    this script does not have access to. What this script DOES do is
    log every triggered event to kinetic_event_log.csv (timestamp,
    headline, E_t, T_t, K, delta_I_hat, signals fired) — that log is
    exactly the dataset you'd need to eventually estimate beta1..beta3
    properly (e.g. with the same HAC/OLS approach used in
    inflation_model.py). Until then, treat delta_I_hat as a
    model-consistent estimate under assumed coefficients, not a
    validated forecast.

Safe-haven and equities signals are a separate, standard flight-to-
quality heuristic (not something beta0..beta3 estimate directly) —
they only fire while K=1 and are labeled as a heuristic overlay in
the UI so the two sources of signal (regression vs. heuristic) are
never presented as the same kind of evidence.

Additional free feeds (weather, flights, labor/enforcement) and the
composite friction index are ALSO heuristic/experimental, and are
labeled that way everywhere they're displayed — same honesty rule as
above. The labor/enforcement radar is intentionally scoped to public
headline text only: no geolocation, no address-level data, no
real-time location alerting. It classifies already-public Google
News headlines the same way the existing "strike"/"outage" keyword
buckets do, as a labor-supply-disruption indicator for labor-
intensive sectors — it does not do anything beyond that.
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

warnings.filterwarnings('ignore')

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
# PLATFORM HANDLING (hotkeys are Windows-only via msvcrt)
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
#   delta_I_hat = beta0 + beta1*E_t + beta2*(E_t*K) + beta3*T_t
# ---------------------------------------------------------
MODEL_COEFFICIENTS = {
    "beta0": 0.0000,   # baseline drift, provisional
    "beta1": 0.0500,   # linear energy pass-through, provisional
    "beta2": 0.1200,   # kinetic amplification of energy shock, provisional
    "beta3": -0.0400,  # transport friction drag, provisional
}
ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]

KINETIC_DECAY_SECONDS = 4 * 3600  # K stays "active" 4h after a verified strike
SIGNAL_STRONG = 0.010   # |delta_I_hat| beyond this => STRONG BUY/SELL
SIGNAL_MODERATE = 0.003  # beyond this but under STRONG => MODERATE

# ---------------------------------------------------------
# ADDITIONAL FREE OSINT FEEDS
# ---------------------------------------------------------
# Weather: NWS/NOAA public alerts API. Free, no key, official US
# government data. Docs ask requests be no more than every ~30s;
# we poll far under that. A descriptive User-Agent is NWS's stated
# best practice for identifying API consumers — replace the email
# below with a real contact if you're running this long-term.
WEATHER_ALERTS_URL = "https://api.weather.gov/alerts/active?severity=Severe,Extreme"
WEATHER_USER_AGENT = "OSINT-Terminal-Personal-Project (replace-with-your-email@example.com)"
WEATHER_POLL_SECONDS = 300

# Flights: FAA NAS Status public XML feed. Free, no key. Schema
# confirmed against FAA's own servlet docs and live sample output:
# <AIRPORT_STATUS_INFORMATION><Delay_type><Name>...</Name>...
# <Airport><ARPT>.../ARPT><Reason>...</Reason></Airport>...
# If this stays empty even during a known ground stop, check
# osint_terminal.log — FAA has changed this schema before.
FAA_STATUS_URL = "https://nasstatus.faa.gov/api/airport-status-information"
FLIGHT_POLL_SECONDS = 180

# Traffic: there is no free, keyless, nationwide live traffic API
# (Google/HERE/TomTom all require paid keys). Rather than fake a
# feed, traffic/logistics friction is folded into headline keyword
# detection below (port congestion, highway closures, border delays)
# — real signal, just lower-resolution than a dedicated feed. Drop
# a Google Maps/HERE key into TRAFFIC_API_KEY and extend
# fetch_traffic_placeholder() below if you get access to one later.
TRAFFIC_API_KEY = None  # optional — not required for the script to run

# Labor / regulatory enforcement: tracks PUBLIC headlines only (same
# Google News RSS mechanism as the kinetic radar) mentioning
# workplace immigration/labor enforcement actions, as a labor-supply
# disruption indicator for labor-intensive sectors (agriculture,
# meatpacking, food processing, construction, hospitality) — this
# mirrors real academic research on how worksite enforcement actions
# affect local labor supply and firm output. This is headline-level
# text classification, identical in kind to how "strike" or "outage"
# headlines are already classified elsewhere in this script. It does
# NOT geolocate events, does NOT track addresses, and does NOT
# attempt real-time location alerting — scope is deliberately capped
# at "a labor-disruption headline was published," nothing more.
LABOR_ENFORCEMENT_QUERY = (
    '"ICE raid" OR "immigration raid" OR "workplace raid" OR '
    '"worksite enforcement" OR "detained by ICE" OR "warrant executed at" OR '
    '"facility raided"'
)
LABOR_DECAY_SECONDS = 6 * 3600
LABOR_POLL_SECONDS = 60

# Composite index: an explicitly EXPERIMENTAL, unweighted heuristic
# blend of everything above. Unlike delta_I_hat (which is your fitted
# equation), these weights are illustrative round numbers, not
# estimated from data — labeled as such everywhere it's displayed.
COMPOSITE_WEIGHTS = {"kinetic": 0.40, "labor": 0.20, "flights": 0.20, "weather": 0.20}

# ---------------------------------------------------------
# MARKET VIEWS (HOTKEYS 1-9)
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

POLL_INTERVAL = 3.0          # seconds between display redraws
FULL_REFRESH_EVERY_N = 4     # only re-download prices every Nth redraw (~12s @ 3.0s)
WINDOW_SIZE = 600            # capped rolling buffer per ticker
SPARK_WIDTH = 12

# ---------------------------------------------------------
# SHARED STATE
# ---------------------------------------------------------
osint_data = []
seen_headlines = set()
osint_lock = threading.Lock()

kinetic_headline_queue = deque()      # newly detected, not-yet-displayed strikes
kinetic_active_until = None           # datetime or None
kinetic_last_headline = None
kinetic_lock = threading.Lock()
seen_kinetic = set()

labor_headline_queue = deque()        # newly detected, not-yet-displayed labor/enforcement events
labor_active_until = None             # datetime or None
labor_last_headline = None
labor_lock = threading.Lock()
seen_labor = set()

weather_alerts = []      # list of {"event", "area", "severity"}
weather_lock = threading.Lock()

flight_disruptions = []  # list of {"category", "airport", "reason"}
flight_lock = threading.Lock()

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}


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
    return "".join(
        chars[int(((x - min_val) / span) * (len(chars) - 1))] for x in subset
    ).rjust(width)


# ---------------------------------------------------------
# RSS FETCH WITH BACKOFF
# ---------------------------------------------------------
def fetch_rss(url, timeout=5):
    """Fetch and parse an RSS feed. Returns the XML root, or None on
    failure. Failures are logged (not silently swallowed)."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return ET.fromstring(response.read())
    except Exception as exc:
        logger.warning("RSS fetch failed for %s: %s", url, exc)
        return None


# ---------------------------------------------------------
# THREAD 1: BACKGROUND OSINT HEADLINE RADAR
# ---------------------------------------------------------
def fetch_osint_headlines():
    global osint_data, seen_headlines, current_view
    base_interval = 33
    backoff = base_interval
    max_backoff = 5 * 60

    while True:
        with view_lock:
            query = MARKETS[current_view]["query"]
        encoded_query = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

        root = fetch_rss(url)
        if root is not None:
            backoff = base_interval  # reset on success
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
            backoff = min(backoff * 1.5, max_backoff)
            logger.info("OSINT headline backoff now %.0fs", backoff)

        time.sleep(backoff)


# ---------------------------------------------------------
# THREAD 2: KINETIC STRIKE RADAR
# ---------------------------------------------------------
def fetch_kinetic_strikes():
    global kinetic_active_until, kinetic_last_headline, seen_kinetic
    query = '"confirmed strike" OR "missile attack" OR "military escalation" OR "airstrike" OR "bombed"'
    encoded_query = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    base_interval = 15
    backoff = base_interval
    max_backoff = 3 * 60

    while True:
        root = fetch_rss(url)
        if root is not None:
            backoff = base_interval
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
        else:
            backoff = min(backoff * 1.5, max_backoff)
            logger.info("Kinetic radar backoff now %.0fs", backoff)

        time.sleep(backoff)


# ---------------------------------------------------------
# THREAD 3: LABOR / WORKSITE ENFORCEMENT RADAR
#   Headline-level only — see LABOR_ENFORCEMENT_QUERY comment above
#   for exactly what this does and does not track.
# ---------------------------------------------------------
def fetch_labor_enforcement():
    global labor_active_until, labor_last_headline, seen_labor
    encoded_query = urllib.parse.quote(LABOR_ENFORCEMENT_QUERY)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    backoff = LABOR_POLL_SECONDS
    max_backoff = 3 * 60

    while True:
        root = fetch_rss(url)
        if root is not None:
            backoff = LABOR_POLL_SECONDS
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
                logger.info("Labor/enforcement headline: %s", title)
        else:
            backoff = min(backoff * 1.5, max_backoff)
            logger.info("Labor radar backoff now %.0fs", backoff)

        time.sleep(backoff)


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
# THREAD 4: WEATHER ALERTS (NWS/NOAA, free, no key)
# ---------------------------------------------------------
def _parse_nws_payload(payload):
    """Pure parser, testable without a network call. Expects the
    GeoJSON FeatureCollection shape returned by api.weather.gov."""
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
    backoff = WEATHER_POLL_SECONDS
    max_backoff = 20 * 60

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
            backoff = WEATHER_POLL_SECONDS
            logger.info("Weather alerts refreshed: %d active", len(parsed))
        except Exception as exc:
            logger.warning("NWS weather fetch failed: %s", exc)
            backoff = min(backoff * 1.5, max_backoff)
        time.sleep(backoff)


# ---------------------------------------------------------
# THREAD 5: FLIGHT / AIRSPACE DISRUPTIONS (FAA NAS, free, no key)
# ---------------------------------------------------------
def _parse_faa_xml(root):
    """Pure parser, testable without a network call. Expects the
    AIRPORT_STATUS_INFORMATION > Delay_type > Name / *_List > Airport
    shape documented by FAA's flyfaa servlet and confirmed against
    live sample output."""
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
    backoff = FLIGHT_POLL_SECONDS
    max_backoff = 15 * 60

    while True:
        root = fetch_rss(FAA_STATUS_URL, timeout=8)
        if root is not None:
            parsed = _parse_faa_xml(root)
            with flight_lock:
                flight_disruptions = parsed
            backoff = FLIGHT_POLL_SECONDS
            logger.info("Flight disruptions refreshed: %d entries", len(parsed))
        else:
            backoff = min(backoff * 1.5, max_backoff)
            logger.info("Flight radar backoff now %.0fs", backoff)
        time.sleep(backoff)


# ---------------------------------------------------------
# LIVE MODEL: E_t, T_t, K, delta_I_hat
# ---------------------------------------------------------
def _avg_last_return(tickers):
    """Average most-recent-cycle return across a list of tickers,
    using whatever is currently buffered. Skips tickers with too
    little history rather than crashing on them."""
    rets = []
    for t in tickers:
        buf = price_buffers.get(t)
        if buf and len(buf) >= 2 and buf[-2] != 0:
            rets.append((buf[-1] - buf[-2]) / buf[-2])
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
    """EXPERIMENTAL, unweighted heuristic blend (0-1) of every OSINT
    signal this script tracks. Round-number weights, not fitted —
    unlike delta_I_hat this is not derived from your regression.
    Useful as a single at-a-glance 'how much is on fire right now'
    gauge, not as a trading input on its own."""
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
    """Map a signed delta_I_hat contribution to a (label, color)."""
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


def display_war_map_alarm(headline):
    E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
    T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
    K, _ = get_kinetic_flag()
    delta_hat = compute_delta_i_hat(E_t, T_t, K)

    c = MODEL_COEFFICIENTS
    energy_component = c["beta1"] * E_t + c["beta2"] * (E_t * K)
    transport_component = c["beta3"] * T_t

    energy_label, energy_color = classify(energy_component)
    transport_label, transport_color = classify(-transport_component)  # negative transport contribution -> sell pressure framing
    # Safe-haven / equities: heuristic overlay, only meaningful while K=1
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
    print(f"{RED}  [!] EVALUATING LIVE MODEL: delta_I_hat = b0 + b1*E_t + b2*(E_t*K) + b3*T_t{RESET}")
    print(f"{GRAY}      E_t={E_t:+.4f}  T_t={T_t:+.4f}  K={K}  ->  delta_I_hat={delta_hat:+.5f}{RESET}")
    print(f"{BLUE}----------------------------------------------------------------------------------------{RESET}")
    print(f"  {'ASSET CLASS':<25} | {'IMPACT VECTOR':<24} | {'EXECUTION SIGNAL'}")
    print(f"{BLUE}----------------------------------------------------------------------------------------{RESET}")
    print(f"  {'Energy (BZ=F, CL=F)':<25} | {'Regression term':<24} | {energy_color}{energy_label}{RESET}")
    print(f"  {'Transport (BDRY, IYT)':<25} | {'Regression term':<24} | {transport_color}{transport_label}{RESET}")
    print(f"  {'Safe Havens (GC=F, UUP)':<25} | {'Heuristic overlay*':<24} | {haven_color}{haven_label}{RESET}")
    print(f"  {'Equities (^GSPC)':<25} | {'Heuristic overlay*':<24} | {equity_color}{equity_label}{RESET}")
    print(f"{BLUE}========================================================================================={RESET}")
    print(f"{GRAY}  * Safe-haven / equities rows are a standard flight-to-quality heuristic, not directly{RESET}")
    print(f"{GRAY}    estimated by beta0..beta3. Coefficients are provisional — see file header.{RESET}")
    print(f"{GRAY}  Event logged to {EVENT_LOG_PATH.name}. Holding for 10s before resuming polling...{RESET}")

    log_kinetic_event(
        headline, E_t, T_t, K, delta_hat,
        energy_label, transport_label, haven_label, equity_label,
    )
    time.sleep(10)


# ---------------------------------------------------------
# STARTUP
# ---------------------------------------------------------
def start_background_threads():
    threading.Thread(target=fetch_osint_headlines, name="OSINT-Radar", daemon=True).start()
    threading.Thread(target=fetch_kinetic_strikes, name="Kinetic-Radar", daemon=True).start()
    threading.Thread(target=fetch_labor_enforcement, name="Labor-Radar", daemon=True).start()
    threading.Thread(target=fetch_weather_alerts, name="Weather-Radar", daemon=True).start()
    threading.Thread(target=fetch_flight_status, name="Flight-Radar", daemon=True).start()


def fetch_prices(tickers):
    """Download current prices for a ticker list. Returns a dict of
    ticker -> latest price (or None). Never raises — logs and
    degrades to cached values on failure."""
    result = {}
    try:
        with open(os.devnull, 'w') as devnull:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = devnull, devnull
            try:
                data = yf.download(tickers, period="1d", interval="1m", progress=False)
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr
    except Exception as exc:
        logger.warning("yf.download failed for %s: %s", tickers, exc)
        return result

    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                series = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
            else:
                series = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
            if not series.empty:
                result[ticker] = float(series.iloc[-1])
        except Exception as exc:
            logger.debug("Price extraction failed for %s: %s", ticker, exc)
    return result


def main():
    global current_view
    print(f"{PURPLE}========================================================================={RESET}")
    print(f"{CYAN} COMMAND SURVEILLANCE TERMINAL | KINETIC OSINT WAR MAPS ONLINE         {RESET}")
    print(f"{PURPLE}========================================================================={RESET}")
    print("\n[+] Engaging event loop...")
    print("[+] Spinning up OSINT headline radar (adaptive interval, base 33s)...")
    print("[+] Spinning up kinetic strike radar (adaptive interval, base 15s)...")
    print("[+] Spinning up labor/worksite enforcement radar (headline-level, base 60s)...")
    print("[+] Spinning up NWS weather alerts feed (free, base 300s)...")
    print("[+] Spinning up FAA airspace disruption feed (free, base 180s)...")
    print(f"[+] Model: delta_I_hat = b0 + b1*E_t + b2*(E_t*K) + b3*T_t "
          f"(coefficients provisional — see file header)")
    print(f"[+] Composite friction index: experimental, unweighted heuristic — see file header")
    if not IS_WINDOWS:
        print(f"{YELLOW}[!] Non-Windows OS detected: hotkey view-switching disabled "
              f"(msvcrt is Windows-only). View 1 will run continuously.{RESET}")
    logger.info("Terminal started.")
    time.sleep(1.5)

    start_background_threads()
    cycle = 0

    try:
        while True:
            cycle += 1

            # Drain any newly-verified kinetic headlines (full alarm screen once each)
            headline_to_show = None
            with kinetic_lock:
                if kinetic_headline_queue:
                    headline_to_show = kinetic_headline_queue.popleft()
            if headline_to_show:
                display_war_map_alarm(headline_to_show)

            # Drain labor/enforcement headlines too — logged and flagged,
            # deliberately NOT given the same full-screen alarm treatment
            # (see LABOR_ENFORCEMENT_QUERY comment: this stays at
            # "headline was published" gravity, not a war-map event).
            with labor_lock:
                while labor_headline_queue:
                    log_labor_event(labor_headline_queue.popleft())

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with view_lock:
                view_key = current_view
            active_tickers = MARKETS[view_key]["tickers"]
            view_name = MARKETS[view_key]["name"]

            # Only hit the network every FULL_REFRESH_EVERY_N cycles to
            # stay well under Yahoo's informal rate limits.
            if cycle % FULL_REFRESH_EVERY_N == 1:
                fresh_prices = fetch_prices(active_tickers)
                for ticker, price in fresh_prices.items():
                    last_known_prices[ticker] = price
                    price_buffers[ticker].append(price)

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
            print(f" {GRAY}MODEL STATE:{RESET} E_t={E_t:+.4f}  T_t={T_t:+.4f}  K={k_str}  "
                  f"delta_I_hat={delta_hat:+.5f}")
            print(f" {'TICKER':<8} | {'PRICE':<9} | {'DELTA (%)':<9} | {'Z-SCORE':<7} | {'MOMENTUM':<{SPARK_WIDTH}} | {'STATUS':<15}")
            print("-" * 89)

            for ticker in active_tickers:
                buf = price_buffers[ticker]
                curr_price = last_known_prices.get(ticker)

                if curr_price is not None and len(buf) > 2:
                    pct_change = (buf[-1] - buf[-2]) / buf[-2] if buf[-2] else 0.0
                    hist = list(buf)[:-1]
                    std = np.std(hist)
                    z_score = (curr_price - np.mean(hist)) / (std if std != 0 else 1.0)
                elif curr_price is not None:
                    pct_change, z_score = 0.0, 0.0
                else:
                    pct_change, z_score = None, None

                sparkline = generate_sparkline(buf)

                if curr_price is None:
                    print(f"  {ticker:<7} | {'NO DATA':<9} | {'--':>9} | {'--':>7} | {' ' * SPARK_WIDTH} | {'OFFLINE':<15}")
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

            for _ in range(int(POLL_INTERVAL * 10)):
                if IS_WINDOWS and _kbhit():
                    key = _getch()
                    if key in MARKETS:
                        with view_lock:
                            current_view = key
                        break
                time.sleep(0.1)

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] Tactical terminal and OSINT streams disengaged by operator.{RESET}")
        logger.info("Terminal stopped by operator.")


if __name__ == "__main__":
    main()
