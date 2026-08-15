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
from zoneinfo import ZoneInfo

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
UNUSUAL_ACTIVITY_LOG_PATH = SCRIPT_DIR / "unusual_activity_log.csv"

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
COMPOSITE_WEIGHTS = {
    "kinetic": 0.30, "labor": 0.15, "flights": 0.15,
    "weather": 0.15, "prediction_markets": 0.25,
}

# ---------------------------------------------------------
# PRICE POLLING — TUNED FOR MINIMUM PRACTICAL LAG
# ---------------------------------------------------------
# HONEST CEILING ON "LAGLESS": yfinance's free endpoint is not
# tick-level real-time infrastructure — it serves ~1-minute bars from
# Yahoo's unofficial API, which carries some inherent delay no client
# polling frequency can remove. What IS in our control: poll as often
# as we safely can, and back off automatically the instant we get
# throttled (empty/failed response) rather than a fixed slow-down
# regardless of whether Yahoo is actually complaining. That's the
# practical floor on lag with a free, keyless feed.
PRICE_POLL_BASE_SECONDS = 2.0
PRICE_POLL_MAX_BACKOFF = 30.0

# ---------------------------------------------------------
# UNUSUAL ACTIVITY / LARGE-TRADE DETECTION
# ---------------------------------------------------------
# Flags abnormal volume (a proxy for "huge investment" / large trades
# — the free feeds here don't expose individual order sizes, so
# volume relative to a ticker's own recent history is the closest
# public signal available) and specifically escalates when that
# coincides with the final stretch before the US equity close.
UNUSUAL_VOLUME_Z = 3.0             # volume std-devs above rolling mean
MARKET_CLOSE_HOUR_ET = 16          # 4:00 PM America/New_York
LATE_SESSION_WINDOW_MINUTES = 30   # "before market closing" window
ET_ZONE = ZoneInfo("America/New_York")

# ---------------------------------------------------------
# INSIDER FILING DISCLOSURES (SEC EDGAR, free, no key)
# ---------------------------------------------------------
# Real, free, government feed of newly-filed Form 4s (insider
# buy/sell disclosures required of corporate officers, directors, and
# >10% owners under Section 16). Endpoint and Atom shape confirmed
# against SEC's own developer docs. SEC's stated fair-access policy
# asks for an identifying User-Agent on every request — replace the
# email below with a real contact if running this long-term, same as
# the weather feed.
#
# DESIGN NOTE: this is deliberately a GENERAL insider-disclosure
# monitor, not a tracker aimed at any single named person. A feed
# built to alert the instant one specific real individual trades is a
# different kind of tool than a market-wide disclosure monitor, and
# not one this script builds — everyone who's legally required to
# disclose shows up here on the same terms, which is both more useful
# (broader coverage) and avoids treating one real person as a special
# surveillance target. The feed also can't yet tell you dollar size —
# EDGAR's lightweight "latest filings" feed lists WHO filed, not the
# transaction amount (that requires a heavier per-filing fetch); this
# gives you the real-time "something was just disclosed" signal, and
# is the foundation to add per-filing $-size filtering later.
SEC_FORM4_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
    "&type=4&company=&dateb=&owner=include&count=40&output=atom"
)
SEC_USER_AGENT = "OSINT-Terminal-Personal-Project (replace-with-your-email@example.com)"
INSIDER_POLL_SECONDS = 90
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}

# ---------------------------------------------------------
# PREDICTION MARKETS (Polymarket + Kalshi, free, no key)
# ---------------------------------------------------------
# Both endpoints confirmed public/read-only, no auth required, as of
# this script's writing (both vendors document this explicitly).
POLYMARKET_URL = (
    "https://gamma-api.polymarket.com/markets"
    "?active=true&closed=false&limit=5&order=volume24hr&ascending=false"
)
KALSHI_URL = "https://api.elections.kalshi.com/trade-api/v2/markets?limit=5&status=open"
PREDICTION_POLL_SECONDS = 90

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

POLL_INTERVAL = 1.5          # seconds between display redraws / keypress checks
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
war_headlines = deque(maxlen=5)       # rolling display of raw war/kinetic-query headlines
                                       # (broader than the "verified" subset that sets K)

labor_headline_queue = deque()        # newly detected, not-yet-displayed labor/enforcement events
labor_active_until = None             # datetime or None
labor_last_headline = None
labor_lock = threading.Lock()
seen_labor = set()

weather_alerts = []      # list of {"event", "area", "severity"}
weather_lock = threading.Lock()

flight_disruptions = []  # list of {"category", "airport", "reason"}
flight_lock = threading.Lock()

insider_filings = deque(maxlen=10)   # list of {"title", "link"}
insider_lock = threading.Lock()
seen_insider_links = set()

prediction_markets = []  # list of {"platform", "question", "yes_price", "volume"}
prediction_lock = threading.Lock()
prev_prediction_prices = {}  # question -> last-seen yes_price, for momentum term

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
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
            for item in root.findall('./channel/item')[:8]:
                title_el = item.find('title')
                title = title_el.text if title_el is not None else None
                if not title:
                    continue

                # Display buffer: EVERY matched war/kinetic-query headline
                # goes here, refreshed on this thread's own cadence
                # (adaptive, base 15s — comfortably "updated" more often
                # than the requested 5-minute floor). This is separate
                # from the stricter "verified" check below that drives K.
                with kinetic_lock:
                    if title not in war_headlines:
                        war_headlines.appendleft(title)

                if title in seen_kinetic:
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
# THREAD 6: INSIDER FILING DISCLOSURES (SEC EDGAR, free, no key)
# ---------------------------------------------------------
def fetch_insider_filings():
    backoff = INSIDER_POLL_SECONDS
    max_backoff = 10 * 60

    while True:
        try:
            req = urllib.request.Request(
                SEC_FORM4_FEED_URL, headers={"User-Agent": SEC_USER_AGENT}
            )
            with urllib.request.urlopen(req, timeout=8) as response:
                root = ET.fromstring(response.read())

            new_count = 0
            for entry in root.findall("a:entry", ATOM_NS):
                title_el = entry.find("a:title", ATOM_NS)
                link_el = entry.find("a:link", ATOM_NS)
                title = title_el.text if title_el is not None else None
                link = link_el.get("href") if link_el is not None else None
                if not title or not link or link in seen_insider_links:
                    continue
                seen_insider_links.add(link)
                with insider_lock:
                    insider_filings.appendleft({"title": title, "link": link})
                new_count += 1

            backoff = INSIDER_POLL_SECONDS
            logger.info("Insider filings refreshed: %d new Form 4(s)", new_count)
        except Exception as exc:
            logger.warning("SEC EDGAR fetch failed: %s", exc)
            backoff = min(backoff * 1.5, max_backoff)
        time.sleep(backoff)


# ---------------------------------------------------------
# THREAD 7: PREDICTION MARKETS (Polymarket + Kalshi, free, no key)
# ---------------------------------------------------------
def _parse_polymarket_payload(markets_json):
    parsed = []
    for m in markets_json:
        try:
            outcome_prices = m.get("outcomePrices")
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)
            yes_price = float(outcome_prices[0]) if outcome_prices else None
            parsed.append({
                "platform": "Polymarket",
                "question": m.get("question", "Unknown market"),
                "yes_price": yes_price,
                "volume": float(m.get("volume24hr") or 0),
            })
        except (TypeError, ValueError, IndexError, json.JSONDecodeError) as exc:
            logger.debug("Polymarket entry parse skipped: %s", exc)
    return parsed


def _parse_kalshi_payload(payload):
    parsed = []
    for m in payload.get("markets", []):
        try:
            yes_bid = m.get("yes_bid")
            parsed.append({
                "platform": "Kalshi",
                "question": m.get("title", m.get("ticker", "Unknown market")),
                "yes_price": (yes_bid / 100.0) if yes_bid is not None else None,
                "volume": float(m.get("volume") or 0),
            })
        except (TypeError, ValueError) as exc:
            logger.debug("Kalshi entry parse skipped: %s", exc)
    return parsed


def fetch_prediction_markets():
    global prediction_markets
    backoff = PREDICTION_POLL_SECONDS
    max_backoff = 10 * 60

    while True:
        combined = []
        try:
            req = urllib.request.Request(POLYMARKET_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as response:
                combined.extend(_parse_polymarket_payload(json.loads(response.read())))
        except Exception as exc:
            logger.warning("Polymarket fetch failed: %s", exc)

        try:
            req = urllib.request.Request(KALSHI_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as response:
                combined.extend(_parse_kalshi_payload(json.loads(response.read())))
        except Exception as exc:
            logger.warning("Kalshi fetch failed: %s", exc)

        if combined:
            with prediction_lock:
                prediction_markets = combined
            backoff = PREDICTION_POLL_SECONDS
            logger.info("Prediction markets refreshed: %d markets", len(combined))
        else:
            backoff = min(backoff * 1.5, max_backoff)

        time.sleep(backoff)


def compute_prediction_momentum():
    """Average absolute change in tracked markets' YES price since the
    last poll — an experimental proxy for 'how much is prediction-
    market sentiment moving right now', feeding the composite index."""
    global prev_prediction_prices
    with prediction_lock:
        snapshot = list(prediction_markets)

    if not snapshot:
        return 0.0, snapshot

    deltas = []
    new_prev = {}
    for m in snapshot:
        key = f"{m['platform']}:{m['question']}"
        new_prev[key] = m["yes_price"]
        if m["yes_price"] is not None and key in prev_prediction_prices and prev_prediction_prices[key] is not None:
            deltas.append(abs(m["yes_price"] - prev_prediction_prices[key]))
    prev_prediction_prices = new_prev

    momentum = float(np.mean(deltas)) if deltas else 0.0
    return min(momentum / 0.05, 1.0), snapshot  # normalize: 5pt move = maxed out


# ---------------------------------------------------------
# UNUSUAL ACTIVITY / LARGE-TRADE DETECTION
# ---------------------------------------------------------
def is_late_session():
    now_et = datetime.datetime.now(ET_ZONE)
    close_dt = now_et.replace(hour=MARKET_CLOSE_HOUR_ET, minute=0, second=0, microsecond=0)
    minutes_to_close = (close_dt - now_et).total_seconds() / 60.0
    return 0 <= minutes_to_close <= LATE_SESSION_WINDOW_MINUTES, minutes_to_close


def log_unusual_activity(ticker, price, pct_change, volume, vol_z, late_session, minutes_to_close):
    is_new = not UNUSUAL_ACTIVITY_LOG_PATH.exists()
    with open(UNUSUAL_ACTIVITY_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow([
                "timestamp", "ticker", "price", "pct_change", "volume",
                "volume_z_score", "late_session", "minutes_to_close", "severity",
            ])
        severity = "SUSPICIOUS_LATE_SESSION" if late_session else "UNUSUAL_VOLUME"
        writer.writerow([
            datetime.datetime.now().isoformat(timespec="seconds"), ticker,
            f"{price:.4f}", f"{pct_change:.5f}", f"{volume:.0f}", f"{vol_z:.2f}",
            late_session, f"{minutes_to_close:.1f}", severity,
        ])
        return severity


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


def compute_composite_index(K, L, weather_count, flight_count, prediction_momentum=0.0):
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
        + w["prediction_markets"] * prediction_momentum
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
    threading.Thread(target=fetch_insider_filings, name="Insider-Radar", daemon=True).start()
    threading.Thread(target=fetch_prediction_markets, name="PredictionMkt-Radar", daemon=True).start()


def fetch_prices_and_volume(tickers):
    """Download current price AND volume for a ticker list. Returns
    (price_dict, volume_dict); either can be partial. Never raises —
    logs and degrades to cached values on failure."""
    price_result, volume_result = {}, {}
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
        return price_result, volume_result

    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                close_series = data['Close'][ticker].dropna() if ticker in data['Close'] else pd.Series(dtype=float)
                vol_series = data['Volume'][ticker].dropna() if 'Volume' in data and ticker in data['Volume'] else pd.Series(dtype=float)
            else:
                close_series = data['Close'].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
                vol_series = data['Volume'].dropna() if len(tickers) == 1 and 'Volume' in data else pd.Series(dtype=float)
            if not close_series.empty:
                price_result[ticker] = float(close_series.iloc[-1])
            if not vol_series.empty:
                volume_result[ticker] = float(vol_series.iloc[-1])
        except Exception as exc:
            logger.debug("Price/volume extraction failed for %s: %s", ticker, exc)
    return price_result, volume_result


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
    print("[+] Spinning up SEC EDGAR insider filing feed (free, base 90s)...")
    print("[+] Spinning up Polymarket + Kalshi prediction market feed (free, base 90s)...")
    print(f"[+] Price polling: base {PRICE_POLL_BASE_SECONDS}s, adaptive backoff on throttling "
          f"(see file header for the honest ceiling on 'lagless')")
    print(f"[+] Model: delta_I_hat = b0 + b1*E_t + b2*(E_t*K) + b3*T_t "
          f"(coefficients provisional — see file header)")
    print(f"[+] Composite friction index: experimental, unweighted heuristic — see file header")
    if not IS_WINDOWS:
        print(f"{YELLOW}[!] Non-Windows OS detected: hotkey view-switching disabled "
              f"(msvcrt is Windows-only). View 1 will run continuously.{RESET}")
    logger.info("Terminal started.")
    time.sleep(1.5)

    start_background_threads()
    price_backoff = PRICE_POLL_BASE_SECONDS
    last_price_fetch = 0.0

    try:
        while True:
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

            # Adaptive-lag price fetch: go as fast as PRICE_POLL_BASE_SECONDS
            # allows, back off only when actually throttled, speed back up
            # the moment a fetch succeeds again.
            now_mono = time.monotonic()
            if now_mono - last_price_fetch >= price_backoff:
                fresh_prices, fresh_volumes = fetch_prices_and_volume(active_tickers)
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
                    logger.info("Price fetch backoff now %.1fs", price_backoff)

            K, k_remaining = get_kinetic_flag()
            E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
            T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
            delta_hat = compute_delta_i_hat(E_t, T_t, K)

            L, l_remaining = get_labor_flag()
            with weather_lock:
                weather_snapshot = list(weather_alerts)
            with flight_lock:
                flight_snapshot = list(flight_disruptions)
            with kinetic_lock:
                war_snapshot = list(war_headlines)
            with insider_lock:
                insider_snapshot = list(insider_filings)
            prediction_momentum, prediction_snapshot = compute_prediction_momentum()
            composite = compute_composite_index(
                K, L, len(weather_snapshot), len(flight_snapshot), prediction_momentum
            )

            late_session, minutes_to_close = is_late_session()

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
                vbuf = volume_buffers[ticker]
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

                # Volume-based "huge investment" detection: z-score of the
                # latest volume reading against this ticker's own recent
                # history. Escalates to SUSPICIOUS when it coincides with
                # the final LATE_SESSION_WINDOW_MINUTES before close.
                vol_alert = None
                if len(vbuf) > 5:
                    vhist = list(vbuf)[:-1]
                    vstd = np.std(vhist)
                    vmean = np.mean(vhist)
                    curr_vol = vbuf[-1]
                    vol_z = (curr_vol - vmean) / (vstd if vstd != 0 else 1.0)
                    if vol_z >= UNUSUAL_VOLUME_Z:
                        severity = log_unusual_activity(
                            ticker, curr_price, pct_change or 0.0, curr_vol, vol_z,
                            late_session, minutes_to_close,
                        )
                        vol_alert = (severity, vol_z)
                        logger.info("%s on %s: vol_z=%.2f late_session=%s",
                                    severity, ticker, vol_z, late_session)

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

                if vol_alert:
                    severity, vol_z = vol_alert
                    color = PURPLE if severity == "SUSPICIOUS_LATE_SESSION" else RED
                    diamond = "!"
                    status = "⚠ LATE-SESSION" if severity == "SUSPICIOUS_LATE_SESSION" else "⚠ HUGE VOLUME"

                print(f"{color} {diamond}{ticker:<7} | {curr_price:<9.2f} | {pct_change*100:>8.2f}% | "
                      f"{z_score:>7.2f} | {sparkline} | {status:<15}{RESET}")

                if vol_alert:
                    severity, vol_z = vol_alert
                    tag = "SUSPICIOUS — LATE-SESSION UNUSUAL ACTIVITY" if severity == "SUSPICIOUS_LATE_SESSION" else "UNUSUAL VOLUME / LARGE TRADE"
                    print(f"{color}    ↳ ALERT: {tag} on {ticker} — volume z-score {vol_z:.1f}"
                          f"{f', {minutes_to_close:.0f}m to close' if late_session else ''}. "
                          f"Logged to {UNUSUAL_ACTIVITY_LOG_PATH.name}.{RESET}")

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
            print(f"{RED}  WAR / KINETIC HEADLINES  (min. 3, refreshed continuously — well under the 5-min floor){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            if len(war_snapshot) < 3:
                print(f" {GRAY}Awaiting more headlines to reach the 3-headline minimum "
                      f"({len(war_snapshot)}/3 so far)...{RESET}")
            for h in war_snapshot[:5]:
                print(f" {RED}•{RESET} {h[:85]}")

            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{PURPLE}  PREDICTION MARKETS  (Polymarket + Kalshi, momentum term: {prediction_momentum:.2f}/1.00){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            if not prediction_snapshot:
                print(f" {GRAY}Aggregating prediction market feeds...{RESET}")
            else:
                for m in prediction_snapshot[:6]:
                    yp = f"{m['yes_price']*100:.0f}%" if m["yes_price"] is not None else "N/A"
                    print(f" [{m['platform']:<10}] {m['question'][:60]:<60} | YES: {yp:<5} | Vol: ${m['volume']:,.0f}")

            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{PURPLE}  INSIDER FILINGS  (SEC EDGAR Form 4, market-wide, general monitor — not person-targeted){RESET}")
            print(f"{BLUE}========================================================================================={RESET}")
            if not insider_snapshot:
                print(f" {GRAY}Aggregating SEC EDGAR filing feed...{RESET}")
            else:
                for f_item in insider_snapshot[:4]:
                    print(f" {f_item['title'][:85]}")

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
            print(f"{GRAY} Log: {LOG_PATH.name}  |  Kinetic: {EVENT_LOG_PATH.name}  |  "
                  f"Labor: {LABOR_EVENT_LOG_PATH.name}  |  Unusual activity: {UNUSUAL_ACTIVITY_LOG_PATH.name}{RESET}")

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
