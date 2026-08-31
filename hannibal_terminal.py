"""
OSINT COMMAND TERMINAL — PROJECT HANNIBAL (Merged Build)
==========================================================================
Base engine: VolcanoLlamaNapalm.py (V2.2 — most complete feature set)
Folded in from other iterations, with nothing dropped:
  - HBL5.py     : SQLite intel-log data lake, interactive AI chat query,
                  SEC EDGAR token-bucket rate limiter
  - addendum.py : labor-strike / unusual-volatility CSV scoring, blended
                  into the composite index as extra terms

Architecture:
- AUTONOMOUS OLLAMA BOOTSTRAPPER: scans drives for ollama.exe and launches it.
- DYNAMIC ALL-POINTS TACTICAL MAP [M]: real-time plotting of active telemetry.
- INTERACTIVE GRAPH MATRIX [G]: multi-option overlays (momentum/volume/friction).
- BULLETPROOF SEC EDGAR: multi-stage XML extraction, rate-limited, with
  local SQLite persistence of every logged intel event.
- LIVE CHAT [T]: ask the local LLM a direct question with full live context.
- LABOR/VOLATILITY LAYER: CSV-driven friction terms blended into the model.

Clearance: EID Verified (B. Noffels)
==========================================================================
"""

import os
import sys
import csv
import json
import time
import platform
import re
import sqlite3
import hashlib
import base64
import atexit
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
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

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
DB_PATH = SCRIPT_DIR / "hannibal_intel.sqlite3"

logging.basicConfig(filename=str(LOG_PATH), level=logging.INFO, format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")
logger = logging.getLogger("osint_terminal")

# ---------------------------------------------------------
# PLATFORM HANDLING
# ---------------------------------------------------------
IS_WINDOWS = platform.system() == "Windows"
if IS_WINDOWS:
    import msvcrt
    import winsound  # [from NPM.py] Windows-only stdlib, no new dependency
    os.system('chcp 65001 > nul')
    os.system('')
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
# ---------------------------------------------------------
# SPEED PROFILES
# ---------------------------------------------------------
# Three named modes governing every polling/refresh interval in the
# terminal. "Normal" numbers are exactly the previous hardcoded defaults --
# switching to Normal is a no-op for behavior, so nothing changes unless
# the person actually opens the speed menu ([Y] key, see main()).
#
# Faster modes mean more frequent network calls (news feeds, prices, SEC
# EDGAR) and a snappier screen redraw; slower modes mean less network load
# and CPU use, at the cost of staler data on screen. The trade-off is
# entirely up to the person running it -- this system only makes it a
# keypress instead of a source-code edit.
SPEED_PROFILES = {
    "slow": {
        "label": "SLOW (low load)",
        "osint_batch_interval": 180.0,   # was 90.0 -- half as often
        "fast_poll_interval": 30.0,      # was 15.0
        "price_poll_base": 3.0,          # was 1.5
        "price_poll_max_backoff": 60.0,  # was 30.0
        "poll_interval": 0.5,            # was 0.3 -- input/redraw check less often
    },
    "normal": {
        "label": "NORMAL (default)",
        "osint_batch_interval": 90.0,
        "fast_poll_interval": 15.0,
        "price_poll_base": 1.5,
        "price_poll_max_backoff": 30.0,
        "poll_interval": 0.3,
    },
    "fast": {
        "label": "FAST (high load)",
        "osint_batch_interval": 45.0,    # twice as often
        "fast_poll_interval": 7.5,
        "price_poll_base": 0.75,
        "price_poll_max_backoff": 15.0,
        "poll_interval": 0.15,
    },
}

_speed_lock = threading.Lock()
_current_speed_mode = "normal"

def set_speed_mode(mode):
    """Switches the active speed profile. Every daemon thread reads its
    interval fresh via get_speed_value() on each loop iteration (see
    fast_poll_daemon, osint_sync_daemon, price_poll_daemon below) rather
    than caching a value once at thread start -- so a switch here takes
    effect on each daemon's very next sleep/poll, without needing to
    restart any thread."""
    global _current_speed_mode
    if mode not in SPEED_PROFILES:
        return False
    with _speed_lock:
        _current_speed_mode = mode
    return True

def get_speed_mode():
    with _speed_lock:
        return _current_speed_mode

def get_speed_value(key):
    with _speed_lock:
        mode = _current_speed_mode
    return SPEED_PROFILES[mode][key]

DATA_TTL_SECONDS = 3600.0

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3"
OLLAMA_TIMEOUT_SECONDS = 180
OLLAMA_CHAT_TIMEOUT_SECONDS = 45

MODEL_COEFFICIENTS = {"beta0": 0.0276, "beta1": 0.0356, "beta2": -0.0251, "beta3": -0.0006, "beta4": 0.0015}

ENERGY_PROXY_TICKERS = ["BZ=F", "CL=F"]
TRANSPORT_PROXY_TICKERS = ["BDRY", "IYT"]
UNUSUAL_VOLUME_Z = 3.0
MARKET_CLOSE_HOUR_ET = 16
LATE_SESSION_WINDOW_MINUTES = 30

ET_ZONE = ZoneInfo("America/New_York")
LOCAL_ZONE = ZoneInfo("Europe/Lisbon")

# NOTE: kinetic/infra/labor/flights/weather/prediction_markets weights sum to 1.0.
# Labor and volatility from the CSV layer are added as small extra friction
# terms on top (see compute_composite_index), matching addendum.py's approach
# of *adding* labor_term/vol_term to a base rather than folding them into the
# weighted average — kept separate so neither signal can silently swamp the
# other five.
COMPOSITE_WEIGHTS = {"kinetic": 0.20, "infrastructure": 0.20, "labor": 0.10, "flights": 0.10, "weather": 0.10, "prediction_markets": 0.30}
COMPOUNDING_CHOKEPOINTS = ["hormuz", "bab el-mandeb", "suez", "malacca", "panama canal", "bosphorus", "tsmc", "subsea cable", "lng terminal", "uranium enrichment", "iaea", "nuclear reactor", "black sea grain", "taiwan strait", "south china sea", "crimea", "donbas", "gaza", "tehran", "riyadh", "beijing", "shanghai", "mumbai", "kashmir"]

FORM4_DETAIL_MAX_PER_BATCH = 10
LARGE_INSIDER_TRADE_USD = 1_000_000
OPEN_MARKET_CODES = {"P": "PURCHASE", "S": "SALE"}

PREDICTION_DISCLAIMER = "Experimental model output under hand-picked, unfit weights — NOT financial advice."

# ---------------------------------------------------------
# SEC EDGAR TOKEN-BUCKET RATE LIMITER  [from HBL5.py]
# ---------------------------------------------------------
# SEC's fair-access policy throttles/blocks IPs that hammer edgar endpoints.
# Volcano's batch sync fires up to 100 concurrent requests per cycle with no
# limiter at all, which risks an IP ban on the exact feed the whole insider-
# trading view depends on. This limiter is shared by every sec.gov request
# (the main atom feed AND each per-filing detail fetch) and caps them to a
# steady rate regardless of how many threads try to call in at once.
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
# SQLITE INTEL DATA LAKE  [from HBL5.py]
# ---------------------------------------------------------
db_lock = threading.Lock()

def init_db():
    """Creates the intel database ENTIRELY IN MEMORY (sqlite3.connect(':memory:'))
    rather than as a file on disk. This is what makes "encrypted even while
    running" real rather than "encrypted at rest but briefly plaintext during
    use": there is no point in the program's life where a plain, readable
    .sqlite3 file exists -- the working copy only ever exists in RAM.

    If an encrypted database file already exists from a previous session,
    it's decrypted and loaded into the in-memory db here via SQLite's
    backup() API (memory <-> file transfer, stdlib only, no extra
    dependency). If decryption fails (wrong password -- shouldn't happen
    since this only runs after a successful login with the same password,
    but checked anyway -- or a pre-encryption plaintext leftover file),
    logs a warning and starts a fresh empty database rather than crashing.

    Must be called AFTER a successful login (see run_login_gate() and
    main()), not at module load time: decrypting an existing db file needs
    the password-derived key, which doesn't exist until login succeeds."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
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
    # [from NPM.py's schema] A structured table for whale trades specifically,
    # with typed REAL columns (shares/price/value_usd) instead of the generic
    # intel_logs table where everything is TEXT -- lets a trade be queried or
    # sorted numerically (e.g. "top 10 by value_usd") without parsing a
    # formatted "$1,234,567" string back into a number. NPM.py created this
    # table but never actually inserted into it; log_insider_to_db() below
    # is the insert that was missing.
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

    if DB_PATH.exists():
        try:
            encrypted_bytes = DB_PATH.read_bytes()
            decrypted_bytes = decrypt_bytes(encrypted_bytes)
            # backup() needs a real file-backed connection as the source,
            # so the decrypted bytes are written to a TEMPORARY file just
            # long enough to load them into the in-memory db, then that
            # temp file is deleted immediately -- it never has a
            # predictable name or lives any longer than this one call.
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
                tmp_path = Path(tmp.name)
                tmp.write(decrypted_bytes)
            try:
                temp_conn = sqlite3.connect(str(tmp_path))
                temp_conn.backup(conn)
                temp_conn.close()
                logger.info("Decrypted and loaded existing intel database from previous session.")
            finally:
                tmp_path.unlink(missing_ok=True)
        except Exception as e:
            # Widened from (InvalidToken, ValueError, sqlite3.Error) to a
            # bare Exception: the narrower tuple didn't cover OS-level
            # failures around the temp file itself (permission errors, a
            # locked/inaccessible temp directory, antivirus interference,
            # disk issues) -- real possibilities on an arbitrary Windows
            # machine, especially for an unsigned .exe. Any of those
            # previously propagated straight out of init_db() uncaught and
            # killed the whole process right after the person's actual
            # data had been decrypted and briefly visible on screen --
            # exactly a "flash of real data then the window goes black"
            # crash, with the real cause left in the log's stderr, which a
            # plain double-click launch doesn't keep open to show anyone.
            # Falling back to a fresh empty database here is a strictly
            # better outcome than the process dying with no visible reason.
            logger.warning(f"Could not decrypt/load existing {DB_PATH.name} (wrong password, corruption, a pre-encryption plaintext file, or a filesystem error): {e}. Starting with a fresh database.")

    return conn

def save_db_encrypted():
    """Serializes the in-memory database to a temporary file (via the same
    backup() API used to load it), encrypts that file's bytes, writes the
    encrypted result to DB_PATH, and deletes the temporary plaintext copy.
    Called on normal exit and on Ctrl+C (see main()) so the session's data
    is actually persisted -- an in-memory database that's never saved would
    lose everything the moment the process ends, which defeats the point
    of logging any of this in the first place."""
    global db_conn
    if db_conn is None:
        return
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            temp_conn = sqlite3.connect(str(tmp_path))
            db_conn.backup(temp_conn)
            temp_conn.close()
            plaintext_bytes = tmp_path.read_bytes()
            encrypted = encrypt_bytes(plaintext_bytes)
            if encrypted is not None:
                DB_PATH.write_bytes(encrypted)
                logger.info("Encrypted intel database saved.")
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception as e:
        logger.error(f"Failed to save encrypted database: {e}")

db_conn = None  # set by init_db() after a successful login, NOT at module
                 # load -- see init_db()'s docstring for why

def log_to_db(category, region, headline, data_val, prediction):
    try:
        with db_lock:
            cursor = db_conn.cursor()
            cursor.execute(
                "INSERT INTO intel_logs (timestamp, category, region, headline, data_val, prediction) VALUES (?, ?, ?, ?, ?, ?)",
                (datetime.datetime.now().isoformat(), category, region, headline, data_val, prediction)
            )
            db_conn.commit()
    except Exception:
        pass

def log_insider_to_db(tx):
    """Inserts the SAME tx dict that log_to_db("SEC_WHALE", ...) and the
    large-trade CSV already log, at the same call site (see sync_osint_feeds)
    -- not a second fetch or a recomputed value, so this can't disagree with
    what's already logged elsewhere for the same trade. shares/price/value
    come straight from fetch_form4_detail's live SEC XML parse."""
    try:
        with db_lock:
            cursor = db_conn.cursor()
            cursor.execute(
                "INSERT INTO insider_logs (timestamp, issuer, owner, role, code, shares, price, value_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (datetime.datetime.now().isoformat(), tx["issuer"], tx["owner"], tx["role"], tx["code_label"], tx["shares"], tx["price"], tx["value"])
            )
            db_conn.commit()
    except Exception:
        pass

# ---------------------------------------------------------
# AUTONOMOUS OLLAMA BOOTSTRAPPER (Finds and boots Llama)
# ---------------------------------------------------------
llm_status_string = "Initializing local neural bridge..."

def recursive_find_ollama():
    """Walks each drive root looking for ollama.exe, rather than checking a
    fixed list of common install paths. The previous version (below, kept
    for reference in git history / prior turns) only checked 5 hardcoded
    paths and silently gave up if Ollama was installed anywhere else --
    a custom install dir, a non-default drive letter's folder structure,
    etc. This finds it wherever it actually lives, at the cost of walking
    the whole drive if it's not in an obvious spot. [from hbl2.py / N4P1M.py]
    """
    drives = ["F:", "C:", "D:", "E:", "G:"]  # F: first, matching prior priority
    for drv in drives:
        if not os.path.exists(drv):
            continue
        try:
            for root, dirs, files in os.walk(drv):
                if "ollama.exe" in files:
                    return os.path.join(root, "ollama.exe")
        except Exception:
            continue
    return None

# ---------------------------------------------------------
# LOGIN GATE
# ---------------------------------------------------------
# Honest framing, worth keeping in the source: this is a real deterrent
# against a casual attempt to open the terminal, NOT cryptographic security.
# It runs on the person's own machine with no server to check against, so:
#   - The hash below CAN be extracted from the compiled .exe and brute-forced
#     offline by someone who specifically wants to; SHA-256 with no salt is
#     fast to attack that way. What it genuinely prevents is a glance at the
#     source (or a decompile) instantly revealing the plain-text password,
#     which plain-text storage would.
#   - The lockout is per-launch, in memory only. Closing and reopening the
#     terminal resets the attempt counter -- there's nowhere to persist a
#     lockout across restarts without a machine-independent store, and an
#     in-memory lock is the honest version of what a single local file can
#     actually enforce.
#   - The SQLite database is now genuinely gated by this login too: it runs
#     entirely in memory and init_db() is only ever called explicitly AFTER
#     a successful login (see main()), since decrypting a previous
#     session's data needs the password-derived key. No plain .sqlite3
#     file exists on disk at any point while the terminal is running.
#
# PLACEHOLDER: replace this with the real SHA-256 hex digest of the actual
# password before building. Compute it with:
#   python3 -c "import hashlib; print(hashlib.sha256('YOUR_PASSWORD'.encode()).hexdigest())"
LOGIN_PASSWORD_HASH = "f7de1c5f2f6289600f12f0653da7cc1eedbcda556992524efca36404bc22c8ea"  # "Inveniemus viam aut faciemus"

LOGIN_MAX_ATTEMPTS = 3
LOGIN_LOCKOUT_SECONDS = 300.0  # 5 minutes

_login_log_buffer = []  # (level, message) tuples generated during login,
                          # before the log file has been safely returned to
                          # plaintext -- see run_login_gate()'s docstring
                          # and flush_login_log_buffer() below

# ASCII banner, generated once with pyfiglet (font: slant) and hardcoded
# here so the running terminal doesn't need pyfiglet installed -- only the
# machine that regenerates this art needs it.
LOGIN_BANNER = r"""
    __  ____ __  _   ___   _________  __ _______
   / / / / // / / | / / | / <  / __ )/ // /__  /
  / /_/ / // /_/  |/ /  |/ // / __  / // /_ / /
 / __  /__  __/ /|  / /|  // / /_/ /__  __// /
/_/ /_/  /_/ /_/ |_/_/ |_//_/_____/  /_/  /_/
"""

def _hash_password(pw):
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def run_login_gate():
    """Blocks until a correct password is entered or the process exits.
    Returns True to proceed, never returns False -- a failed final attempt
    exits the process directly (sys.exit), since there is no "continue
    without access" state for this terminal to fall back to.

    Log messages generated here are buffered in memory (_login_log_buffer)
    rather than written via logger directly. Reason: this function runs
    BEFORE decrypt_log_for_session() ever gets a chance to run (that only
    happens in main(), after this function returns successfully) -- so at
    this point LOG_PATH may still hold a PREVIOUS session's ENCRYPTED
    bytes, and any direct logger call here would append plaintext onto
    the end of that encrypted blob, corrupting it (the exact bug
    decrypt_log_for_session() exists to prevent, hit via a different
    path). The buffer is flushed to the real log safely in main(), after
    decrypt_log_for_session() has made the file plaintext again. On a
    lockout or aborted login, the buffer is simply discarded -- there is
    no key available at that point to write anything to disk safely."""
    global _login_log_buffer
    _login_log_buffer = []

    if LOGIN_PASSWORD_HASH == "REPLACE_WITH_REAL_SHA256_HASH_BEFORE_BUILDING":
        # Fails loudly and immediately rather than silently accepting any
        # password (or none) -- an unset password gate is worse than no
        # gate at all, because it LOOKS locked while accepting anything.
        print(f"{RED}[!] LOGIN_PASSWORD_HASH is still the placeholder value.{RESET}")
        print(f"{RED}    Set it to a real SHA-256 hash before running. See the comment above it.{RESET}")
        sys.exit(1)

    clear_screen()
    print(f"{RED}{LOGIN_BANNER}{RESET}")
    print(f"{PURPLE}                    PROJECT NAPALM-1.0{RESET}")
    print(f"{BLUE}========================================================================={RESET}")

    attempts_remaining = LOGIN_MAX_ATTEMPTS
    while attempts_remaining > 0:
        try:
            entered = input(f"{YELLOW} ACCESS CODE ({attempts_remaining} attempt(s) remaining): {RESET}")
        except (EOFError, KeyboardInterrupt):
            print(f"\n{RED}[!] Login aborted.{RESET}")
            sys.exit(1)

        if _hash_password(entered) == LOGIN_PASSWORD_HASH:
            print(f"{GREEN}[+] Access granted.{RESET}")
            _derive_encryption_key(entered)
            _login_log_buffer.append(("info", "Login succeeded."))
            time.sleep(0.6)
            return True

        attempts_remaining -= 1
        _login_log_buffer.append(("warning", f"Failed login attempt ({LOGIN_MAX_ATTEMPTS - attempts_remaining}/{LOGIN_MAX_ATTEMPTS})."))
        if attempts_remaining > 0:
            print(f"{RED}[!] Access denied. {attempts_remaining} attempt(s) remaining.{RESET}")
        else:
            print(f"{RED}[!] Access denied. Maximum attempts reached.{RESET}")
            print(f"{RED}[!] Locked for {int(LOGIN_LOCKOUT_SECONDS // 60)} minutes. Terminal will now exit.{RESET}")
            # NOT buffered for later writing -- there is no successful
            # login coming, so no safe point will ever exist to flush
            # this to disk. It's printed to the screen above, which is
            # the only place it can safely go.
            # Held here (not just exited immediately) so the lockout window
            # is actually enforced within this run -- exiting right away
            # would let someone just relaunch the exe instantly and get
            # 3 fresh attempts with no real delay at all.
            time.sleep(LOGIN_LOCKOUT_SECONDS)
            sys.exit(1)
    return False  # unreachable, but explicit rather than implicit None

# ---------------------------------------------------------
# DATA ENCRYPTION AT REST
# ---------------------------------------------------------
# All 7 output files (this .log, the 4 .csv logs, the prediction audit
# .txt, and the SQLite intel database) are encrypted using the SAME
# password that opens the login screen -- so opening any of them in
# Notepad, a spreadsheet app, or a SQLite browser shows unreadable bytes,
# not the plain data.
#
# Honest limits, worth stating plainly rather than implying more than this
# delivers: this is real, correctly-implemented encryption (AES via the
# `cryptography` library's Fernet scheme, not a hand-rolled cipher) -- but
# the key is DERIVED FROM a password that also has to live in this same
# compiled .exe (as a hash, for the login check) for the program to
# function offline with no server to check against. Someone who
# specifically extracts and cracks that hash gains the same password that
# unlocks the data. What this genuinely stops: casually opening any of
# these files by name (double-click, right-click > Open With) shows
# nothing readable -- not a determined, targeted attack on the file itself.
ENCRYPTION_SALT = b'PROJECT_HANNIBAL_NAPALM_SALT_v1'  # fixed and NOT secret
                    # -- salts defend against rainbow-table attacks across
                    # many different users/passwords sharing infrastructure,
                    # which doesn't apply here: one password, one person,
                    # one machine. A random salt would need to be stored
                    # somewhere retrievable anyway to decrypt next run,
                    # which buys nothing extra in this single-user case.
ENCRYPTION_KDF_ITERATIONS = 480000  # OWASP's 2023 minimum recommendation
                                     # for PBKDF2-SHA256; slows down anyone
                                     # trying to brute-force the password
                                     # against the login hash without
                                     # making normal login noticeably slower

_fernet = None  # set once by _derive_encryption_key() after a successful
                 # login; every encrypt/decrypt call below uses this

def _derive_encryption_key(password):
    """Derives a Fernet key from the password and stores it for the
    session. Called once, immediately after the login hash check succeeds
    in run_login_gate() -- so the exact string that unlocked the terminal
    is also what unlocks the data, with no second password to remember."""
    global _fernet
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                      salt=ENCRYPTION_SALT, iterations=ENCRYPTION_KDF_ITERATIONS)
    key = base64.urlsafe_b64encode(kdf.derive(password.encode("utf-8")))
    _fernet = Fernet(key)

def encrypt_bytes(data):
    """Encrypts raw bytes. Returns None if called before a successful
    login (shouldn't happen in practice since every write site runs after
    the login gate, but fails loudly rather than silently writing
    plaintext if it ever did)."""
    if _fernet is None:
        logger.error("encrypt_bytes() called before login -- refusing to write plaintext.")
        return None
    return _fernet.encrypt(data)

def decrypt_bytes(data):
    """Decrypts bytes. Raises InvalidToken if the data was encrypted with
    a different password, or ValueError/InvalidToken if it's not
    encrypted data at all (e.g. a leftover plaintext file from before
    encryption was added) -- both cases are handled at each call site
    rather than here, since the right fallback differs per file type."""
    if _fernet is None:
        raise RuntimeError("decrypt_bytes() called before login.")
    return _fernet.decrypt(data)

def read_encrypted_text(path, encoding="utf-8"):
    """Reads and decrypts a text file written by write_encrypted_text().
    Returns None if the file doesn't exist. If the file exists but isn't
    valid encrypted data (an old plaintext file from before this feature,
    or genuine corruption), logs a warning and returns None rather than
    crashing -- callers already treat None/missing the same way they
    treated a missing file before encryption existed."""
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        if not raw:
            return None
        return decrypt_bytes(raw).decode(encoding)
    except (InvalidToken, ValueError) as e:
        logger.warning(f"Could not decrypt {path.name} (wrong password, or a pre-encryption plaintext file): {e}")
        return None

def write_encrypted_text(path, text, encoding="utf-8"):
    """Encrypts and writes a full text file, REPLACING any existing
    content. Unlike a plain append, encrypted data can't be appended to
    in place (each encrypt call produces a self-contained token) -- so
    every write site that used to open a file in append mode now reads
    the current decrypted content first, adds the new line, and writes
    the whole thing back encrypted. See the CSV logging call sites for
    how this is handled without changing the visible row-by-row logging
    behavior."""
    try:
        path.write_bytes(encrypt_bytes(text.encode(encoding)))
    except Exception as e:
        logger.error(f"Failed to write encrypted file {path.name}: {e}")

def decrypt_log_for_session():
    """Must be called once, immediately after a successful login and
    BEFORE any other logging call in this session (see main()). Fixes a
    real bug found during testing: logging.basicConfig() opens LOG_PATH in
    append mode at module load time, before login -- if the file still
    holds last session's ENCRYPTED bytes (written by
    save_encrypted_data_on_exit() when that session closed), a new
    session's plaintext log lines get appended directly onto the end of
    that encrypted blob. The result is a file that's neither valid
    encrypted data (Fernet can't decrypt encrypted-bytes-plus-trailing-
    plaintext) nor valid plaintext (it still starts with binary) --
    genuinely corrupted, and it would get worse every session.

    This decrypts any existing encrypted log content back to plaintext,
    closes logging's file handle, rewrites the file as plain text, and
    reopens logging in append mode against the now-plaintext file -- so
    for the rest of this session, logging is appending to genuinely
    plaintext content exactly as it always has. The file only becomes
    encrypted again at final exit, via save_encrypted_data_on_exit()."""
    if not LOG_PATH.exists():
        return
    try:
        existing_bytes = LOG_PATH.read_bytes()
        if not existing_bytes:
            return
        try:
            decrypted = decrypt_bytes(existing_bytes)
        except (InvalidToken, ValueError):
            # Not encrypted (a pre-encryption plaintext leftover file, or
            # this is somehow already the first-ever run) -- leave it
            # alone, logging can keep appending to it as-is.
            return

        # Close the handler logging currently holds open on this file,
        # rewrite it as plaintext, then reopen logging against it in
        # append mode so the rest of this session's log lines land after
        # the recovered plaintext, not after raw encrypted bytes.
        for handler in list(logging.root.handlers):
            handler.close()
            logging.root.removeHandler(handler)
        LOG_PATH.write_bytes(decrypted)
        logging.basicConfig(filename=str(LOG_PATH), level=logging.INFO,
                             format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")
    except Exception:
        pass  # if this fails, logging continues against whatever state
              # the file was already in -- not ideal, but no worse than
              # before this function existed

def flush_login_log_buffer():
    """Writes the (level, message) pairs buffered by run_login_gate() to
    the real logger. Must be called AFTER decrypt_log_for_session() (see
    main()), since that's the point where LOG_PATH is confirmed to hold
    plaintext again -- writing these any earlier would risk the same
    corruption decrypt_log_for_session() exists to prevent."""
    for level, message in _login_log_buffer:
        if level == "info":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)

def save_encrypted_data_on_exit():
    """Runs once, at process exit, via atexit (registered below). Handles
    the two things that can't be encrypted incrementally as the program
    runs:
      - The SQLite database: see save_db_encrypted(), which serializes the
        in-memory db to disk, encrypted, since it otherwise only exists in
        RAM and would be lost entirely when the process ends.
      - The .log file: logging owns this file handle continuously for the
        whole process lifetime (see logging.basicConfig above), so it's
        genuinely plaintext WHILE the terminal is running (per your
        confirmed "encrypt-on-rotate is fine") -- here, logging's handlers
        are closed first (releasing the file lock), then the file's bytes
        are read, encrypted, and written back in place.

    ORDER MATTERS, and this cost real debugging time to pin down: the log
    MUST be encrypted LAST. save_db_encrypted() (and init_db(), if this
    were ever called from elsewhere) calls logger.info()/logger.warning()
    internally -- and Python's logging module LAZILY REOPENS a closed
    file handler the next time anything logs, even after
    logging.shutdown(). Encrypting the log first, then running
    save_db_encrypted() (which logs), meant its log calls silently
    reopened the just-encrypted file and appended fresh plaintext directly
    onto the end of the ciphertext -- producing a file that was neither
    valid encrypted data nor valid plaintext, and would fail to decrypt
    (InvalidToken) on the very next session. This only showed up from the
    session AFTER the one that introduced a fresh save_db_encrypted() log
    call (e.g. "Decrypted and loaded..." only fires when there WAS a
    previous db to load), which is why it looked intermittent across
    several rounds of testing before the actual mechanism was found.

    Registered with atexit rather than only in the except KeyboardInterrupt
    block, so it also covers a normal fall-through exit and the sys.exit()
    calls in the login gate -- atexit handlers run in all of these cases."""
    save_db_encrypted()  # may log internally -- must finish, and any log
                          # calls it makes must land, BEFORE the log itself
                          # is read and encrypted below
    try:
        logging.shutdown()  # closes the file handle logging holds open
                             # (including any handler save_db_encrypted()'s
                             # own logging calls may have lazily reopened),
                             # so the file below isn't locked when we try
                             # to overwrite it, and nothing can silently
                             # reopen it after this point in this function
        if LOG_PATH.exists() and _fernet is not None:
            plaintext = LOG_PATH.read_bytes()
            if plaintext:
                encrypted = encrypt_bytes(plaintext)
                if encrypted is not None:
                    LOG_PATH.write_bytes(encrypted)
    except Exception:
        pass  # logging is already shut down at this point, so there's
              # nowhere left to report a failure here to

atexit.register(save_encrypted_data_on_exit)

def ping_ollama_quick(timeout=2):
    """Fast check only -- just a localhost ping, no drive walk. Returns
    True if Ollama is currently responding. Used by process_chat_query()
    to re-check right before sending a message (see that function), since
    Ollama may have been started AFTER this terminal launched -- the
    startup-time check in auto_bootstrap_ollama() only runs once and has
    no way to notice that on its own."""
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags", headers={"User-Agent": "HANNIBAL"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False

def auto_bootstrap_ollama():
    """Checks if Ollama is already running; if not, walks the drives for
    ollama.exe and launches it. Updates llm_status_string throughout so the
    [P] AI view can show live bootstrap progress instead of only a one-time
    startup print. Runs on its own daemon thread (see main()) since a full
    drive walk can take real time on a large C: drive -- it shouldn't block
    the terminal from coming up and starting its other daemons.

    This only runs ONCE, at startup -- if Ollama is started AFTER this
    check happens, this function has no way to notice on its own for the
    rest of the session. See ping_ollama_quick(), used by
    process_chat_query() to re-check right before every chat message
    specifically to cover that case."""
    global llm_status_string
    print(f"{CYAN}[*] Checking local Ollama API endpoint ({OLLAMA_ENDPOINT})...{RESET}")
    if ping_ollama_quick():
        print(f"{GREEN}[+] Ollama background service is active and responding.{RESET}")
        llm_status_string = "Llama 3 Active (Localhost:11434)"
        return True
    print(f"{YELLOW}[!] Ollama service not responding on localhost. Initiating system-wide scan...{RESET}")

    llm_status_string = "Searching drives for ollama.exe..."
    ollama_bin = recursive_find_ollama()

    if ollama_bin:
        print(f"{GREEN}[+] Located ollama.exe at: {ollama_bin}. Booting server subprocess...{RESET}")
        llm_status_string = f"Booting Ollama from {ollama_bin}..."
        try:
            subprocess.Popen([ollama_bin, "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(15):
                time.sleep(2)
                try:
                    req = urllib.request.Request("http://localhost:11434/api/tags", headers={"User-Agent": "HANNIBAL"})
                    with urllib.request.urlopen(req, timeout=2) as resp:
                        if resp.status == 200:
                            print(f"{GREEN}[+] Ollama server successfully booted and online!{RESET}")
                            llm_status_string = "Llama 3 Active (Background Daemon)"
                            return True
                except Exception:
                    continue
        except Exception as e:
            print(f"{RED}[!] Failed to auto-launch Ollama process: {e}{RESET}")
            llm_status_string = f"Ollama error: {e}"
    else:
        print(f"{RED}[!] Could not automatically locate ollama.exe. Please verify installation path.{RESET}")
        llm_status_string = "Ollama binary not found. Running heuristic fallback."
    return False

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
# Map cursor state [from hbl2.py] -- added alongside zoom_level, not instead
# of it, so [Z]/[X] zoom and [WASD] cursor inspection both work together on
# whichever MAP_LEVELS grid is currently active.
map_cursor_row, map_cursor_col = 5, 40
selected_node_info = "Nominal airspace. No active threat node under crosshair."

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

# ---------------------------------------------------------
# EXPANDED REGIONAL/TOPIC COVERAGE  [from napalmai2_0.py]
# ---------------------------------------------------------
# Added alongside REGIONAL_TOPIC_FEEDS above (not a replacement) for two
# reasons: (1) broader coverage -- 5 regions instead of 2, 7 topics instead
# of 5, specific multi-phrase queries chunked in groups of 5 rather than one
# broad OR-string per topic, via Google News RSS as a third search backend
# alongside Bing/Yahoo; (2) it's the only thing feeding the "atomic_nuclear"
# and "tech_cyber" categories -- sync_osint_feeds already has a branch for
# atomic_nuclear (grouped with infrastructure) but nothing was ever tagging
# headlines into it before this.
GOOGLE_REGIONS = {
    "US": {"gl": "US", "ceid": "US:en", "hl": "en-US"}, "UK": {"gl": "GB", "ceid": "GB:en", "hl": "en-GB"},
    "CA": {"gl": "CA", "ceid": "CA:en", "hl": "en-CA"}, "EU": {"gl": "IE", "ceid": "IE:en", "hl": "en-IE"},
    "IN": {"gl": "IN", "ceid": "IN:en", "hl": "en-IN"}
}
RAW_TOPICS = {
    "kinetic": ['missile strike', 'artillery barrage', 'warship deployed', 'nato readiness', 'drone swarm', 'naval blockade', 'border skirmish', 'military mobilization', 'hypersonic test', 'paramilitary strike'],
    "atomic_nuclear": ['IAEA inspection', 'uranium enrichment', 'nuclear reactor scram', 'Euratom warning', 'atomic energy agency', 'radiological alert', 'spent fuel storage', 'centrifuge cascade'],
    "infrastructure": ['grid collapse', 'rolling blackout', 'power grid substation', 'pipeline explosion', 'refinery fire', 'subsea cable cut', 'fiber optic severed', 'hydroelectric dam breach', 'LNG terminal shutdown'],
    "labor": ['union strike', 'mass walkout', 'wildcat strike', 'port workers strike', 'railway labor stoppage', 'refinery strike', 'miners walkout', 'air traffic controller strike', 'general strike called'],
    "supply_chain": ['port congestion crisis', 'shipping lane blocked', 'Suez transit halted', 'Strait of Hormuz tanker', 'freight container shortage', 'railway freight derailment', 'cargo ship attacked'],
    "macro": ['interest rate decision', 'inflation surprise', 'central bank liquidity', 'yield curve inversion', 'quantitative tightening', 'sovereign bond auction fail', 'debt ceiling default', 'recession indicator trigger'],
    "tech_cyber": ['critical infrastructure ransomware', 'zero-day scada exploit', 'state-sponsored grid attack', 'telecom satellite down', 'cloud region disaster', 'semiconductor foundry halt', 'undersea cable sabotage']
}
GOOGLE_NEWS_FEEDS = []
for region, params in GOOGLE_REGIONS.items():
    for topic, queries in RAW_TOPICS.items():
        for i in range(0, len(queries), 5):
            chunk = queries[i:i + 5]
            q_string = " OR ".join([f'"{x}"' for x in chunk])
            encoded = urllib.parse.quote(q_string)
            GOOGLE_NEWS_FEEDS.append({"type": "rss", "category": topic, "region": region, "url": f"https://news.google.com/rss/search?q={encoded}&hl={params['hl']}&gl={params['gl']}&ceid={params['ceid']}"})

FEED_REGISTRY = [{"type": "api", "category": name, "url": url} for name, url in API_ENDPOINTS.items()] + DIRECT_WIRES + REGIONAL_TOPIC_FEEDS + GOOGLE_NEWS_FEEDS

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
    # Regional pages [from HBL5.py] — Volcano didn't have these, HBL5 did.
    "R": {"name": "RUSSIAN FEDERATION", "tickers": ["BZ=F", "GC=F", "PL=F", "OGZPY"]},
    "A": {"name": "GREATER CHINA", "tickers": ["FXI", "KWEB", "BABA", "TCEHY", "TSM"]},
    "J": {"name": "JAPAN & NORTH ASIA", "tickers": ["EWJ", "DXJ", "JPY=X", "TM"]},
    "E": {"name": "MIDDLE EAST ENERGY", "tickers": ["BZ=F", "CL=F", "NG=F", "XOM", "CVX"]},
    "N": {"name": "INDIA & SOUTH ASIA", "tickers": ["INDA", "EPI", "INR=X", "INFY"]},
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
    # Regional pages [from Napatch.py / nmnm.py] -- these tabs existed already
    # (added from HBL5.py earlier) but had NO keyword tagging at all, so a
    # headline about Russia, China, Japan, the Middle East, or India could
    # never be routed to its page via tag_sectors()/distill_per_sector().
    "R": ["russia", "kremlin", "moscow", "gazprom", "rosatom", "black sea"],
    "A": ["china", "beijing", "taiwan", "south china sea", "rare earth"],
    "J": ["japan", "tokyo", "boj", "nikkei", "toyota"],
    "E": ["iran", "israel", "saudi", "uae", "qatar", "hormuz", "opec"],
    "N": ["india", "new delhi", "mumbai", "sensex", "kashmir"],
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

napalm_active_synthesis = "HANNIBAL Engine initializing. Awaiting local LLM generation..."
napalm_advisories = []
napalm_chat_history = []  # [from HBL5.py]

global_E_t, global_T_t, global_K, global_L, global_I_score, global_C_t, global_composite = 0.0, 0.0, 0, 0, 0, 1.0, 0.0
global_labor_fric, global_vol_int = 0.0, 0.0  # [from addendum.py]

all_possible_tickers = sorted(set(t for m in MARKETS.values() for t in m["tickers"]))
price_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
volume_buffers = {t: deque(maxlen=WINDOW_SIZE) for t in all_possible_tickers}
last_known_prices = {t: None for t in all_possible_tickers}
last_batch_sync_time = 0.0
last_fast_sync_time = 0.0

def clear_screen():
    # \x1b[H moves the cursor to the top-left home position.
    # \x1b[2J clears the ENTIRE visible screen (not just cursor-to-end, which
    # is what the previous \033[J version did -- that could leave leftover
    # content from a prior tab/page if the cursor wasn't already at the top).
    # \x1b[3J additionally clears the scrollback buffer, so nothing from a
    # previous page's render can bleed through into a new tab via scrollback.
    # No subprocess spawn (unlike the old os.system('cls')/os.system('clear')
    # version) -- this writes directly to the existing stdout stream, which
    # matters here since this runs on every single render tick.
    sys.stdout.write("\x1b[H\x1b[2J\x1b[3J")
    sys.stdout.flush()

def generate_sparkline(data_list, width=SPARK_WIDTH):
    if len(data_list) < 2: return " " * width
    subset = list(data_list)[-width:]
    min_val, max_val = min(subset), max(subset)
    if max_val == min_val: return "-" * len(subset)
    chars = "  ▂▃▄▅▆▇█"
    span = max_val - min_val
    return "".join(chars[int(((x - min_val) / span) * (len(chars) - 1))] for x in subset).rjust(width)

ALERT_SOUND_COOLDOWN_SECONDS = 180.0  # 3 min -- long enough not to spam while
                                       # the composite stays elevated, short
                                       # enough that a genuinely new event
                                       # still gets a fresh alert reasonably soon
_alert_sound_lock = threading.Lock()
_last_alert_sound_time = {}  # keyed per level ("critical"/"warning")

def trigger_alert_sound(level="warning"):
    """[from NPM.py] Windows beep alert, gated by ALERT_SOUND_COOLDOWN_SECONDS
    per level. NPM.py's original had no cooldown: its score>0.8 check runs
    inside compute_composite_index, which fires on every render tick (several
    times a second), so a sustained high-friction period would beep
    continuously rather than once. This tracks the last-fired time per level
    and skips firing again until the cooldown elapses, without changing the
    trigger CONDITIONS themselves (still a whale trade, or score > 0.8)."""
    if not IS_WINDOWS:
        return
    now = time.monotonic()
    with _alert_sound_lock:
        last = _last_alert_sound_time.get(level, 0.0)
        if now - last < ALERT_SOUND_COOLDOWN_SECONDS:
            return
        _last_alert_sound_time[level] = now
    try:
        if level == "critical":
            winsound.Beep(2500, 400)
            winsound.Beep(2000, 400)
        else:
            winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
    except Exception:
        pass

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
    """Wrapper around urllib with SEC-aware headers AND SEC-aware rate limiting.
    Volcano set the sec.gov User-Agent but never throttled request rate; the
    limiter here (from HBL5.py) is what actually keeps this off SEC's block
    list under the 100-worker concurrency Volcano's batch sync uses.
    """
    if "sec.gov" in url:
        sec_limiter.wait()
        headers = {"User-Agent": "ProjectHannibal bram.noffels@example.com", "Host": "www.sec.gov"}
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
def render_ascii_map(level, war_snap, infra_snap, weather_snap, cursor_row=None, cursor_col=None):
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

    # Cursor inspection [from hbl2.py]. Optional: passing no cursor position
    # (the default) renders exactly as before, so this doesn't change
    # behavior for any caller that hasn't been updated to pass one.
    cursor_line = ""
    if cursor_row is not None and cursor_col is not None:
        hovered_label = None
        for label, info in detected_nodes.items():
            if info["row"] == cursor_row and info["col"] == cursor_col:
                hovered_label = f"[{label}] {info['text']}"
                break
        node_info = hovered_label or "Nominal airspace. No active threat node under crosshair."
        if 0 <= cursor_row < len(grid) and 0 <= cursor_col < len(grid[0]):
            grid[cursor_row][cursor_col] = f"{RED}╬{RESET}"
        cursor_line = (f"\n{PURPLE}  --- INTERACTIVE MAP CONTROLS: [W/A/S/D] Move Crosshair ---{RESET}\n"
                        f"  {CYAN}CURSOR POS: [{cursor_row:02d}, {cursor_col:02d}] | NODE INSPECT: {node_info}{RESET}\n")

    rendered_map = "\n".join(["".join(row) for row in grid])
    sitrep = f"\n{PURPLE}  --- DYNAMIC TACTICAL SITREP & ACTIVE TELEMETRY NODES ---{RESET}\n"
    if not detected_nodes:
        sitrep += f"  {GRAY}Scanning global telemetry streams for geographic coordinates...{RESET}\n"
    else:
        for label, info in list(detected_nodes.items())[:8]:
            sitrep += f"  {RED}[ACTIVE NODE: {label}]{RESET} -> {info['text'][:75]}\n"
    return rendered_map + cursor_line + sitrep

# ---------------------------------------------------------
# OLLAMA AI: FULL NEURAL OVERRIDE (HANNIBAL SYNTHESIS)
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

    prompt = f"""You are PROJECT HANNIBAL, a geopolitical/macro risk-signal AI.
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
        napalm_active_synthesis = f"HANNIBAL Engine offline or processing (Localhost:11434). Detail: {e}"

# ---------------------------------------------------------
# LIVE AI CHAT QUERY  [from HBL5.py]
# ---------------------------------------------------------
def process_chat_query(user_query, composite, C_t, war_snap, infra_snap, ins_snap):
    """Runs in its own thread so it never blocks the render loop. Appends
    both the question and the reply to napalm_chat_history under state_lock.

    Re-checks Ollama reachability (ping_ollama_quick()) right before
    sending, rather than trusting the one-time startup check in
    auto_bootstrap_ollama() -- that check has no way to notice Ollama
    being started later in the session, which is exactly what made chat
    look intermittently broken. This adds a couple of seconds at most
    (ping_ollama_quick's timeout) before a message that would have failed
    anyway; a message that would have succeeded barely notices the extra
    check."""
    global napalm_chat_history
    context_str = f"SYSTEM STATE -> Composite: {composite:.2f}, C_t: {C_t:.2f}. "
    if war_snap: context_str += f"Latest Threat: {war_snap[0][1]['text']}. "
    if infra_snap: context_str += f"Latest Grid Alert: {infra_snap[0][1]['text']}. "
    if ins_snap:
        tx = ins_snap[0][1]
        context_str += f"Latest SEC Whale Trade: {tx.get('owner', 'Unknown')} traded ${tx.get('value', 0):,.0f} of {tx.get('issuer', 'Unknown')}. "

    with state_lock:
        napalm_chat_history.append({"role": "user", "content": user_query})

    if not ping_ollama_quick():
        with state_lock:
            napalm_chat_history.append({"role": "hannibal", "content": "Ollama isn't reachable right now (localhost:11434). If you just started it, give it a moment and try again."})
        return

    prompt = f"""You are PROJECT HANNIBAL, the core tactical AI for this terminal.
Current Live Context: {context_str}
Respond directly to the commander's query below. Be concise and analytical. Do not use markdown. Do not apologize.

Commander: {user_query}"""

    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}

    try:
        req = urllib.request.Request(OLLAMA_ENDPOINT, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=OLLAMA_CHAT_TIMEOUT_SECONDS) as response:
            result = json.loads(response.read().decode('utf-8'))
            reply = result.get("response", "").strip()
            with state_lock:
                napalm_chat_history.append({"role": "hannibal", "content": reply})
    except Exception as e:
        logger.warning(f"Ollama chat query failed: {e}")
        with state_lock:
            napalm_chat_history.append({"role": "hannibal", "content": f"Ollama was reachable but the request itself failed ({e}). Check the model name ({OLLAMA_MODEL}) is actually pulled."})

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

def price_poll_daemon():
    """Runs the yfinance price fetch on its own thread, on the same backoff
    schedule as before. Previously this call sat directly in the render loop,
    so a slow network response stalled the ENTIRE screen redraw (not just the
    price rows) for however long yfinance took. Moving it here removes that
    stall from the render path; the fetch interval, backoff growth/reset, and
    what gets fetched are all unchanged -- only where the call runs moves.

    Speed-mode note: price_backoff is stateful (grows on repeated failures,
    resets to the base on success) -- that growth logic is independent of
    speed mode and stays as-is. What DOES need to track the live speed mode
    is what it resets TO and what it's capped AT, so both are re-read from
    get_speed_value() every time they're used rather than fixed at function
    start -- otherwise a mode switch would never reach this thread, since it
    only runs through this setup once."""
    global last_known_prices
    price_backoff = get_speed_value("price_poll_base")
    last_price_fetch = 0.0
    while True:
        now_mono = time.monotonic()
        if now_mono - last_price_fetch >= price_backoff:
            fresh_prices, _ = fetch_prices_and_volume(all_possible_tickers)
            last_price_fetch = now_mono
            if fresh_prices:
                with state_lock:
                    for ticker, price in fresh_prices.items():
                        last_known_prices[ticker] = price
                        price_buffers[ticker].append(price)
                price_backoff = get_speed_value("price_poll_base")
            else:
                price_backoff = min(price_backoff * 1.5, get_speed_value("price_poll_max_backoff"))
        time.sleep(0.2)

def fast_poll_daemon():
    while True:
        fetch_fast_telemetry()
        time.sleep(get_speed_value("fast_poll_interval"))

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
    # Set to 69 per explicit request. Note this only partially addresses the
    # original throttling concern: FEED_REGISTRY now has 70+ entries, so 69
    # workers still fires nearly the entire registry in one concurrent wave
    # against the same set of public hosts. If throttling/blocking from any
    # of the news sites actually shows up in practice, the real fix would be
    # batching requests per-host rather than lowering this single number.
    with concurrent.futures.ThreadPoolExecutor(max_workers=69) as executor:
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
                        log_to_db("KINETIC", region_tag, title, nums_str, "ESCALATION")
                    elif cat in ["infrastructure", "atomic_nuclear"]:
                        raw_infra.append((ts, {"text": f"[{region_tag}] {title[:60]}", "nums": nums_str, "prediction": f"{RED}▼ CRITICAL{RESET}"}))
                        log_to_db("INFRA", region_tag, title, nums_str, "CRITICAL")
                    elif cat in ["labor"]:
                        raw_labor.append((ts, {"text": f"[{region_tag}] {title[:60]}", "nums": nums_str, "prediction": f"{PURPLE}♦ DISRUPTION{RESET}"}))
                        log_to_db("LABOR", region_tag, title, nums_str, "DISRUPTION")
                        # Mirrors to labor_event_log.csv, encrypted. Note:
                        # this file is meant to also be read by addendum.py's
                        # standalone labor_intensity() scorer (see that
                        # script) -- since it's now encrypted with this
                        # terminal's login password, that separate script
                        # would need matching decryption logic to read it;
                        # as a plain, unmodified script it cannot currently
                        # do so. Flagging this rather than silently letting
                        # it silently fail to parse.
                        try:
                            import io as _io
                            existing = read_encrypted_text(LABOR_EVENT_LOG_PATH) or ""
                            buf = _io.StringIO()
                            buf.write(existing)
                            if not existing:
                                buf.write("timestamp,headline\r\n")
                            w = csv.writer(buf)
                            w.writerow([datetime.datetime.now().isoformat(), f"[{region_tag}] {title}"])
                            write_encrypted_text(LABOR_EVENT_LOG_PATH, buf.getvalue())
                        except Exception: pass
                    else:
                        raw_osint.append((ts, {"text": f"[{region_tag}] {title[:75]}", "nums": nums_str, "prediction": f"{CYAN}► MACRO{RESET}"}))
                        log_to_db("MACRO", region_tag, title, nums_str, "MACRO")

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
                        trigger_alert_sound("critical")
                        log_to_db("SEC_WHALE", tx["issuer"], f"{tx['owner']} {tx['code_label']} {tx['issuer']}", f"${tx['value']:,.0f}", tx["prediction"])
                        log_insider_to_db(tx)
                        try:
                            import io as _io
                            existing = read_encrypted_text(LARGE_INSIDER_TRADE_LOG_PATH) or ""
                            buf = _io.StringIO()
                            buf.write(existing)
                            if not existing:
                                buf.write("timestamp,issuer,owner,role,code,shares,price,value\r\n")
                            w = csv.writer(buf)
                            w.writerow([datetime.datetime.now().isoformat(), tx["issuer"], tx["owner"], tx["role"], tx["code_label"], tx["shares"], tx["price"], tx["value"]])
                            write_encrypted_text(LARGE_INSIDER_TRADE_LOG_PATH, buf.getvalue())
                        except Exception: pass
        with state_lock: large_insider_trade_flag = 1 if new_large_trade else 0

def osint_sync_daemon():
    while True:
        sync_osint_feeds()
        time.sleep(get_speed_value("osint_batch_interval"))

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

# ---------------------------------------------------------
# LABOR & UNUSUAL-VOLATILITY CSV SCORING  [from addendum.py]
# ---------------------------------------------------------
def labor_intensity():
    """Simple labor friction score (0-1) from labor_event_log.csv, mirroring
    addendum.py's scorer. sync_osint_feeds() above keeps this CSV populated
    live so this doesn't depend on a separately-run script. File is
    encrypted at rest; read_encrypted_text() handles decryption and
    returns None on a missing/undecryptable file (same effective behavior
    as the old "file doesn't exist" case below)."""
    text = read_encrypted_text(LABOR_EVENT_LOG_PATH)
    if text is None:
        return 0.0
    try:
        import io as _io
        rows = list(csv.reader(_io.StringIO(text)))[1:]
    except Exception:
        return 0.0
    if not rows: return 0.0
    recent = rows[-50:]
    score = 0
    for row in recent:
        if len(row) < 2: continue
        h = row[1].lower()
        if "strike" in h or "work stoppage" in h or "walkout" in h:
            score += 2
        elif "raid" in h or "deportation" in h or "ice" in h:
            score += 1
    return min(score / 50.0, 1.0)

def volatility_intensity():
    """Volatility score (0-1) from unusual_activity_log.csv, mirroring
    addendum.py's scorer. Tolerant of short/malformed rows since this file
    may be written by other tools in the pipeline.

    Unlike labor_event_log.csv (which this terminal writes AND reads, so
    both sides agree on encryption), this file is only ever READ here --
    it comes from an external tool this program doesn't control. That
    external tool has no reason to know about this terminal's encryption
    scheme, so this function tries decrypting first (in case something
    ever does encrypt it the same way) and falls back to plain-text if
    that fails, rather than assuming one or the other and silently
    breaking a working plaintext pipeline."""
    if not UNUSUAL_ACTIVITY_LOG_PATH.exists():
        return 0.0
    text = read_encrypted_text(UNUSUAL_ACTIVITY_LOG_PATH)
    if text is None:
        try:
            text = UNUSUAL_ACTIVITY_LOG_PATH.read_text(encoding="utf-8")
        except Exception:
            return 0.0
    try:
        import io as _io
        rows = list(csv.reader(_io.StringIO(text)))[1:]
    except Exception:
        return 0.0
    if not rows: return 0.0
    recent = rows[-20:]
    score = 0
    for row in recent:
        if len(row) < 9: continue
        z, sev = row[5], row[8]
        try: z = float(z)
        except Exception: z = 0.0
        if z > 3.0 or "UNUSUAL" in sev:
            score += 1
    return min(score / 20.0, 1.0)

def compute_composite_index(K, I_score, L, weather_count, flight_count, prediction_momentum, C_t, labor_fric=0.0, vol_int=0.0):
    """Volcano's original weighted-average composite, PLUS addendum.py's two
    friction terms (labor_term / vol_term) added on top before the C_t
    multiplier — same additive structure addendum.py used, so a strike wave
    or a volatility spike nudges the index without needing to fit new
    weights into the existing 1.0-sum weighted average above."""
    w = COMPOSITE_WEIGHTS
    base = (w["kinetic"] * K + w["infrastructure"] * I_score + w["labor"] * L +
            w["flights"] * min(flight_count / 10.0, 1.0) + w["weather"] * min(weather_count / 20.0, 1.0) +
            w["prediction_markets"] * prediction_momentum)
    labor_term = 0.05 * labor_fric
    vol_term = 0.05 * vol_int
    score = min((base + labor_term + vol_term) * C_t, 1.0)
    # [from NPM.py] Warning alert on elevated friction, cooldown-gated (see
    # trigger_alert_sound) since this function runs on every render tick.
    if score > 0.8:
        trigger_alert_sound("warning")
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

def calculate_sector_prediction(view_key, E_t, T_t, C_t, composite, K=0):
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
        reason = "Chokepoint vulnerabilities elevating risk profile."
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
    elif view_key == "R":
        # [adapted from nmnm.py] Russia: kinetic flag + chokepoint index are
        # the primary drivers, not E_t/T_t -- sanctions/conflict exposure
        # rather than routine energy-market movement.
        score = (K * 0.40) + (C_t * 0.35)
        reason = "Active kinetic exposure elevating sanctions/conflict risk." if K else "Chokepoint-driven baseline risk, no active kinetic flag."
    elif view_key == "A":
        # [adapted from nmnm.py] China: chokepoint index + overall composite
        # friction -- Taiwan Strait / South China Sea tension shows up
        # through C_t, broader friction through composite.
        score = (C_t * 0.20) + (composite * 0.40)
        reason = f"Composite friction + chokepoint exposure (C_t {C_t:.2f})."
    elif view_key == "J":
        # [adapted from nmnm.py] Japan: treated as a relative safe-haven
        # within the region -- inverse of overall composite friction.
        score = -composite * 0.15
        reason = "Regional safe-haven positioning inverse to composite friction."
    elif view_key == "E":
        # [adapted from nmnm.py] Middle East: energy proxy return is the
        # dominant driver (Hormuz/OPEC exposure), plus chokepoint index.
        score = (E_t * 65) + (C_t * 0.25)
        reason = f"Energy proxy + chokepoint exposure (E_t {E_t:+.4f})."
    elif view_key == "N":
        # [adapted from nmnm.py] India: transport proxy return (shipping/
        # logistics through the region) plus a smaller composite term.
        score = (T_t * 35) + (composite * 0.10)
        reason = f"Transport proxy + composite friction (T_t {T_t:+.4f})."

    score = max(-1.0, min(score, 1.0))
    action = "STRONG BUY" if score > 0.3 else ("BUY" if score > 0.1 else ("STRONG SELL" if score < -0.3 else ("SELL" if score < -0.1 else "NEUTRAL")))
    color = GREEN if score > 0.1 else (RED if score < -0.1 else CYAN)
    return score, action, color, reason

def load_prediction_history():
    text = read_encrypted_text(PREDICTION_AUDIT_PATH)
    history = []
    if text:
        try:
            for line in text.splitlines():
                if line.strip(): history.append(json.loads(line.strip()))
        except Exception: pass
    return history[-1:]

def write_prediction(timestamp, action, asset, score, composite, reason):
    """CSV/JSONL audit trail — kept from addendum.py's write_prediction so
    every composite tick is on record, not just the [H] on-screen snapshot.
    Encrypted at rest: reads current content, appends the new line, and
    writes the whole file back encrypted (can't append directly to
    encrypted data -- each encrypt() call produces one self-contained
    token, not something concatenable)."""
    try:
        existing = read_encrypted_text(PREDICTION_AUDIT_PATH) or ""
        new_line = (
            f'{{"timestamp": "{timestamp}", "action": "{action}", "asset": "{asset}", '
            f'"score": "{score:.4f}", "composite": "{composite:.2f}", "reason": "{reason}"}}\n'
        )
        write_encrypted_text(PREDICTION_AUDIT_PATH, existing + new_line)
    except Exception: pass

# ---------------------------------------------------------
# MAIN UI LOOP
# ---------------------------------------------------------
def main():
    global current_view, last_batch_sync_time, zoom_level, graph_overlay_mode
    global map_cursor_row, map_cursor_col
    global global_E_t, global_T_t, global_K, global_L, global_I_score, global_C_t, global_composite
    global global_labor_fric, global_vol_int
    global db_conn

    run_login_gate()  # blocks here; exits the process on lockout, never
                       # returns unless access was actually granted

    # DIAGNOSTIC WRAPPER: everything from here through the start of the
    # main render loop used to run with NO exception handling at all --
    # any unhandled error killed the process instantly and silently (the
    # traceback goes to stderr, which a plain double-click launch doesn't
    # keep open to show anyone). This is exactly the "flickers with data
    # then black" symptom reported after login: real data decrypts and
    # briefly renders, then something in this startup sequence throws and
    # the window vanishes with no visible reason.
    #
    # A previous fix widened init_db()'s exception handling around
    # database loading specifically, but that did NOT resolve the reported
    # crash -- meaning the actual failure is happening somewhere else in
    # this sequence that hasn't been identified yet. Rather than guess at
    # more individual exception types one at a time, this wraps the whole
    # sequence: if anything here fails, the FULL error and traceback print
    # directly to the screen, and the window is held open with input()
    # instead of closing immediately -- so the actual cause can finally be
    # read and reported precisely, instead of guessed at again.
    try:
        # Must run before any further logging happens this session -- see
        # decrypt_log_for_session()'s docstring for the corruption bug this
        # prevents (appending plaintext onto last session's encrypted log).
        decrypt_log_for_session()
        flush_login_log_buffer()

        # Must happen AFTER login (needs the password-derived key to decrypt
        # any existing database from a previous session) and BEFORE any daemon
        # thread starts (osint_sync_daemon calls log_to_db/log_insider_to_db,
        # which need db_conn to already be a real connection, not None).
        db_conn = init_db()

        print(f"{PURPLE}========================================================================={RESET}")
        print(f"{CYAN} PROJECT HANNIBAL (Merged Build) | AUTONOMOUS OLLAMA BOOTSTRAPPER {RESET}")
        print(f"{CYAN} DYNAMIC MAPS | SQLITE INTEL LOG | LIVE CHAT | LABOR/VOL SCORING {RESET}")
        print(f"{PURPLE}========================================================================={RESET}")

        # Backgrounded: a full os.walk() drive scan (recursive_find_ollama, above)
        # can take real time on a large C: drive. Running it synchronously here
        # would block every other daemon from starting until it finished.
        threading.Thread(target=auto_bootstrap_ollama, name="Ollama-Bootstrap", daemon=True).start()
    except Exception:
        import traceback
        print(f"\n{RED}=========================================================================================={RESET}")
        print(f"{RED}  STARTUP CRASH -- this is the real error, please copy this exact text{RESET}")
        print(f"{RED}=========================================================================================={RESET}\n")
        traceback.print_exc()
        sys.stdout.flush()
        sys.stderr.flush()
        print(f"\n{RED}=========================================================================================={RESET}")
        try:
            input(f"{YELLOW}Press Enter to close...{RESET}")
        except (EOFError, KeyboardInterrupt):
            pass
        sys.exit(1)


    threading.Thread(target=osint_sync_daemon, name="Batch-Orchestrator", daemon=True).start()
    threading.Thread(target=fast_poll_daemon, name="Fast-Telemetry", daemon=True).start()
    threading.Thread(target=price_poll_daemon, name="Price-Poller", daemon=True).start()

    last_prediction_run = 0.0

    try:
        while True:
            timestamp = datetime.datetime.now(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S WEST")
            now_mono = time.monotonic()

            with view_lock: view_key = current_view
            active_tickers = MARKETS.get(view_key, MARKETS["1"])["tickers"] if view_key in MARKETS else ["BZ=F", "^GSPC", "BTC-USD"]
            view_name = MARKETS.get(view_key, MARKETS["1"])["name"] if view_key in MARKETS else f"SPECIALIZED PAGE: [{view_key}]"

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
                chat_snap = list(napalm_chat_history)  # [from HBL5.py]

            global_K = 1 if len(war_snap) > 0 else 0
            global_I_score = 1 if len(infra_snap) > 0 else 0
            global_L = 1 if len(l_snap) > 0 else 0
            global_E_t = _avg_last_return(ENERGY_PROXY_TICKERS)
            global_T_t = _avg_last_return(TRANSPORT_PROXY_TICKERS)
            global_labor_fric = labor_intensity()   # [from addendum.py]
            global_vol_int = volatility_intensity() # [from addendum.py]

            prediction_momentum, _ = compute_prediction_momentum()
            global_C_t = get_compounding_multiplier(war_snap, infra_snap, osint_snap)
            delta_hat = compute_delta_i_hat(global_E_t, global_T_t, global_K, insider_flag_snap)
            global_composite = compute_composite_index(
                global_K, global_I_score, global_L, len(w_snap), len(f_snap),
                prediction_momentum, global_C_t, global_labor_fric, global_vol_int
            )

            if now_mono - last_prediction_run >= 90.0:
                threading.Thread(target=run_napalm_engine, args=(global_composite, global_C_t, global_K, global_E_t, global_T_t, war_snap, infra_snap, [], ins_detail_snap, p_snap, osint_snap), daemon=True).start()
                ts_now = datetime.datetime.now().isoformat(timespec="seconds")
                write_prediction(
                    ts_now, "COMPOSITE_TICK", "Global Aggregate", global_composite * 100, global_composite,
                    f"E_t={global_E_t:.3f}, T_t={global_T_t:.3f}, K={global_K}, C_t={global_C_t:.2f}, "
                    f"Labor={global_labor_fric:.2f}, Vol={global_vol_int:.2f}"
                )
                last_prediction_run = now_mono

            # ---------------------------------------------------------
            # CRASH ISOLATION FOR THE PAGE RENDER
            # ---------------------------------------------------------
            # Everything from clear_screen() through the footer used to run
            # completely unguarded. A single bad value anywhere in it (a
            # market's price field coming back in an unexpected shape, a
            # malformed API response, anything this code didn't anticipate)
            # raised an exception that only KeyboardInterrupt (at the very
            # bottom of main()) would catch -- so it propagated, killed the
            # process, and left a blank terminal with no visible message
            # (the traceback goes to stderr, which a plain double-click
            # launch doesn't keep open). This is what "brief flash of text
            # then the window goes black" actually is.
            #
            # Wrapping the render in try/except means a bad value on one
            # page logs the real traceback to osint_terminal.log, shows a
            # visible on-screen message instead of vanishing, and lets the
            # loop keep running -- so the terminal survives a page render
            # failure instead of dying from it, and the log tells you
            # exactly what broke instead of nothing at all.
            try:
                clear_screen()
                print(f"{BLUE}========================================================================================={RESET}")
                print(f"{CYAN}  TACTICAL VIEW: {view_name:<30} | {timestamp} | ONLINE {RESET}")
                print(f"{BLUE}========================================================================================={RESET}")

                # ---------------------------------------------------------
                # MULTI-PAGE RENDER
                # ---------------------------------------------------------
                if view_key in MARKETS:
                    p_score, p_action, p_color, p_reason = calculate_sector_prediction(view_key, global_E_t, global_T_t, global_C_t, global_composite, global_K)
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
                    print(f" {GRAY}MODEL STATE:{RESET} E_t={global_E_t:+.4f}  T_t={global_T_t:+.4f}  K={k_str}  C_t={global_C_t:.2f}  Labor={global_labor_fric:.2f}  Vol={global_vol_int:.2f}  delta_I_hat={delta_hat:+.5f}")
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

                    # yes_price hardening: the [C] Consensus view (further
                    # below) already guarded this field with an is-not-None
                    # check; these two lines did not, despite reading the
                    # exact same field from the exact same data. Brought up
                    # to the same standard here rather than leaving three
                    # unguarded reads next to one guarded one.
                    poly_summary = " | ".join([f"{m['question'][:25]} - {int(m['yes_price']*100)}¢" for m in p_snap if m.get('platform') == 'Polymarket' and m.get('yes_price') is not None][:2])
                    kalshi_summary = " | ".join([f"{m['question'][:25]} - {int(m['yes_price']*100)}¢" for m in p_snap if m.get('platform') == 'Kalshi' and m.get('yes_price') is not None][:2])
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
                    print(f"  [Z] ZOOM IN | [X] ZOOM OUT | CURRENT ZOOM LEVEL: {zoom_level} | [W/A/S/D] MOVE CROSSHAIR")
                    print(f"{BLUE}========================================================================================={RESET}")
                    print(render_ascii_map(zoom_level, war_snap, infra_snap, w_snap, map_cursor_row, map_cursor_col))

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
                    print(f"{PURPLE}  PROJECT HANNIBAL: AUTONOMOUS NEURAL ADVISORY ENGINE{RESET}")
                    print(f"{BLUE}========================================================================================={RESET}")
                    print(f" {YELLOW}{PREDICTION_DISCLAIMER}{RESET}")
                    print(f" Neural Bridge Status          : {CYAN}{llm_status_string}{RESET}")
                    print(f" Composite Friction Score      : {global_composite:.2f} / 1.00")
                    print(f" Cascading Threat Index (C_t)  : {global_C_t:.2f}")

                    print(f"\n {YELLOW}--- HANNIBAL SYNTHESIS ---{RESET}")
                    print(f" {CYAN}{napalm_active_synthesis}{RESET}")

                    print(f"\n {YELLOW}--- LIVE AI SECTOR ADVISORIES ---{RESET}")
                    if not napalm_advisories:
                        print(f" {GRAY}Awaiting structured JSON response from HANNIBAL engine...{RESET}")
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
                    print(f"\n {GRAY}Large trades also logged to {LARGE_INSIDER_TRADE_LOG_PATH.name} and hannibal_intel.sqlite3{RESET}")

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
                            yp = f"{m['yes_price']*100:.1f}¢" if m.get('yes_price') is not None else "N/A"
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
                    print(f"{PURPLE}  AUDIT TRAIL: LATEST PROJECT HANNIBAL PREDICTION DOSSIER SNAPSHOT{RESET}")
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

                elif view_key == "T":
                    # LIVE CHAT VIEW  [from HBL5.py]
                    print(f"{PURPLE}  LIVE UPLINK: DIRECT QUERY TO PROJECT HANNIBAL AI{RESET}")
                    print(f"{BLUE}========================================================================================={RESET}")
                    if not chat_snap:
                        print(f" {GRAY}No queries yet this session. Press [T] again to ask a question.{RESET}")
                    else:
                        for msg in chat_snap[-10:]:
                            if msg["role"] == "user":
                                print(f" {CYAN}COMMANDER:{RESET} {msg['content']}")
                            else:
                                print(f" {PURPLE}HANNIBAL:{RESET} {msg['content']}\n")

                # ---------------------------------------------------------
                # FOOTER / ENGINE STATUS
                # ---------------------------------------------------------
                time_since_batch = time.time() - last_batch_sync_time
                next_batch_in = max(get_speed_value("osint_batch_interval") - time_since_batch, 0)

                print(f"\n{BLUE}========================================================================================={RESET}")
                print(f"{PURPLE}  HANNIBAL STATUS (Composite Index: {global_composite:.2f} | C_t: {global_C_t:.2f}){RESET}")
                print(f"{BLUE}========================================================================================={RESET}")
                print(f" {GRAY}Telemetry Pipeline:{RESET} Fast-Poll ({get_speed_value('fast_poll_interval'):.0f}s) + Batch Sync ({get_speed_value('osint_batch_interval'):.0f}s) -> Active | {GRAY}Speed Mode:{RESET} {SPEED_PROFILES[get_speed_mode()]['label']}")
                print(f" {GRAY}Next Ingestion Wave:{RESET} {next_batch_in:.0f}s | {GRAY}Feeds:{RESET} {len(FEED_REGISTRY)} | {GRAY}SQLite:{RESET} {DB_PATH.name}")

            except Exception as e:
                logger.error(f"Render crash on view [{view_key}]: {e}", exc_info=True)
                clear_screen()
                print(f"{RED}=========================================================================================={RESET}")
                print(f"{RED}  RENDER ERROR on view [{view_key}] -- terminal is still running, this page just failed.{RESET}")
                print(f"{RED}=========================================================================================={RESET}")
                print(f" {YELLOW}Error:{RESET} {e}")
                print(f" {GRAY}Full details logged to {LOG_PATH.name}. Switch pages with a key below, or press a number.{RESET}")

            print(f"\n{BLUE}========================================================================================={RESET}")
            print(f"{YELLOW} [1-0,R,A,J,E,N] Markets | [M] Map | [G] Graphs | [W] Weather/FAA | [K] War | [I] Infra/Atomic | [S] Insider $ | [C] Consensus | [P] HANNIBAL AI | [T] Chat | [Y] Speed | [H] Audit {RESET}")

            key_pressed = None
            # Same total window as the current speed profile's poll_interval;
            # checking for a keystroke more often WITHIN that window (every
            # 0.03s) is what makes input feel responsive, without changing
            # how often the screen actually redraws or touching any of the
            # background daemon intervals/locks. poll_interval itself now
            # comes from get_speed_value(), so switching speed mode ([Y])
            # changes this too, on the very next loop.
            poll_steps = int(max(1, get_speed_value("poll_interval") / 0.03))
            for _ in range(poll_steps):
                if IS_WINDOWS and _kbhit():
                    key = _getch()
                    # WASD cursor movement [from hbl2.py] -- checked FIRST and
                    # gated to current_view == 'M', because 'w' collides with
                    # the existing Weather view shortcut below. Outside the
                    # map view this branch never fires, so 'w' still means
                    # "switch to Weather" exactly as before.
                    if current_view == 'M' and key in ('w', 'a', 's', 'd'):
                        grid_h = len(MAP_LEVELS[zoom_level])
                        grid_w = len(MAP_LEVELS[zoom_level][0])
                        if key == 'w': map_cursor_row = max(0, map_cursor_row - 1)
                        elif key == 's': map_cursor_row = min(grid_h - 1, map_cursor_row + 1)
                        elif key == 'a': map_cursor_col = max(0, map_cursor_col - 1)
                        elif key == 'd': map_cursor_col = min(grid_w - 1, map_cursor_col + 1)
                        break
                    elif key in MARKETS or key in ['w', 'k', 'i', 'c', 'p', 'h', 'g', 'm', 's', 't', 'y']:
                        key_pressed = key.upper()
                        break
                    elif key == 'z':
                        zoom_level = min(2, zoom_level + 1)
                        map_cursor_row = min(map_cursor_row, len(MAP_LEVELS[zoom_level]) - 1)
                        map_cursor_col = min(map_cursor_col, len(MAP_LEVELS[zoom_level][0]) - 1)
                        break
                    elif key == 'x':
                        zoom_level = max(0, zoom_level - 1)
                        map_cursor_row = min(map_cursor_row, len(MAP_LEVELS[zoom_level]) - 1)
                        map_cursor_col = min(map_cursor_col, len(MAP_LEVELS[zoom_level][0]) - 1)
                        break
                    elif key == 'o' and current_view == 'G':
                        graph_overlay_mode = (graph_overlay_mode % 3) + 1
                        break
                time.sleep(0.03)

            if key_pressed == 'T':
                # Chat needs a real blocking input() for multi-character text —
                # it can't share the single-keystroke _kbhit()/_getch() poll
                # above, so it's handled here as its own step rather than
                # being crammed into that loop (which is what would have made
                # HBL5.py's version Windows-only in practice: msvcrt has no
                # line-input primitive, and non-Windows has neither).
                with view_lock: current_view = 'T'
                try:
                    print(f"\n{CYAN}Enter query for HANNIBAL (blank to cancel):{RESET} ", end="", flush=True)
                    user_input = input().strip()
                    if user_input:
                        threading.Thread(target=process_chat_query, args=(user_input, global_composite, global_C_t, war_snap, infra_snap, ins_detail_snap), daemon=True).start()
                except (EOFError, KeyboardInterrupt):
                    pass
            elif key_pressed == 'Y':
                # Speed menu -- same blocking-input pattern as [T] chat above,
                # for the same reason (line input needs a real input() call,
                # not the single-keystroke poll). set_speed_mode() is picked
                # up by every daemon thread on its next iteration (see the
                # SPEED_PROFILES block and each daemon's docstring) -- no
                # restart needed.
                try:
                    print(f"\n{PURPLE}--- SPEED MODE (current: {SPEED_PROFILES[get_speed_mode()]['label']}) ---{RESET}")
                    print(f" {CYAN}1{RESET} = Slow (low load)   {CYAN}2{RESET} = Normal (default)   {CYAN}3{RESET} = Fast (high load)")
                    choice = input(f"{YELLOW}Select 1-3 (blank to cancel): {RESET}").strip()
                    mode_map = {"1": "slow", "2": "normal", "3": "fast"}
                    if choice in mode_map:
                        set_speed_mode(mode_map[choice])
                        print(f"{GREEN}Speed set to {SPEED_PROFILES[mode_map[choice]]['label']}.{RESET}")
                        time.sleep(1.0)
                except (EOFError, KeyboardInterrupt):
                    pass
            elif key_pressed:
                with view_lock: current_view = key_pressed

    except KeyboardInterrupt:
        print(f"\n{CYAN}[+] PROJECT HANNIBAL terminal disengaged safely by operator.{RESET}")
        logger.info("Terminal stopped by operator.")

if __name__ == "__main__":
    main()
