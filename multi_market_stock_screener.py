# ============================================================
# STOCK SCREENER - VERSION 9.1 - MULTI-MARKET STABLE BUILD
# MULTI-MARKET: US + CANADA + INDIA
# ============================================================
#
# VERSION 9 IS THE FINAL STABLE BUILD OF THIS PROJECT.
#
# It keeps the working Version-8 architecture and finalizes India's
# Layer-2 catalyst logic.
#
# FINAL DATA-SOURCE DESIGN
# ------------------------
#
# US
#   Universe: Nasdaq Trader
#   Stage 1:  Yahoo/yfinance daily price-volume
#   Stage 2:  Yahoo company info + direct news/catalyst checks
#
# CANADA
#   Universe: official TMX TSX/TSXV MOC-eligible symbol table
#   Stage 1:  Yahoo/yfinance daily price-volume
#   Stage 2:  Yahoo company info + direct news/catalyst checks
#
# INDIA
#   Universe: official NSE + BSE equity lists
#   Stage 1:  OFFICIAL NSE/BSE daily UDiFF bhavcopy files
#   Stage 2:  OFFICIAL NSE/BSE corporate announcements FIRST
#             Yahoo/news only as a SECONDARY fallback
#
# INDIA NEWS FRESHNESS
# --------------------
#
# For each India Stage-1 survivor, Version 9:
#
#   1. Checks the official exchange announcement feed for the stock.
#   2. Matches announcements to the exact catalyst window:
#          previous session close -> target session close
#   3. Requires an identifiable material event.
#   4. Looks back 30 days for a similar earlier exchange filing.
#   5. Shows the official attachment link.
#   6. Labels the event:
#          FRESH OFFICIAL FILING
#          POSSIBLE REPEAT / FOLLOW-UP
#   7. Uses Yahoo only if no official exchange catalyst passes.
#
# This directly implements your requirement to know whether a story is
# genuinely fresh or whether a similar announcement appeared earlier,
# including a link to that earlier filing when available.
#
# FLOAT HANDLING
# --------------
#
# Float remains PASS / FAIL / UNVERIFIED.
#
# If Yahoo does not provide floatShares but total shares outstanding are
# <= 20M, Version 9 can safely confirm PASS because public float can never
# exceed total shares outstanding. The display marks this as an upper
# bound rather than pretending it is the exact float.
#
# VERSION 8 KEEPS THE MARKET SELECTOR AND CHANGES INDIA'S
# STAGE-1 DATA SOURCE.
#
# IMPORTANT VERSION-8 CHANGE:
#
#     US      -> yfinance batch price/volume engine
#     CANADA  -> yfinance batch price/volume engine
#     INDIA   -> OFFICIAL NSE + BSE END-OF-DAY BHAVCOPY DATA
#
# Why?
#
# Version 7 proved that Yahoo had the correct India dates when stocks
# were downloaded one-by-one, but its large India batch download was
# missing the target session for thousands of securities.
#
# For India, Version 8 therefore downloads official daily exchange
# files for the target session plus the previous 20 trading sessions.
#
# It calculates directly from exchange data:
#
#     Price
#     Day Change
#     Volume
#     20-day Average Volume
#     RVOL
#     Turnover
#
# Only the much smaller list of Stage-1 survivors goes to Yahoo for:
#
#     Float
#     News / catalyst
#
# This keeps the expensive / less reliable Yahoo requests out of the
# thousands-of-stock India quantitative scan.
#
# VERSION 7 KEEPS THE MARKET SELECTOR AND FIXES A MAJOR
# INDIA STAGE-1 PROBLEM FOUND IN VERSION 6.
#
# THE VERSION-6 PROBLEM:
#     India correctly loaded ~5,000 NSE/BSE symbols and correctly
#     identified the 2026-09-07 trading session, but Stage 1 returned
#     zero survivors.
#
# VERSION 7 FIXES THIS BY:
#
# 1) Making yfinance MultiIndex extraction robust whether Ticker is
#    stored on column level 0 or column level 1.
#
# 2) Looking for the TARGET SESSION by its actual date inside each
#    ticker history, rather than requiring the latest two DataFrame
#    rows to be exactly target-date / previous-date.
#
# 3) Calculating 20-day average volume from the 20 sessions BEFORE
#    the target session, even if Yahoo has an extra/stale row.
#
# 4) Printing Stage-1 diagnostics:
#       histories extracted
#       target-session rows found
#       previous-session rows found
#       stocks evaluated
#       stocks passing Day Change
#       stocks passing RVOL
#       stocks passing Price
#       stocks passing at least 2/3
#
# 5) If India still has zero survivors, automatically testing a few
#    liquid NSE tickers one-by-one so we can see the exact dates Yahoo
#    is returning.
#
# VERSION 6 ADDED A MARKET SELECTOR.
#
# When you run the program, it asks:
#
#     1 = US
#     2 = CANADA
#     3 = INDIA
#
# Then the SAME screener logic is applied to the selected market.
#
# ------------------------------------------------------------
# YOUR 5 MAIN STOCK-SELECTION RULES
# ------------------------------------------------------------
#
# 1) Strong daily price move
# 2) High Relative Volume (RVOL)
# 3) A real, identifiable recent catalyst/news event
# 4) Price inside the preferred range
# 5) Public float below 20 million shares
#
# ------------------------------------------------------------
# OUTPUT
# ------------------------------------------------------------
#
# QUALIFIED
#     Confirmed 5/5.
#
# NEAR MATCH
#     Confirmed 4/5 AND the one failed numerical rule is still
#     reasonably close to your target.
#
# REVIEW / UNVERIFIED
#     The stock could still reach at least 4/5, but one required
#     data point is unavailable.
#
# Everything weaker remains hidden.
#
# ------------------------------------------------------------
# OFFICIAL UNIVERSE SOURCES USED
# ------------------------------------------------------------
#
# US:
#     Nasdaq Trader symbol directory.
#
# CANADA:
#     TMX official TSX/TSXV Market-on-Close eligible-stock table.
#     This is a broad official TSX/TSXV stock universe, but it is
#     not guaranteed to contain every single listed security.
#
# INDIA:
#     NSE official Equity-segment security CSV
#     +
#     BSE official active-equity API.
#
# For India, BSE listings whose ISIN already appears on NSE are
# removed so the same company is not scanned twice. In practice,
# NSE becomes the preferred listing when the company trades on
# both exchanges.
#
# ------------------------------------------------------------
# PRICE / VOLUME / FLOAT / NEWS DATA
# ------------------------------------------------------------
#
# yfinance / Yahoo Finance is still used for the prototype data
# layer because it is convenient for learning.
#
# We can later replace this with professional real-time APIs
# without changing the main screening architecture.
# ============================================================


# ============================================================
# SECTION 1: IMPORT LIBRARIES
# ============================================================

import json
import logging
import re
import io
import zipfile
import time as pytime
import gc
import traceback
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone, timedelta, time
from difflib import SequenceMatcher
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf


# Reduce noisy yfinance log output inside IDLE.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# ============================================================
# SECTION 2: GLOBAL SCREENER RULES
# ============================================================

# Criterion 1: daily price change.
MIN_DAY_CHANGE = 10.0

# Criterion 2: relative volume.
MIN_RVOL = 5.0

# Criterion 3: news/catalyst.
REQUIRE_NEWS = True
REQUIRE_DIRECT_NEWS = True

# For now we DISPLAY whether news looks repeated/follow-up,
# but do not automatically reject repeats.
REQUIRE_FRESH_CATALYST = False
NEWS_SIMILARITY_THRESHOLD = 0.68

# Criterion 5: public float.
MAX_FLOAT = 20_000_000

# Near-match float tolerance.
NEAR_FLOAT_MAX = 30_000_000

# A News failure can still be shown as a 4/5 near match.
SHOW_NEAR_MATCH_IF_NEWS_FAILS = True

# Minimum score shown.
MINIMUM_SCORE_TO_SHOW = 4

# Download 100 tickers at a time.
BATCH_SIZE = 100

# We need at least 20 historical trading days.
PRICE_HISTORY_PERIOD = "3mo"

# If normal average volume is below this, a huge RVOL is flagged
# as being based on a very small historical denominator.
MIN_AVG_VOLUME_FOR_GOOD_RVOL = 25_000

# None = scan the whole market.
#
# For quick debugging you could temporarily set:
# TEST_SYMBOL_LIMIT = 500
TEST_SYMBOL_LIMIT = None

# ============================================================
# MULTI-MARKET STABILITY SETTINGS
# ============================================================
#
# These settings are deliberately separate from your stock-selection
# rules. They only make the program more reliable when switching from
# one market to another in the SAME Python session.
#
YF_DOWNLOAD_RETRIES = 3
YF_RETRY_BASE_SECONDS = 2.0
MARKET_SWITCH_PAUSE_SECONDS = 2.0
UNIVERSE_CACHE_MAX_AGE_DAYS = 30

# Minimum universe sizes used as a sanity check before a newly
# downloaded official universe is written to the local cache.
MIN_UNIVERSE_SIZE = {
    "US": 1000,
    "CANADA": 100,
    "INDIA": 1000,
}


# ============================================================
# SECTION 3: MARKET-SPECIFIC SETTINGS
# ============================================================
#
# Different markets use different currencies, ticker suffixes,
# market hours, and sensible nominal share-price ranges.
#
# We are NOT simply converting US dollars into rupees.
# ============================================================

MARKET_CONFIG = {

    "US": {
        "label": "United States",
        "currency": "$",
        "timezone": ZoneInfo("America/New_York"),

        # Regular cash-market close.
        "close_hour": 16,
        "close_minute": 0,

        # Criterion 4.
        "min_price": 2.0,
        "max_price": 20.0,

        # Near-match price range.
        "near_price_min": 1.0,
        "near_price_max": 25.0,

        # If Day Change misses +10%, it must still be >= +5%.
        "near_day_change_min": 5.0,

        # If RVOL misses 5x, it must still be >= 3x.
        "near_rvol_min": 3.0,

        # Quality warning only, NOT a scoring rule.
        "min_turnover_for_good_liquidity": 500_000,

        # Benchmarks used only to determine the latest trading date.
        "session_benchmarks": ["SPY", "QQQ", "DIA"],

        "output_csv": "us_screener_results_final.csv",
    },

    "CANADA": {
        "label": "Canada - TSX / TSXV",
        "currency": "C$",
        "timezone": ZoneInfo("America/Toronto"),

        "close_hour": 16,
        "close_minute": 0,

        # Same nominal $2-$20 idea, but in Canadian dollars.
        "min_price": 2.0,
        "max_price": 20.0,

        "near_price_min": 1.0,
        "near_price_max": 25.0,

        "near_day_change_min": 5.0,
        "near_rvol_min": 3.0,

        # Slightly lower because TSXV contains many smaller names.
        "min_turnover_for_good_liquidity": 250_000,

        "session_benchmarks": [
            "XIU.TO",
            "XIC.TO",
            "^GSPTSE",
        ],

        "output_csv": "canada_screener_results_final.csv",
    },

    "INDIA": {
        "label": "India - NSE + BSE",
        "currency": "₹",
        "timezone": ZoneInfo("Asia/Kolkata"),

        "close_hour": 15,
        "close_minute": 30,

        # We chose a separate nominal price range for India.
        "min_price": 50.0,
        "max_price": 2000.0,

        "near_price_min": 25.0,
        "near_price_max": 2500.0,

        "near_day_change_min": 5.0,
        "near_rvol_min": 3.0,

        # ₹50 lakh approximate daily turnover quality threshold.
        # This is ONLY a quality warning.
        "min_turnover_for_good_liquidity": 5_000_000,

        "session_benchmarks": [
            "^NSEI",
            "NIFTYBEES.NS",
            "RELIANCE.NS",
        ],

        "output_csv": "india_screener_results_final.csv",
    },
}


# ============================================================
# SECTION 4: OFFICIAL UNIVERSE URLs
# ============================================================

# -------------------- US / Nasdaq Trader --------------------

NASDAQ_LISTED_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
)

OTHER_LISTED_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
)


# -------------------- Canada / TMX --------------------

TMX_MOC_URL = (
    "https://www.tsx.com/en/trading/market-data-and-statistics/"
    "market-statistics-and-reports/tsx-tsxv-moc-eligible-stocks"
)

# TMX also publishes a monthly list of all TSX/TSXV issuers here.
# Version 9.1 tries this fuller official source first, then falls back
# to the MOC-eligible list if the downloadable issuer file cannot be
# discovered or fetched automatically.
TMX_CURRENT_MARKET_STATISTICS_URL = (
    "https://www.tsx.com/en/listings/current-market-statistics"
)


# -------------------- India / NSE --------------------

NSE_EQUITY_URL = (
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
)


# -------------------- India / BSE --------------------

BSE_SCRIP_API = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
)


# -------------------- India / Official Daily Bhavcopy --------------------
#
# NSE discontinued its old CM bhavcopy/common-bhavcopy format and uses
# the UDiFF Common Bhavcopy Final file.
#
# Date placeholder format:
#     YYYYMMDD
#

NSE_UDIFF_BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)

# BSE's official UDiFF equity bhavcopy endpoint.
BSE_UDIFF_BHAVCOPY_URL = (
    "https://www.bseindia.com/download/BhavCopy/Equity/"
    "BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV"
)


# -------------------- India / Official Corporate Announcements --------------------

# Official NSE page and JSON endpoint used by the exchange's
# Corporate Filings / Announcements screen.
NSE_ANNOUNCEMENT_PAGE = (
    "https://www.nseindia.com/companies-listing/"
    "corporate-filings-announcements"
)

NSE_ANNOUNCEMENT_API = (
    "https://www.nseindia.com/api/corporate-announcements"
)

# Current BSE corporate-announcement API.
BSE_ANNOUNCEMENT_API = (
    "https://api.bseindia.com/BseIndiaAPI/api/"
    "AnnSubCategoryGetData/w"
)

# Recent BSE attachments are exposed from this official location.
BSE_ATTACHMENT_BASE = (
    "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
)

# How far back to look for an older similar filing.
INDIA_OFFICIAL_NEWS_LOOKBACK_DAYS = 30

# Prevent runaway pagination if BSE changes its API unexpectedly.
MAX_BSE_ANNOUNCEMENT_PAGES = 20

# Small pause between retried/paginated exchange requests.
EXCHANGE_REQUEST_PAUSE_SECONDS = 0.10


# Number of completed historical sessions needed for 20-day RVOL.
INDIA_REQUIRED_SESSIONS = 21

# 45 calendar days normally gives us plenty of room to find
# 21 trading sessions even with weekends and market holidays.
INDIA_MAX_CALENDAR_LOOKBACK = 45



# ============================================================
# SECTION 5: CHOOSE THE MARKET
# ============================================================

def choose_market():
    """
    Persistent market menu.

    Version 9 had a one-shot menu: it asked once, ran one market,
    and then the program ended. Version 9.1 deliberately supports
    switching markets without restarting IDLE.

    Returns:
        "US"
        "CANADA"
        "INDIA"
        "ALL"
        "EXIT"
    """

    print("\n" + "=" * 72)
    print("CHOOSE A STOCK MARKET")
    print("=" * 72)

    print("\n1 = US")
    print("2 = CANADA")
    print("3 = INDIA")
    print("4 = RUN ALL 3 MARKETS")
    print("0 = EXIT")

    while True:

        choice = input(
            "\nEnter 0, 1, 2, 3, or 4: "
        ).strip().upper()

        if choice in {"1", "US", "USA"}:
            return "US"

        if choice in {
            "2",
            "CANADA",
            "CAD",
            "CA"
        }:
            return "CANADA"

        if choice in {
            "3",
            "INDIA",
            "IND",
            "IN"
        }:
            return "INDIA"

        if choice in {
            "4",
            "ALL",
            "ALL 3",
            "ALL3"
        }:
            return "ALL"

        if choice in {
            "0",
            "EXIT",
            "QUIT",
            "Q"
        }:
            return "EXIT"

        print(
            "Invalid choice. Please enter 0, 1, 2, 3, or 4."
        )


# ============================================================
# SECTION 6: GENERAL HELPER FUNCTIONS
# ============================================================

def request_text(url, headers=None, timeout=30):
    """
    Download text with retries.

    Try urllib first, then requests as a fallback. This improves
    reliability when official exchange sites behave differently across
    repeated market runs.
    """

    final_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/151.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    if headers:
        final_headers.update(headers)

    last_error = None

    for attempt in range(1, 4):

        try:
            request = Request(
                url,
                headers=final_headers
            )

            with urlopen(
                request,
                timeout=timeout
            ) as response:

                return response.read().decode(
                    "utf-8",
                    errors="replace"
                )

        except Exception as urllib_error:
            last_error = urllib_error

        # Some exchange/CDN endpoints behave better through requests.
        try:
            response = requests.get(
                url,
                headers=final_headers,
                timeout=timeout
            )

            response.raise_for_status()

            return response.text

        except Exception as requests_error:
            last_error = requests_error

        if attempt < 3:
            pytime.sleep(
                1.0 * attempt
            )

    raise RuntimeError(
        f"Could not download text from {url}: {last_error}"
    )

def request_json(
    url,
    params=None,
    headers=None,
    timeout=30
):
    """
    Download JSON from an API.
    """

    if params:

        url = (
            url
            + "?"
            + urlencode(params)
        )

    text = request_text(
        url,
        headers=headers,
        timeout=timeout
    )

    return json.loads(text)


# ============================================================
# MULTI-MARKET NETWORK / CACHE / STATE HELPERS
# ============================================================

def safe_yf_download(*args, **kwargs):
    """
    Retry yfinance downloads before giving up.

    This matters when US and Canada are scanned one after another,
    because Yahoo can intermittently return an empty frame or a
    transient network/rate-limit error even though the ticker is valid.
    """

    last_error = None

    for attempt in range(1, YF_DOWNLOAD_RETRIES + 1):

        try:
            data = yf.download(*args, **kwargs)

            if data is not None and not data.empty:
                return data

            last_error = RuntimeError(
                "Yahoo/yfinance returned an empty DataFrame"
            )

        except Exception as error:
            last_error = error

        if attempt < YF_DOWNLOAD_RETRIES:
            wait_seconds = (
                YF_RETRY_BASE_SECONDS
                * attempt
            )

            pytime.sleep(wait_seconds)

    raise RuntimeError(
        "yfinance download failed after "
        f"{YF_DOWNLOAD_RETRIES} attempts: {last_error}"
    )


def safe_get_ticker_info(stock):
    """Retry Yahoo company-info retrieval and return {} on failure."""

    for attempt in range(1, 3):

        try:
            info = stock.get_info()

            if isinstance(info, dict):
                return info

        except Exception:
            pass

        if attempt < 2:
            pytime.sleep(YF_RETRY_BASE_SECONDS)

    return {}


def safe_get_yahoo_news(stock):
    """Retry Yahoo news retrieval before marking it unavailable."""

    last_error = None

    for attempt in range(1, 3):

        try:
            return stock.news

        except Exception as error:
            last_error = error

        if attempt < 2:
            pytime.sleep(YF_RETRY_BASE_SECONDS)

    raise RuntimeError(
        f"Yahoo news fetch failed after retries: {last_error}"
    )


def get_cache_directory():
    """Directory used only for previously successful official universes."""

    directory = (
        Path(__file__)
        .resolve()
        .parent
        / ".stock_screener_cache"
    )

    directory.mkdir(
        parents=True,
        exist_ok=True
    )

    return directory


def universe_cache_path(market):
    return (
        get_cache_directory()
        / f"{market.lower()}_universe.txt"
    )


def save_universe_cache(market, symbols):
    """Save only a sane, successfully loaded official universe."""

    minimum = MIN_UNIVERSE_SIZE.get(
        market,
        1
    )

    if len(symbols) < minimum:
        return

    path = universe_cache_path(market)

    path.write_text(
        "\n".join(sorted(set(symbols))) + "\n",
        encoding="utf-8"
    )


def load_cached_universe(market):
    """Return (symbols, age_days) or (None, None)."""

    path = universe_cache_path(market)

    if not path.exists():
        return None, None

    try:
        symbols = [
            line.strip()
            for line in path.read_text(
                encoding="utf-8"
            ).splitlines()
            if line.strip()
        ]

        if len(symbols) < MIN_UNIVERSE_SIZE.get(market, 1):
            return None, None

        age_seconds = (
            datetime.now().timestamp()
            - path.stat().st_mtime
        )

        age_days = age_seconds / 86400

        return symbols, age_days

    except Exception:
        return None, None


def load_market_universe_resilient(market):
    """
    Prefer a fresh official universe. If the exchange website is
    temporarily unavailable, fall back to the last successful local
    official-universe cache instead of crashing the whole program.
    """

    try:
        symbols = load_market_universe(market)

        minimum = MIN_UNIVERSE_SIZE.get(
            market,
            1
        )

        if len(symbols) < minimum:
            raise RuntimeError(
                f"{market} universe unexpectedly small: "
                f"{len(symbols):,} symbols"
            )

        save_universe_cache(
            market,
            symbols
        )

        return symbols

    except Exception as official_error:

        cached, age_days = load_cached_universe(
            market
        )

        if cached is None:
            raise

        print(
            "\nWARNING: Fresh official universe could not be loaded."
        )

        print(
            f"Official-source error: {official_error}"
        )

        print(
            f"Using last successful {market} universe cache "
            f"({len(cached):,} symbols; "
            f"about {age_days:.1f} days old)."
        )

        if age_days > UNIVERSE_CACHE_MAX_AGE_DAYS:
            print(
                "WARNING: This cache is older than the preferred "
                f"{UNIVERSE_CACHE_MAX_AGE_DAYS}-day limit."
            )

        return cached


def reset_runtime_state():
    """
    Close market-specific HTTP sessions between market runs.

    This is the key protection against stale NSE/BSE cookies when
    India is run, followed by another market, and then India again.
    """

    global _NSE_ANNOUNCEMENT_SESSION
    global _BSE_ANNOUNCEMENT_SESSION

    for session in [
        _NSE_ANNOUNCEMENT_SESSION,
        _BSE_ANNOUNCEMENT_SESSION
    ]:

        if session is not None:
            try:
                session.close()
            except Exception:
                pass

    _NSE_ANNOUNCEMENT_SESSION = None
    _BSE_ANNOUNCEMENT_SESSION = None

    # yfinance currently uses shared dictionaries internally. Clear
    # them opportunistically if they exist, but never depend on this
    # private implementation detail.
    try:
        import yfinance.shared as yf_shared

        for attribute in [
            "_DFS",
            "_ERRORS",
            "_TRACEBACKS"
        ]:
            value = getattr(
                yf_shared,
                attribute,
                None
            )

            if isinstance(value, dict):
                value.clear()

    except Exception:
        pass

    gc.collect()


def write_error_log(market, stage, error):
    """Write the full traceback without flooding the IDLE screen."""

    try:
        log_path = (
            Path(__file__)
            .resolve()
            .parent
            / "stock_screener_error.log"
        )

        with log_path.open(
            "a",
            encoding="utf-8"
        ) as handle:

            handle.write(
                "\n"
                + "=" * 80
                + "\n"
            )

            handle.write(
                f"Time: {datetime.now().isoformat()}\n"
            )

            handle.write(
                f"Market: {market}\n"
            )

            handle.write(
                f"Stage: {stage}\n"
            )

            handle.write(
                f"Error: {error!r}\n"
            )

            handle.write(
                traceback.format_exc()
            )

    except Exception:
        pass


def format_large_number(number):
    """
    Examples:
        1,250,000,000 -> 1.25B
        45,000,000    -> 45.00M
        850,000       -> 850.00K
    """

    if number is None or pd.isna(number):
        return "N/A"

    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}B"

    if number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"

    if number >= 1_000:
        return f"{number / 1_000:.2f}K"

    return f"{number:,.0f}"


def format_money(number, market):
    """
    Format turnover in the selected market's currency.
    """

    if number is None or pd.isna(number):
        return "N/A"

    symbol = MARKET_CONFIG[
        market
    ]["currency"]

    if number >= 1_000_000_000:
        return (
            f"{symbol}"
            f"{number / 1_000_000_000:.2f}B"
        )

    if number >= 1_000_000:
        return (
            f"{symbol}"
            f"{number / 1_000_000:.2f}M"
        )

    if number >= 1_000:
        return (
            f"{symbol}"
            f"{number / 1_000:.2f}K"
        )

    return f"{symbol}{number:,.0f}"


def normalize_text(text):
    """
    Lowercase text and remove punctuation.
    """

    if not text:
        return ""

    text = str(text).lower()

    text = re.sub(
        r"[^a-z0-9\s]",
        " ",
        text
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# ============================================================
# SECTION 7: LOAD US STOCK UNIVERSE
# ============================================================

def is_us_common_stock_like(security_name):
    """
    Remove obvious US non-common-stock instruments.
    """

    if not isinstance(security_name, str):
        return False

    upper = security_name.upper()

    excluded_patterns = [
        r"\bWARRANTS?\b",
        r"\bRIGHTS?\b",
        r"\bUNITS?\b",
        r"\bPREFERRED\b",
        r"\bPREFERENCE\b",
        r"\bPFD\b",
        r"\bPREF\b",
        r"\bNOTES?\b",
        r"\bBONDS?\b",
        r"\bDEBENTURES?\b",
        r"\bETN\b",
    ]

    for pattern in excluded_patterns:

        if re.search(
            pattern,
            upper
        ):
            return False

    # Keep American Depositary Shares because they can represent
    # genuine common-equity listings.
    if (
        "DEPOSITARY SHARES" in upper
        and "AMERICAN DEPOSITARY SHARES" not in upper
    ):
        return False

    return True


def yahoo_us_symbol(symbol):
    """
    Convert US exchange notation to Yahoo notation.

    Example:
        BRK.B -> BRK-B
    """

    return (
        str(symbol)
        .strip()
        .upper()
        .replace(".", "-")
    )


def valid_us_symbol(symbol):
    """
    Keep normal equity-like ticker formats.
    """

    return bool(
        re.fullmatch(
            r"[A-Z]{1,6}(?:-[A-Z0-9]{1,2})?",
            symbol
        )
    )


def load_us_stock_universe():
    """
    Load US stocks from official Nasdaq Trader symbol files.
    """

    print(
        "Loading US stock universe from Nasdaq Trader..."
    )

    # -------------------- Nasdaq-listed --------------------

    nasdaq_text = request_text(
        NASDAQ_LISTED_URL
    )

    nasdaq = pd.read_csv(
        StringIO(nasdaq_text),
        sep="|"
    )

    nasdaq = nasdaq[
        ~nasdaq["Symbol"]
        .astype(str)
        .str.startswith("File Creation Time")
    ].copy()

    if "Test Issue" in nasdaq.columns:

        nasdaq = nasdaq[
            nasdaq["Test Issue"] == "N"
        ].copy()

    if "ETF" in nasdaq.columns:

        nasdaq = nasdaq[
            nasdaq["ETF"] == "N"
        ].copy()

    nasdaq = nasdaq[
        nasdaq["Security Name"]
        .apply(is_us_common_stock_like)
    ].copy()

    nasdaq_symbols = (
        nasdaq["Symbol"]
        .dropna()
        .map(yahoo_us_symbol)
        .tolist()
    )

    # -------------------- NYSE / AMEX / other --------------------

    other_text = request_text(
        OTHER_LISTED_URL
    )

    other = pd.read_csv(
        StringIO(other_text),
        sep="|"
    )

    other = other[
        ~other["ACT Symbol"]
        .astype(str)
        .str.startswith("File Creation Time")
    ].copy()

    if "Test Issue" in other.columns:

        other = other[
            other["Test Issue"] == "N"
        ].copy()

    if "ETF" in other.columns:

        other = other[
            other["ETF"] == "N"
        ].copy()

    other = other[
        other["Security Name"]
        .apply(is_us_common_stock_like)
    ].copy()

    symbol_column = (
        "NASDAQ Symbol"
        if "NASDAQ Symbol" in other.columns
        else "ACT Symbol"
    )

    other_symbols = (
        other[symbol_column]
        .dropna()
        .map(yahoo_us_symbol)
        .tolist()
    )

    symbols = (
        nasdaq_symbols
        + other_symbols
    )

    symbols = [
        symbol
        for symbol in symbols
        if valid_us_symbol(symbol)
    ]

    symbols = sorted(
        set(symbols)
    )

    if TEST_SYMBOL_LIMIT is not None:
        symbols = symbols[
            :TEST_SYMBOL_LIMIT
        ]

    print(
        f"US symbols loaded: "
        f"{len(symbols):,}\n"
    )

    return symbols


# ============================================================
# SECTION 8: LOAD CANADIAN STOCK UNIVERSE
# ============================================================

def is_canadian_common_stock_like(
    symbol,
    company_name
):
    """
    Remove obvious Canadian preferreds, debt, ETFs, funds,
    warrants and rights.

    REIT/trust units are allowed unless they are clearly funds.
    """

    symbol_upper = str(
        symbol
    ).upper()

    name_upper = str(
        company_name
    ).upper()

    # Preferred-share style symbols.
    if (
        ".PR." in symbol_upper
        or symbol_upper.endswith(".PR")
    ):
        return False

    excluded_name_words = [
        " ETF",
        "EXCHANGE TRADED FUND",
        "PREFERRED",
        "PREF ",
        " DEBENTURE",
        " DEBT",
        " BOND",
        " WARRANT",
        " RIGHTS",
        " SPLIT CORP",
        " CLOSED-END",
        " CLOSED END",
    ]

    for phrase in excluded_name_words:

        if phrase in name_upper:
            return False

    return True


def yahoo_canada_symbol(
    symbol,
    market
):
    """
    Convert TMX symbols to Yahoo Finance notation.

    Examples:
        BBD.B on TSX -> BBD-B.TO
        AP.UN on TSX -> AP-UN.TO
        ABC on TSXV  -> ABC.V
    """

    base = (
        str(symbol)
        .strip()
        .upper()
        .replace(".", "-")
    )

    market = str(
        market
    ).upper()

    if "VENTURE" in market or market == "TSXV":
        return base + ".V"

    return base + ".TO"


def load_canada_from_full_tmx_issuer_list():
    """
    Try TMX's monthly FULL TSX/TSXV issuer list before the narrower
    MOC-eligible table. The current-market-statistics page is official
    TMX and publishes a latest "TSX & TSXV Listed Companies" file.

    The function is intentionally flexible because TMX may change the
    exact downloadable filename. If discovery/parsing fails, the caller
    falls back to the known MOC-eligible official page.
    """

    html = request_text(
        TMX_CURRENT_MARKET_STATISTICS_URL,
        headers={
            "Referer": "https://www.tsx.com/",
            "Accept-Language": "en-CA,en;q=0.9",
        }
    )

    hrefs = re.findall(
        r'href=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE
    )

    candidates = []

    for href in hrefs:
        lower = href.lower()

        if not lower.endswith((
            ".xlsx",
            ".xls",
            ".csv"
        )):
            continue

        if any(
            keyword in lower
            for keyword in [
                "listed",
                "issuer",
                "tsx",
                "companies"
            ]
        ):
            candidates.append(href)

    if not candidates:
        raise RuntimeError(
            "TMX full issuer download link was not discoverable."
        )

    from urllib.parse import urljoin

    last_error = None

    for href in candidates:

        try:
            url = urljoin(
                TMX_CURRENT_MARKET_STATISTICS_URL,
                href
            )

            raw = request_bytes(
                url,
                headers={
                    "Referer": TMX_CURRENT_MARKET_STATISTICS_URL,
                    "Accept-Language": "en-CA,en;q=0.9",
                }
            )

            lower = url.lower()

            if lower.endswith(".csv"):
                table = pd.read_csv(
                    io.BytesIO(raw),
                    low_memory=False
                )
            else:
                table = pd.read_excel(
                    io.BytesIO(raw)
                )

            normalized_columns = {
                re.sub(
                    r"[^a-z0-9]",
                    "",
                    str(column).lower()
                ): column
                for column in table.columns
            }

            symbol_col = None
            market_col = None
            company_col = None

            for key, original in normalized_columns.items():
                if symbol_col is None and "symbol" in key:
                    symbol_col = original

                if market_col is None and (
                    "exchange" in key
                    or key == "market"
                    or "marketname" in key
                ):
                    market_col = original

                if company_col is None and (
                    "company" in key
                    or "issuer" in key
                    or key == "name"
                ):
                    company_col = original

            if symbol_col is None:
                raise RuntimeError(
                    "No symbol column in TMX issuer file."
                )

            symbols = []

            for _, row in table.iterrows():

                raw_symbol = row.get(
                    symbol_col
                )

                if raw_symbol is None or pd.isna(raw_symbol):
                    continue

                symbol_text = str(
                    raw_symbol
                ).strip().upper()

                company_name = (
                    str(row.get(company_col, ""))
                    if company_col is not None
                    else ""
                )

                market_text = (
                    str(row.get(market_col, ""))
                    if market_col is not None
                    else ""
                ).upper()

                # Some TMX files may put TSX:/TSXV: in the symbol.
                if symbol_text.startswith("TSXV:"):
                    market_text = "TSXV"
                    symbol_text = symbol_text[5:]

                elif symbol_text.startswith("TSX:"):
                    market_text = "TSX"
                    symbol_text = symbol_text[4:]

                if not is_canadian_common_stock_like(
                    symbol_text,
                    company_name
                ):
                    continue

                if (
                    "VENTURE" in market_text
                    or "TSXV" in market_text
                ):
                    yahoo_ticker = yahoo_canada_symbol(
                        symbol_text,
                        "TSXV"
                    )

                elif "TSX" in market_text:
                    yahoo_ticker = yahoo_canada_symbol(
                        symbol_text,
                        "TSX"
                    )

                else:
                    # If market is genuinely unavailable, do not guess
                    # whether the ticker should be .TO or .V.
                    continue

                symbols.append(
                    yahoo_ticker
                )

            symbols = sorted(
                set(symbols)
            )

            if len(symbols) >= MIN_UNIVERSE_SIZE["CANADA"]:
                print(
                    "Canadian universe source: "
                    "TMX full TSX/TSXV listed-companies file"
                )
                return symbols

            raise RuntimeError(
                f"Parsed TMX issuer file was too small: {len(symbols)}"
            )

        except Exception as error:
            last_error = error

    raise RuntimeError(
        f"TMX full issuer-list parsing failed: {last_error}"
    )


def load_canada_stock_universe():
    """
    Load Canadian equities from official TMX sources.

    Priority:
        1. TMX monthly FULL TSX/TSXV listed-companies file
        2. TMX TSX/TSXV MOC-eligible-stock table fallback

    This improves coverage while preserving a known official fallback.
    """

    print(
        "Loading Canadian TSX/TSXV universe from TMX..."
    )

    try:
        full_symbols = load_canada_from_full_tmx_issuer_list()

        if TEST_SYMBOL_LIMIT is not None:
            full_symbols = full_symbols[
                :TEST_SYMBOL_LIMIT
            ]

        print(
            f"Canadian symbols loaded: "
            f"{len(full_symbols):,}\n"
        )

        return full_symbols

    except Exception as full_list_error:

        print(
            "  Full TMX issuer-list loader unavailable; "
            "falling back to TMX MOC-eligible list."
        )

        print(
            f"  Full-list note: {full_list_error}"
        )

    headers = {
        "Referer": "https://www.tsx.com/",
        "Accept-Language": "en-CA,en;q=0.9",
    }

    html = request_text(
        TMX_MOC_URL,
        headers=headers
    )

    # pandas can extract HTML tables directly.
    tables = pd.read_html(
        StringIO(html)
    )

    target_table = None

    for table in tables:

        normalized_columns = {
            str(column).strip().lower():
                column
            for column in table.columns
        }

        if (
            "symbol" in normalized_columns
            and "market" in normalized_columns
        ):

            target_table = table.copy()
            break

    if target_table is None:

        raise RuntimeError(
            "TMX page downloaded, but the Symbol/Market table "
            "could not be found."
        )

    # Find actual case-sensitive column names.
    column_lookup = {
        str(column).strip().lower():
            column
        for column in target_table.columns
    }

    symbol_col = column_lookup["symbol"]
    market_col = column_lookup["market"]

    company_col = (
        column_lookup.get("company name")
        or column_lookup.get("name")
    )

    symbols = []

    for _, row in target_table.iterrows():

        symbol = row.get(
            symbol_col
        )

        market = row.get(
            market_col
        )

        company = (
            row.get(company_col)
            if company_col is not None
            else ""
        )

        if pd.isna(symbol) or pd.isna(market):
            continue

        if not is_canadian_common_stock_like(
            symbol,
            company
        ):
            continue

        yahoo_ticker = yahoo_canada_symbol(
            symbol,
            market
        )

        symbols.append(
            yahoo_ticker
        )

    symbols = sorted(
        set(symbols)
    )

    if TEST_SYMBOL_LIMIT is not None:
        symbols = symbols[
            :TEST_SYMBOL_LIMIT
        ]

    print(
        f"Canadian symbols loaded: "
        f"{len(symbols):,}\n"
    )

    return symbols


# ============================================================
# SECTION 9: LOAD NSE STOCK UNIVERSE
# ============================================================

def load_nse_universe():
    """
    Load the official NSE Equity-segment security list.

    Returns:
        list of Yahoo tickers
        set of NSE ISINs

    We keep the ISINs so BSE duplicates can be removed later.
    """

    print(
        "  Loading NSE equity list..."
    )

    text = request_text(
        NSE_EQUITY_URL,
        headers={
            "Referer": "https://www.nseindia.com/",
        }
    )

    dataframe = pd.read_csv(
        StringIO(text)
    )

    # Clean column names because NSE CSV headers can contain spaces.
    dataframe.columns = [
        str(column).strip()
        for column in dataframe.columns
    ]

    # Find required columns safely.
    symbol_col = None
    isin_col = None
    name_col = None

    for column in dataframe.columns:

        upper = column.upper()

        if upper == "SYMBOL":
            symbol_col = column

        elif "ISIN" in upper:
            isin_col = column

        elif (
            "NAME OF COMPANY" in upper
            or upper == "NAME"
        ):
            name_col = column

    if symbol_col is None:

        raise RuntimeError(
            "Could not find SYMBOL column in NSE equity CSV."
        )

    symbols = []
    isins = set()

    for _, row in dataframe.iterrows():

        symbol = row.get(
            symbol_col
        )

        if pd.isna(symbol):
            continue

        company_name = (
            str(row.get(name_col, ""))
            if name_col is not None
            else ""
        )

        upper_name = company_name.upper()

        # Safety filter for obvious funds/ETFs.
        if any(
            phrase in upper_name
            for phrase in [
                " ETF",
                "EXCHANGE TRADED FUND",
                "MUTUAL FUND",
            ]
        ):
            continue

        symbol = str(
            symbol
        ).strip().upper()

        symbols.append(
            symbol + ".NS"
        )

        if isin_col is not None:

            isin = row.get(
                isin_col
            )

            if (
                isin is not None
                and not pd.isna(isin)
            ):

                isin = str(
                    isin
                ).strip().upper()

                if isin:
                    isins.add(isin)

    return (
        sorted(set(symbols)),
        isins
    )


# ============================================================
# SECTION 10: LOAD BSE STOCK UNIVERSE
# ============================================================

def load_bse_universe(
    nse_isins
):
    """
    Load active BSE equities from BSE's official API.

    Yahoo Finance typically represents BSE stocks using the
    six-digit BSE scrip code:

        500325.BO

    If the same ISIN already exists on NSE, we SKIP the BSE copy.
    This prevents duplicate scanning of the same company.
    """

    print(
        "  Loading BSE active equity list..."
    )

    params = {
        "Group": "",
        "Scripcode": "",
        "industry": "",
        "segment": "Equity",
        "status": "Active",
    }

    headers = {
        "Host": "api.bseindia.com",
        "Referer": (
            "https://www.bseindia.com/"
            "corporates/ann.html"
        ),
        "Accept": (
            "application/json, "
            "text/plain, */*"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    data = request_json(
        BSE_SCRIP_API,
        params=params,
        headers=headers
    )

    # BSE generally returns a list directly.
    if isinstance(data, dict):

        # Some API versions may wrap the list.
        possible_lists = [
            value
            for value in data.values()
            if isinstance(value, list)
        ]

        if not possible_lists:
            raise RuntimeError(
                "BSE API returned JSON but no security list."
            )

        records = possible_lists[0]

    elif isinstance(data, list):

        records = data

    else:

        raise RuntimeError(
            "Unexpected BSE API response format."
        )

    symbols = []

    for record in records:

        if not isinstance(
            record,
            dict
        ):
            continue

        # BSE field names may vary slightly in capitalization.
        scrip_code = (
            record.get("SCRIP_CD")
            or record.get("scrip_cd")
            or record.get("ScripCode")
            or record.get("SCRIPCODE")
        )

        isin = (
            record.get("ISIN_NUMBER")
            or record.get("ISIN")
            or record.get("isin")
        )

        scrip_name = (
            record.get("Scrip_Name")
            or record.get("scrip_name")
            or record.get("SCRIP_NAME")
            or ""
        )

        if scrip_code is None:
            continue

        scrip_code = str(
            scrip_code
        ).strip()

        # Yahoo's BSE code is normally six digits.
        if not re.fullmatch(
            r"\d{6}",
            scrip_code
        ):
            continue

        if isin is not None:

            isin = str(
                isin
            ).strip().upper()

            if (
                isin
                and isin in nse_isins
            ):
                continue

        upper_name = str(
            scrip_name
        ).upper()

        if any(
            phrase in upper_name
            for phrase in [
                " ETF",
                "EXCHANGE TRADED FUND",
                "MUTUAL FUND",
                "PREFERENCE",
                "PREFERRED",
                "DEBENTURE",
                " BOND",
                " WARRANT",
            ]
        ):
            continue

        symbols.append(
            scrip_code + ".BO"
        )

    return sorted(
        set(symbols)
    )


# ============================================================
# SECTION 11: LOAD INDIA NSE + BSE UNIVERSE
# ============================================================

def load_india_stock_universe():
    """
    Combine:
        NSE official equities
        +
        BSE active equities not already represented by NSE ISIN
    """

    print(
        "Loading Indian stock universe from NSE + BSE..."
    )

    nse_symbols, nse_isins = (
        load_nse_universe()
    )

    print(
        f"  NSE Yahoo symbols: "
        f"{len(nse_symbols):,}"
    )

    try:

        bse_symbols = load_bse_universe(
            nse_isins
        )

    except Exception as error:

        # NSE still gives us a valid India universe even if BSE's
        # anti-bot/API endpoint temporarily fails.
        print(
            "\n  WARNING: BSE universe could not be loaded."
        )

        print(
            f"  BSE error: {error}"
        )

        print(
            "  Continuing with NSE stocks only.\n"
        )

        bse_symbols = []

    print(
        f"  BSE-only Yahoo symbols: "
        f"{len(bse_symbols):,}"
    )

    symbols = sorted(
        set(
            nse_symbols
            + bse_symbols
        )
    )

    if TEST_SYMBOL_LIMIT is not None:
        symbols = symbols[
            :TEST_SYMBOL_LIMIT
        ]

    print(
        f"Indian symbols loaded: "
        f"{len(symbols):,}\n"
    )

    return symbols


# ============================================================
# SECTION 12: LOAD SELECTED MARKET UNIVERSE
# ============================================================

def load_market_universe(market):
    """
    Route the program to the correct official market loader.
    """

    if market == "US":
        return load_us_stock_universe()

    if market == "CANADA":
        return load_canada_stock_universe()

    if market == "INDIA":
        return load_india_stock_universe()

    raise ValueError(
        f"Unsupported market: {market}"
    )


# ============================================================
# SECTION 13: DETERMINE TARGET TRADING SESSION
# ============================================================

def get_target_sessions(market):
    """
    Determine the latest trading session for the selected market.

    We use a liquid benchmark for the market rather than the
    computer's calendar date.

    Example:
        If you run on Sunday,
        the target session will normally be Friday.
    """

    benchmarks = MARKET_CONFIG[
        market
    ]["session_benchmarks"]

    for benchmark in benchmarks:

        try:

            history = safe_yf_download(
                benchmark,
                period="10d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False
            )

            if (
                history is None
                or history.empty
                or len(history) < 2
            ):
                continue

            target_date = pd.Timestamp(
                history.index[-1]
            ).date()

            previous_date = pd.Timestamp(
                history.index[-2]
            ).date()

            return (
                target_date,
                previous_date,
                benchmark
            )

        except Exception:
            continue

    raise RuntimeError(
        "Could not determine the latest trading session "
        "from the market benchmarks."
    )


# ============================================================
# SECTION 14: BUILD SESSION-MATCHED CATALYST WINDOW
# ============================================================

def build_catalyst_window(
    market,
    previous_session_date,
    target_session_date
):
    """
    Match news to the SAME session as the daily price move.

    Correct causal window:

        PREVIOUS trading-session close
              ->
        TARGET trading-session close

    Why?

    News released after yesterday's close can cause today's move.
    News released AFTER today's close cannot have caused today's
    regular-session closing move, so Version 6 excludes it.

    This is slightly stricter than Version 5.
    """

    config = MARKET_CONFIG[
        market
    ]

    tz = config[
        "timezone"
    ]

    close_hour = config[
        "close_hour"
    ]

    close_minute = config[
        "close_minute"
    ]

    start_local = datetime.combine(
        previous_session_date,
        time(
            close_hour,
            close_minute
        ),
        tzinfo=tz
    )

    planned_end_local = datetime.combine(
        target_session_date,
        time(
            close_hour,
            close_minute
        ),
        tzinfo=tz
    )

    now_local = datetime.now(
        tz
    )

    # If target session is today and the market has not closed yet,
    # only use news available up to NOW.
    if (
        target_session_date == now_local.date()
        and now_local < planned_end_local
    ):
        end_local = now_local
    else:
        end_local = planned_end_local

    return (
        start_local,
        end_local,
        start_local.astimezone(
            timezone.utc
        ),
        end_local.astimezone(
            timezone.utc
        )
    )


def format_market_datetime(
    value,
    market
):
    """
    Show article times in the selected market's local timezone.
    """

    if value is None:
        return "N/A"

    tz = MARKET_CONFIG[
        market
    ]["timezone"]

    try:

        return (
            value
            .astimezone(tz)
            .strftime(
                "%Y-%m-%d %I:%M %p %Z"
            )
        )

    except Exception:

        return str(value)


# ============================================================
# SECTION 15: EXTRACT ONE STOCK FROM A YFINANCE BATCH
# ============================================================

def extract_ticker_history(
    batch_data,
    symbol
):
    """
    VERSION 7: robust extraction of ONE ticker from a yfinance batch.

    yfinance can return MultiIndex columns in more than one layout:

        Ticker -> Price field

    OR

        Price field -> Ticker

    Version 6 assumed the ticker was always on column level 0.
    If Yahoo/yfinance returned the other layout, every Indian stock
    could be silently treated as "missing."

    Version 7 searches ALL column levels for the ticker.
    """

    if (
        batch_data is None
        or batch_data.empty
    ):
        return None

    try:

        history = None

        # ----------------------------------------------------
        # MULTI-TICKER DOWNLOAD
        # ----------------------------------------------------

        if isinstance(
            batch_data.columns,
            pd.MultiIndex
        ):

            # Search every MultiIndex level for this symbol.
            for level in range(
                batch_data.columns.nlevels
            ):

                level_values = [
                    str(value).upper()
                    for value in
                    batch_data.columns.get_level_values(level)
                ]

                if symbol.upper() in level_values:

                    history = batch_data.xs(
                        symbol,
                        axis=1,
                        level=level,
                        drop_level=True
                    ).copy()

                    break

            if history is None:
                return None

        # ----------------------------------------------------
        # SINGLE-TICKER / SIMPLE DATAFRAME
        # ----------------------------------------------------

        else:

            history = batch_data.copy()

        # ----------------------------------------------------
        # IF COLUMNS ARE STILL MULTIINDEX, FIND THE PRICE LEVEL
        # ----------------------------------------------------

        if isinstance(
            history.columns,
            pd.MultiIndex
        ):

            price_fields = {
                "OPEN",
                "HIGH",
                "LOW",
                "CLOSE",
                "ADJ CLOSE",
                "VOLUME",
            }

            price_level = None

            for level in range(
                history.columns.nlevels
            ):

                values = {
                    str(value).upper()
                    for value in
                    history.columns.get_level_values(level)
                }

                if "CLOSE" in values and "VOLUME" in values:
                    price_level = level
                    break

            if price_level is None:
                return None

            # Collapse to simple price-field columns.
            history.columns = [
                str(value)
                for value in
                history.columns.get_level_values(price_level)
            ]

        # Standardize column labels.
        history.columns = [
            str(column).strip()
            for column in history.columns
        ]

        history = history.dropna(
            how="all"
        )

        if (
            "Close" not in history.columns
            or "Volume" not in history.columns
        ):
            return None

        history = history.dropna(
            subset=[
                "Close",
                "Volume"
            ]
        )

        # Remove duplicate index rows, if Yahoo returned any.
        history = history[
            ~history.index.duplicated(
                keep="last"
            )
        ]

        history = history.sort_index()

        return history

    except Exception:

        return None


def scalar_from_row(
    row,
    column
):
    """
    Safely extract one numeric value from a pandas row.

    This protects us if pandas gives a one-item Series instead of
    a simple scalar because of duplicate/multi-level columns.
    """

    value = row[column]

    if isinstance(
        value,
        pd.Series
    ):

        if value.empty:
            raise ValueError(
                f"No value for {column}"
            )

        value = value.iloc[0]

    return float(value)


def session_date_positions(
    history
):
    """
    Convert each historical index timestamp into a plain calendar date.

    Returns:
        list of date objects

    yfinance's own documentation notes that daily downloads may have
    timezone behavior that differs from intraday downloads, so Version 7
    compares normalized calendar dates rather than assuming row position.
    """

    return [
        pd.Timestamp(index_value).date()
        for index_value in history.index
    ]


def get_rows_for_target_session(
    history,
    target_session_date,
    previous_session_date
):
    """
    Locate the exact target-session and previous-session rows BY DATE.

    Version 6 required:
        history.iloc[-1] == target date
        history.iloc[-2] == previous date

    That is unnecessarily fragile.

    Version 7 searches the full returned history for those dates.

    It also returns the 20 trading sessions immediately BEFORE the
    target session for the RVOL denominator.
    """

    if history is None or history.empty:
        return None

    dates = session_date_positions(
        history
    )

    target_positions = [
        position
        for position, date_value in enumerate(dates)
        if date_value == target_session_date
    ]

    previous_positions = [
        position
        for position, date_value in enumerate(dates)
        if date_value == previous_session_date
    ]

    if not target_positions:
        return {
            "status": "TARGET_MISSING",
            "dates": dates,
        }

    if not previous_positions:
        return {
            "status": "PREVIOUS_MISSING",
            "dates": dates,
        }

    target_position = target_positions[-1]
    previous_position = previous_positions[-1]

    # We need twenty completed sessions BEFORE the target day.
    prior_positions = [
        position
        for position, date_value in enumerate(dates)
        if date_value < target_session_date
    ]

    if len(prior_positions) < 20:
        return {
            "status": "INSUFFICIENT_HISTORY",
            "dates": dates,
        }

    prior_20_positions = prior_positions[-20:]

    return {
        "status": "OK",
        "current_day": history.iloc[target_position],
        "previous_day": history.iloc[previous_position],
        "prior_20": history.iloc[prior_20_positions].copy(),
        "dates": dates,
    }



# ============================================================
# VERSION 8: DOWNLOAD BINARY EXCHANGE FILES
# ============================================================

def request_bytes(
    url,
    headers=None,
    timeout=30
):
    """
    Download raw bytes with retries.

    Used for official NSE/BSE bhavcopy files and TMX downloadable
    issuer files.
    """

    final_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/151.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    if headers:
        final_headers.update(
            headers
        )

    last_error = None

    for attempt in range(1, 4):

        try:
            request = Request(
                url,
                headers=final_headers
            )

            with urlopen(
                request,
                timeout=timeout
            ) as response:

                return response.read()

        except Exception as urllib_error:
            last_error = urllib_error

        try:
            response = requests.get(
                url,
                headers=final_headers,
                timeout=timeout
            )

            response.raise_for_status()

            return response.content

        except Exception as requests_error:
            last_error = requests_error

        if attempt < 3:
            pytime.sleep(
                1.0 * attempt
            )

    raise RuntimeError(
        f"Could not download bytes from {url}: {last_error}"
    )


# ============================================================
# VERSION 8: FLEXIBLE COLUMN FINDER
# ============================================================

def find_dataframe_column(
    dataframe,
    candidate_names
):
    """
    Find a column even if capitalization varies slightly.

    Example:
        "TckrSymb"
        "TCKRSYMB"
        "tckrsymb"

    all resolve to the same logical field.
    """

    normalized = {
        re.sub(
            r"[^A-Z0-9]",
            "",
            str(column).upper()
        ):
        column

        for column in dataframe.columns
    }

    for candidate in candidate_names:

        key = re.sub(
            r"[^A-Z0-9]",
            "",
            str(candidate).upper()
        )

        if key in normalized:
            return normalized[key]

    return None


# ============================================================
# VERSION 8: NSE UDiFF BHAVCOPY
# ============================================================

def fetch_nse_official_bhavcopy(
    trade_date,
    allowed_nse_symbols
):
    """
    Download ONE official NSE CM UDiFF bhavcopy.

    Returns a normalized DataFrame:

        Ticker
        Date
        Close
        Volume

    Ticker is converted to Yahoo notation:
        RELIANCE -> RELIANCE.NS
    """

    date_string = trade_date.strftime(
        "%Y%m%d"
    )

    url = NSE_UDIFF_BHAVCOPY_URL.format(
        date=date_string
    )

    raw_bytes = request_bytes(
        url,
        headers={
            "Referer": "https://www.nseindia.com/",
            "Accept": (
                "application/zip,"
                "application/octet-stream,"
                "*/*"
            ),
        },
        timeout=20
    )

    # NSE distributes this file as ZIP.
    with zipfile.ZipFile(
        io.BytesIO(raw_bytes)
    ) as archive:

        csv_names = [
            name
            for name in archive.namelist()
            if name.lower().endswith(
                ".csv"
            )
        ]

        if not csv_names:

            raise RuntimeError(
                "NSE ZIP contains no CSV."
            )

        with archive.open(
            csv_names[0]
        ) as file_handle:

            dataframe = pd.read_csv(
                file_handle,
                low_memory=False
            )

    # UDiFF standardized fields.
    symbol_col = find_dataframe_column(
        dataframe,
        [
            "TckrSymb",
            "SYMBOL"
        ]
    )

    close_col = find_dataframe_column(
        dataframe,
        [
            "ClsPric",
            "CLOSE"
        ]
    )

    volume_col = find_dataframe_column(
        dataframe,
        [
            "TtlTradgVol",
            "TOTTRDQTY",
            "VOLUME"
        ]
    )

    series_col = find_dataframe_column(
        dataframe,
        [
            "SctySrs",
            "SERIES"
        ]
    )

    if (
        symbol_col is None
        or close_col is None
        or volume_col is None
    ):

        raise RuntimeError(
            "Required NSE UDiFF columns were not found."
        )

    normalized = pd.DataFrame()

    normalized["Base Symbol"] = (
        dataframe[symbol_col]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    normalized["Ticker"] = (
        normalized["Base Symbol"]
        + ".NS"
    )

    normalized["Close"] = pd.to_numeric(
        dataframe[close_col],
        errors="coerce"
    )

    normalized["Volume"] = pd.to_numeric(
        dataframe[volume_col],
        errors="coerce"
    )

    normalized["Date"] = trade_date

    # --------------------------------------------------------
    # Keep only securities that came from our official/current NSE
    # Equity-segment security universe.
    # --------------------------------------------------------

    normalized = normalized[
        normalized["Ticker"].isin(
            allowed_nse_symbols
        )
    ].copy()

    # --------------------------------------------------------
    # If series exists, remove obvious block / T0 / special rows.
    #
    # We do NOT restrict only to EQ because genuine listed equities
    # may temporarily trade in surveillance/trade-for-trade series.
    # --------------------------------------------------------

    if series_col is not None:

        series = (
            dataframe.loc[
                normalized.index,
                series_col
            ]
            .astype(str)
            .str.upper()
            .str.strip()
        )

        bad_series = {
            "BL",
            "BT",
            "T0",
        }

        normalized = normalized[
            ~series.isin(
                bad_series
            )
        ].copy()

    normalized = normalized.dropna(
        subset=[
            "Ticker",
            "Close",
            "Volume"
        ]
    )

    # A symbol could theoretically appear in multiple acceptable
    # rows/series. Keep the row with the highest trading volume.
    normalized = (
        normalized
        .sort_values(
            "Volume",
            ascending=False
        )
        .drop_duplicates(
            subset=["Ticker"],
            keep="first"
        )
    )

    return normalized[
        [
            "Ticker",
            "Date",
            "Close",
            "Volume"
        ]
    ].reset_index(
        drop=True
    )


# ============================================================
# VERSION 8: BSE SECURITY LOOKUP MAP
# ============================================================

def get_bse_official_lookup_maps():
    """
    Build official BSE identifier maps using BSE's active-equity API.

    Returns:
        ISIN -> six-digit BSE scrip code
        ticker/symbol text -> six-digit BSE scrip code

    The ISIN map is preferred because it is the strongest identifier.
    """

    params = {
        "Group": "",
        "Scripcode": "",
        "industry": "",
        "segment": "Equity",
        "status": "Active",
    }

    headers = {
        "Host": "api.bseindia.com",
        "Referer": "https://www.bseindia.com/",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    data = request_json(
        BSE_SCRIP_API,
        params=params,
        headers=headers,
        timeout=30
    )

    if isinstance(
        data,
        dict
    ):

        possible_lists = [
            value
            for value in data.values()
            if isinstance(
                value,
                list
            )
        ]

        records = (
            possible_lists[0]
            if possible_lists
            else []
        )

    elif isinstance(
        data,
        list
    ):

        records = data

    else:

        records = []

    isin_to_code = {}
    ticker_to_code = {}

    for record in records:

        if not isinstance(
            record,
            dict
        ):
            continue

        scrip_code = (
            record.get("SCRIP_CD")
            or record.get("scrip_cd")
            or record.get("ScripCode")
            or record.get("SCRIPCODE")
            or record.get("Scrip_Code")
        )

        if scrip_code is None:
            continue

        # BSE scrip codes are six-digit numeric identifiers.
        try:
            scrip_code = str(
                int(
                    float(
                        str(scrip_code)
                    )
                )
            ).zfill(6)
        except Exception:
            continue

        if not re.fullmatch(
            r"\d{6}",
            scrip_code
        ):
            continue

        isin = (
            record.get("ISIN_NUMBER")
            or record.get("ISIN")
            or record.get("isin")
            or record.get("ISIN_Number")
        )

        if isin is not None:

            isin = str(
                isin
            ).strip().upper()

            if isin:
                isin_to_code[
                    isin
                ] = scrip_code

        ticker_text = (
            record.get("Scrip_Name")
            or record.get("scrip_name")
            or record.get("SCRIP_NAME")
            or record.get("Scrip_Id")
            or record.get("SCRIP_ID")
        )

        if ticker_text:

            ticker_text = str(
                ticker_text
            ).strip().upper()

            if ticker_text:
                ticker_to_code[
                    ticker_text
                ] = scrip_code

    return (
        isin_to_code,
        ticker_to_code
    )


# ============================================================
# VERSION 8: BSE UDiFF BHAVCOPY
# ============================================================

def fetch_bse_official_bhavcopy(
    trade_date,
    allowed_bse_symbols,
    bse_isin_to_code,
    bse_ticker_to_code
):
    """
    Download ONE official BSE equity UDiFF bhavcopy and normalize it.

    Yahoo BSE ticker convention:
        BSE scrip code 500325 -> 500325.BO

    We try, in order:
        1. direct six-digit instrument/scrip code in the bhavcopy
        2. ISIN -> official BSE active-equity map
        3. ticker text -> official BSE map
    """

    date_string = trade_date.strftime(
        "%Y%m%d"
    )

    url = BSE_UDIFF_BHAVCOPY_URL.format(
        date=date_string
    )

    raw_bytes = request_bytes(
        url,
        headers={
            "Referer": "https://www.bseindia.com/",
            "Accept": (
                "text/csv,"
                "application/octet-stream,"
                "*/*"
            ),
        },
        timeout=20
    )

    dataframe = pd.read_csv(
        io.BytesIO(raw_bytes),
        low_memory=False
    )

    close_col = find_dataframe_column(
        dataframe,
        [
            "ClsPric",
            "CLOSE",
            "CLOSE_PRICE"
        ]
    )

    volume_col = find_dataframe_column(
        dataframe,
        [
            "TtlTradgVol",
            "NO_OF_SHRS",
            "TOTTRDQTY",
            "VOLUME"
        ]
    )

    isin_col = find_dataframe_column(
        dataframe,
        [
            "ISIN",
            "ISIN_NUMBER",
            "ISINNo"
        ]
    )

    direct_code_col = find_dataframe_column(
        dataframe,
        [
            "FinInstrmId",
            "SCRIP_CD",
            "SC_CODE",
            "ScripCode"
        ]
    )

    ticker_col = find_dataframe_column(
        dataframe,
        [
            "TckrSymb",
            "SCRIP_ID",
            "SC_NAME",
            "SYMBOL"
        ]
    )

    if (
        close_col is None
        or volume_col is None
    ):

        raise RuntimeError(
            "Required BSE UDiFF price/volume columns were not found."
        )

    def resolve_bse_code(row):
        """
        Resolve one bhavcopy row to a six-digit BSE scrip code.
        """

        # 1) Direct instrument/scrip code.
        if direct_code_col is not None:

            raw_code = row.get(
                direct_code_col
            )

            if (
                raw_code is not None
                and not pd.isna(raw_code)
            ):

                text = str(
                    raw_code
                ).strip()

                # Handle values such as 500325.0
                try:
                    numeric_text = str(
                        int(
                            float(text)
                        )
                    ).zfill(6)

                    if re.fullmatch(
                        r"\d{6}",
                        numeric_text
                    ):
                        return numeric_text

                except Exception:
                    pass

        # 2) ISIN mapping.
        if isin_col is not None:

            raw_isin = row.get(
                isin_col
            )

            if (
                raw_isin is not None
                and not pd.isna(raw_isin)
            ):

                isin = str(
                    raw_isin
                ).strip().upper()

                mapped = bse_isin_to_code.get(
                    isin
                )

                if mapped:
                    return mapped

        # 3) Ticker/security-id text mapping.
        if ticker_col is not None:

            raw_ticker = row.get(
                ticker_col
            )

            if (
                raw_ticker is not None
                and not pd.isna(raw_ticker)
            ):

                ticker_text = str(
                    raw_ticker
                ).strip().upper()

                mapped = bse_ticker_to_code.get(
                    ticker_text
                )

                if mapped:
                    return mapped

        return None

    work = dataframe.copy()

    work["BSE Code"] = work.apply(
        resolve_bse_code,
        axis=1
    )

    work["Ticker"] = (
        work["BSE Code"]
        .astype(str)
        + ".BO"
    )

    work["Close"] = pd.to_numeric(
        work[close_col],
        errors="coerce"
    )

    work["Volume"] = pd.to_numeric(
        work[volume_col],
        errors="coerce"
    )

    work["Date"] = trade_date

    work = work[
        work["Ticker"].isin(
            allowed_bse_symbols
        )
    ].copy()

    work = work.dropna(
        subset=[
            "Ticker",
            "Close",
            "Volume"
        ]
    )

    work = (
        work
        .sort_values(
            "Volume",
            ascending=False
        )
        .drop_duplicates(
            subset=["Ticker"],
            keep="first"
        )
    )

    return work[
        [
            "Ticker",
            "Date",
            "Close",
            "Volume"
        ]
    ].reset_index(
        drop=True
    )


# ============================================================
# VERSION 8: DOWNLOAD 21 OFFICIAL TRADING SESSIONS
# ============================================================

def download_official_india_history(
    source_name,
    target_session_date,
    fetch_function
):
    """
    Download the target session plus the previous 20 successful
    official trading-session files.

    Weekends are skipped without making a network request.
    Exchange holidays naturally return no valid file and are skipped.

    Returns:
        list of normalized daily DataFrames
    """

    frames = []

    current_date = (
        target_session_date
    )

    attempts = 0

    print(
        f"\n  Downloading {source_name} "
        f"official bhavcopies..."
    )

    while (
        len(frames)
        < INDIA_REQUIRED_SESSIONS
        and attempts
        < INDIA_MAX_CALENDAR_LOOKBACK
    ):

        attempts += 1

        # Saturday / Sunday.
        if current_date.weekday() >= 5:

            current_date -= timedelta(
                days=1
            )

            continue

        try:

            dataframe = fetch_function(
                current_date
            )

            if (
                dataframe is not None
                and not dataframe.empty
            ):

                frames.append(
                    dataframe
                )

                print(
                    f"    {source_name}: "
                    f"{current_date} "
                    f"({len(dataframe):,} securities)"
                )

        except Exception:

            # Most commonly:
            # market holiday / file not available / temporary endpoint issue.
            pass

        current_date -= timedelta(
            days=1
        )

    # Oldest -> newest.
    frames = sorted(
        frames,
        key=lambda frame:
            frame["Date"].iloc[0]
    )

    print(
        f"  {source_name} trading sessions loaded: "
        f"{len(frames)}/{INDIA_REQUIRED_SESSIONS}"
    )

    return frames


# ============================================================
# VERSION 8: CALCULATE INDIA STAGE-1 FROM EXCHANGE FILES
# ============================================================

def screen_official_india_history(
    frames,
    source_name
):
    """
    Calculate Price, Day Change and 20-day RVOL directly from
    official exchange bhavcopies.

    We require exactly:
        target session
        +
        20 previous trading sessions

    A stock must itself have 20 historical observations to receive
    a valid 20-day RVOL.
    """

    if (
        frames is None
        or len(frames)
        < INDIA_REQUIRED_SESSIONS
    ):

        print(
            f"  {source_name}: not enough official sessions "
            "for a 20-day RVOL calculation."
        )

        return (
            pd.DataFrame(),
            {}
        )

    # Use the newest 21 successful sessions.
    frames = frames[
        -INDIA_REQUIRED_SESSIONS:
    ]

    all_data = pd.concat(
        frames,
        ignore_index=True
    )

    session_dates = sorted(
        all_data["Date"]
        .dropna()
        .unique()
    )

    if len(session_dates) < 21:

        return (
            pd.DataFrame(),
            {}
        )

    target_date = session_dates[-1]
    previous_date = session_dates[-2]
    prior_20_dates = session_dates[-21:-1]

    target = all_data[
        all_data["Date"]
        == target_date
    ][
        [
            "Ticker",
            "Close",
            "Volume"
        ]
    ].copy()

    target = target.rename(
        columns={
            "Close": "Target Close",
            "Volume": "Target Volume"
        }
    )

    previous = all_data[
        all_data["Date"]
        == previous_date
    ][
        [
            "Ticker",
            "Close"
        ]
    ].copy()

    previous = previous.rename(
        columns={
            "Close": "Previous Close"
        }
    )

    history_20 = all_data[
        all_data["Date"].isin(
            prior_20_dates
        )
    ].copy()

    volume_stats = (
        history_20
        .groupby("Ticker")
        .agg(
            Avg_Volume=(
                "Volume",
                "mean"
            ),
            History_Count=(
                "Volume",
                "count"
            )
        )
        .reset_index()
    )

    merged = (
        target
        .merge(
            previous,
            on="Ticker",
            how="inner"
        )
        .merge(
            volume_stats,
            on="Ticker",
            how="inner"
        )
    )

    # Require a full 20-session RVOL denominator.
    merged = merged[
        merged["History_Count"]
        >= 20
    ].copy()

    merged = merged[
        merged["Previous Close"]
        > 0
    ].copy()

    merged = merged[
        merged["Avg_Volume"]
        > 0
    ].copy()

    merged["Day Change %"] = (
        (
            merged["Target Close"]
            - merged["Previous Close"]
        )
        / merged["Previous Close"]
    ) * 100

    merged["RVOL"] = (
        merged["Target Volume"]
        / merged["Avg_Volume"]
    )

    config = MARKET_CONFIG[
        "INDIA"
    ]

    merged["Change Pass"] = (
        merged["Day Change %"]
        >= MIN_DAY_CHANGE
    )

    merged["RVOL Pass"] = (
        merged["RVOL"]
        >= MIN_RVOL
    )

    merged["Price Pass"] = (
        (
            merged["Target Close"]
            >= config["min_price"]
        )
        &
        (
            merged["Target Close"]
            <= config["max_price"]
        )
    )

    merged["Stage 1 Score"] = (
        merged[
            [
                "Change Pass",
                "RVOL Pass",
                "Price Pass"
            ]
        ]
        .sum(
            axis=1
        )
    )

    merged["Current Turnover"] = (
        merged["Target Close"]
        * merged["Target Volume"]
    )

    merged["RVOL Quality"] = (
        merged["Avg_Volume"]
        .apply(
            lambda value:
                "GOOD"
                if value
                >= MIN_AVG_VOLUME_FOR_GOOD_RVOL
                else "LOW BASE"
        )
    )

    merged["Liquidity Quality"] = (
        merged["Current Turnover"]
        .apply(
            lambda value:
                "GOOD"
                if value
                >= config[
                    "min_turnover_for_good_liquidity"
                ]
                else "LOW TURNOVER"
        )
    )

    diagnostics = {
        "source": source_name,
        "target_date": target_date,
        "previous_date": previous_date,
        "target_securities": len(target),
        "full_20day_history": len(merged),
        "pass_change": int(
            merged["Change Pass"].sum()
        ),
        "pass_rvol": int(
            merged["RVOL Pass"].sum()
        ),
        "pass_price": int(
            merged["Price Pass"].sum()
        ),
        "survivors": int(
            (
                merged["Stage 1 Score"]
                >= 2
            ).sum()
        ),
    }

    survivors = merged[
        merged["Stage 1 Score"]
        >= 2
    ].copy()

    if survivors.empty:

        return (
            pd.DataFrame(),
            diagnostics
        )

    output = pd.DataFrame(
        {
            "Ticker":
                survivors["Ticker"],

            "Market":
                "INDIA",

            "Session Date":
                target_date,

            "Previous Session Date":
                previous_date,

            "Price":
                survivors[
                    "Target Close"
                ],

            "Day Change %":
                survivors[
                    "Day Change %"
                ],

            "RVOL":
                survivors[
                    "RVOL"
                ],

            "Volume":
                survivors[
                    "Target Volume"
                ],

            "20D Avg Volume":
                survivors[
                    "Avg_Volume"
                ],

            "Current Turnover":
                survivors[
                    "Current Turnover"
                ],

            # Approximation based on target price and historical volume.
            # It is used only for display/sorting and not for scoring.
            "20D Avg Turnover":
                (
                    survivors[
                        "Target Close"
                    ]
                    * survivors[
                        "Avg_Volume"
                    ]
                ),

            "RVOL Quality":
                survivors[
                    "RVOL Quality"
                ],

            "Liquidity Quality":
                survivors[
                    "Liquidity Quality"
                ],

            "Change Pass":
                survivors[
                    "Change Pass"
                ],

            "RVOL Pass":
                survivors[
                    "RVOL Pass"
                ],

            "Price Pass":
                survivors[
                    "Price Pass"
                ],

            "Stage 1 Score":
                survivors[
                    "Stage 1 Score"
                ],

            "Stage 1 Source":
                source_name,
        }
    )

    return (
        output.reset_index(
            drop=True
        ),
        diagnostics
    )


# ============================================================
# VERSION 8: INDIA OFFICIAL STAGE 1
# ============================================================

def run_india_official_stage_one(
    symbols,
    target_session_date,
    previous_session_date
):
    """
    India no longer uses the large Yahoo batch download for Stage 1.

    NSE:
        Official NSE UDiFF Common Bhavcopy Final files.

    BSE:
        Official BSE UDiFF equity bhavcopy files.

    Existing India universe is still useful because it defines which
    current equity listings we are willing to screen.
    """

    allowed_nse_symbols = {
        symbol
        for symbol in symbols
        if symbol.endswith(
            ".NS"
        )
    }

    allowed_bse_symbols = {
        symbol
        for symbol in symbols
        if symbol.endswith(
            ".BO"
        )
    }

    print(
        "\n"
        + "=" * 88
    )

    print(
        "INDIA STAGE 1 - OFFICIAL NSE/BSE EXCHANGE DATA"
    )

    print(
        "=" * 88
    )

    print(
        f"NSE universe allowed: "
        f"{len(allowed_nse_symbols):,}"
    )

    print(
        f"BSE-only universe allowed: "
        f"{len(allowed_bse_symbols):,}"
    )

    # --------------------------------------------------------
    # NSE
    # --------------------------------------------------------

    nse_frames = download_official_india_history(
        source_name="NSE",
        target_session_date=target_session_date,
        fetch_function=(
            lambda trade_date:
                fetch_nse_official_bhavcopy(
                    trade_date,
                    allowed_nse_symbols
                )
        )
    )

    (
        nse_survivors,
        nse_diagnostics

    ) = screen_official_india_history(
        nse_frames,
        "NSE UDiFF Bhavcopy"
    )

    # --------------------------------------------------------
    # BSE
    # --------------------------------------------------------

    bse_survivors = pd.DataFrame()
    bse_diagnostics = {}

    try:

        (
            bse_isin_to_code,
            bse_ticker_to_code

        ) = get_bse_official_lookup_maps()

        bse_frames = download_official_india_history(
            source_name="BSE",
            target_session_date=target_session_date,
            fetch_function=(
                lambda trade_date:
                    fetch_bse_official_bhavcopy(
                        trade_date,
                        allowed_bse_symbols,
                        bse_isin_to_code,
                        bse_ticker_to_code
                    )
            )
        )

        (
            bse_survivors,
            bse_diagnostics

        ) = screen_official_india_history(
            bse_frames,
            "BSE UDiFF Bhavcopy"
        )

    except Exception as error:

        print(
            "\nBSE OFFICIAL BHAVCOPY WARNING:"
        )

        print(
            f"  {error}"
        )

        print(
            "  NSE results will continue. "
            "BSE is marked incomplete for this run."
        )

    # --------------------------------------------------------
    # COMBINE
    # --------------------------------------------------------

    frames_to_combine = [
        dataframe
        for dataframe in [
            nse_survivors,
            bse_survivors
        ]
        if (
            dataframe is not None
            and not dataframe.empty
        )
    ]

    if frames_to_combine:

        combined = pd.concat(
            frames_to_combine,
            ignore_index=True
        )

        combined = (
            combined
            .sort_values(
                [
                    "Stage 1 Score",
                    "RVOL",
                    "Day Change %"
                ],
                ascending=[
                    False,
                    False,
                    False
                ]
            )
            .drop_duplicates(
                subset=["Ticker"],
                keep="first"
            )
            .reset_index(
                drop=True
            )
        )

    else:

        combined = pd.DataFrame()

    # --------------------------------------------------------
    # DIAGNOSTICS
    # --------------------------------------------------------

    print(
        "\n"
        + "-" * 88
    )

    print(
        "INDIA OFFICIAL STAGE-1 DIAGNOSTICS"
    )

    print(
        "-" * 88
    )

    for diagnostics in [
        nse_diagnostics,
        bse_diagnostics
    ]:

        if not diagnostics:
            continue

        print(
            f"\nSource: "
            f"{diagnostics['source']}"
        )

        print(
            f"  Target session:            "
            f"{diagnostics['target_date']}"
        )

        print(
            f"  Previous session:          "
            f"{diagnostics['previous_date']}"
        )

        print(
            f"  Securities on target day: "
            f"{diagnostics['target_securities']:,}"
        )

        print(
            f"  Full 20-day histories:     "
            f"{diagnostics['full_20day_history']:,}"
        )

        print(
            f"  Passed Day Change >= "
            f"{MIN_DAY_CHANGE:.0f}%: "
            f"{diagnostics['pass_change']:,}"
        )

        print(
            f"  Passed RVOL >= "
            f"{MIN_RVOL:.1f}x: "
            f"{diagnostics['pass_rvol']:,}"
        )

        print(
            f"  Passed Price rule:         "
            f"{diagnostics['pass_price']:,}"
        )

        print(
            f"  Passed at least 2/3:       "
            f"{diagnostics['survivors']:,}"
        )

    print(
        "\nCombined India Stage-1 survivors: "
        f"{len(combined):,}"
    )

    print(
        "-" * 88
        + "\n"
    )

    return combined


# ============================================================
# SECTION 16: STAGE 1 - FAST PRICE / VOLUME SCREEN
# ============================================================

def run_stage_one(
    symbols,
    market,
    target_session_date,
    previous_session_date
):
    """
    VERSION 8 Stage 1.

    INDIA:
        uses official NSE/BSE bhavcopies.

    US/CANADA:
        keeps the yfinance batch engine.
    """

    if market == "INDIA":

        return run_india_official_stage_one(
            symbols=symbols,
            target_session_date=target_session_date,
            previous_session_date=previous_session_date
        )

    """
    VERSION 7-style yfinance Stage 1 for US/CANADA.

    Checks:
        1. Day Change
        2. RVOL
        4. Price

    A stock must pass at least TWO of the THREE to continue.

    Version 7 also records diagnostic counts so a zero-survivor result
    cannot happen silently.
    """

    config = MARKET_CONFIG[
        market
    ]

    survivors = []

    # --------------------------------------------------------
    # DIAGNOSTIC COUNTERS
    # --------------------------------------------------------

    diagnostics = {
        "symbols_requested": len(symbols),
        "histories_extracted": 0,
        "target_missing": 0,
        "previous_missing": 0,
        "insufficient_history": 0,
        "evaluated": 0,
        "pass_change": 0,
        "pass_rvol": 0,
        "pass_price": 0,
        "pass_two_of_three": 0,
        "data_errors": 0,
    }

    total_batches = (
        len(symbols)
        + BATCH_SIZE
        - 1
    ) // BATCH_SIZE

    print(
        "STAGE 1: Checking Price, Day Change, "
        "RVOL and liquidity..."
    )

    for batch_no, start in enumerate(
        range(
            0,
            len(symbols),
            BATCH_SIZE
        ),
        start=1
    ):

        batch_symbols = symbols[
            start:start + BATCH_SIZE
        ]

        print(
            f"  Batch "
            f"{batch_no}/{total_batches} "
            f"({len(batch_symbols)} stocks)"
        )

        # ----------------------------------------------------
        # PRIMARY BATCH DOWNLOAD
        # ----------------------------------------------------

        try:

            batch_data = safe_yf_download(
                tickers=batch_symbols,
                period=PRICE_HISTORY_PERIOD,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
                multi_level_index=True,

                # Explicit for daily multi-market consistency.
                ignore_tz=True
            )

        except Exception as error:

            print(
                f"  Batch download error: "
                f"{error}"
            )

            continue

        for symbol in batch_symbols:

            history = extract_ticker_history(
                batch_data,
                symbol
            )

            if history is None:

                diagnostics[
                    "data_errors"
                ] += 1

                continue

            diagnostics[
                "histories_extracted"
            ] += 1

            session_rows = (
                get_rows_for_target_session(
                    history,
                    target_session_date,
                    previous_session_date
                )
            )

            if session_rows is None:

                diagnostics[
                    "data_errors"
                ] += 1

                continue

            status = session_rows[
                "status"
            ]

            if status == "TARGET_MISSING":

                diagnostics[
                    "target_missing"
                ] += 1

                continue

            if status == "PREVIOUS_MISSING":

                diagnostics[
                    "previous_missing"
                ] += 1

                continue

            if status == "INSUFFICIENT_HISTORY":

                diagnostics[
                    "insufficient_history"
                ] += 1

                continue

            current_day = session_rows[
                "current_day"
            ]

            previous_day = session_rows[
                "previous_day"
            ]

            prior_20 = session_rows[
                "prior_20"
            ]

            try:

                current_price = scalar_from_row(
                    current_day,
                    "Close"
                )

                previous_close = scalar_from_row(
                    previous_day,
                    "Close"
                )

                current_volume = scalar_from_row(
                    current_day,
                    "Volume"
                )

            except Exception:

                diagnostics[
                    "data_errors"
                ] += 1

                continue

            if previous_close <= 0:
                continue

            # ------------------------------------------------
            # CRITERION 1: DAY CHANGE
            # ------------------------------------------------

            day_change = (
                (
                    current_price
                    - previous_close
                )
                / previous_close
            ) * 100

            # ------------------------------------------------
            # CRITERION 2: RVOL
            # ------------------------------------------------

            try:

                volume_series = pd.to_numeric(
                    prior_20["Volume"],
                    errors="coerce"
                )

                average_volume = float(
                    volume_series.mean()
                )

            except Exception:

                diagnostics[
                    "data_errors"
                ] += 1

                continue

            if average_volume > 0:

                rvol = (
                    current_volume
                    / average_volume
                )

            else:

                rvol = 0.0

            # ------------------------------------------------
            # LIQUIDITY / TURNOVER
            # ------------------------------------------------

            current_turnover = (
                current_price
                * current_volume
            )

            try:

                close_series = pd.to_numeric(
                    prior_20["Close"],
                    errors="coerce"
                )

                volume_series = pd.to_numeric(
                    prior_20["Volume"],
                    errors="coerce"
                )

                average_turnover = float(
                    (
                        close_series
                        * volume_series
                    ).mean()
                )

            except Exception:

                average_turnover = 0.0

            if (
                average_volume
                >= MIN_AVG_VOLUME_FOR_GOOD_RVOL
            ):

                rvol_quality = "GOOD"

            else:

                rvol_quality = "LOW BASE"

            if (
                current_turnover
                >= config[
                    "min_turnover_for_good_liquidity"
                ]
            ):

                liquidity_quality = "GOOD"

            else:

                liquidity_quality = "LOW TURNOVER"

            # ------------------------------------------------
            # CRITERION 4: PRICE
            # ------------------------------------------------

            passes_change = (
                day_change
                >= MIN_DAY_CHANGE
            )

            passes_rvol = (
                rvol
                >= MIN_RVOL
            )

            passes_price = (
                config["min_price"]
                <= current_price
                <= config["max_price"]
            )

            diagnostics[
                "evaluated"
            ] += 1

            if passes_change:

                diagnostics[
                    "pass_change"
                ] += 1

            if passes_rvol:

                diagnostics[
                    "pass_rvol"
                ] += 1

            if passes_price:

                diagnostics[
                    "pass_price"
                ] += 1

            stage_one_score = sum(
                [
                    passes_change,
                    passes_rvol,
                    passes_price
                ]
            )

            if stage_one_score < 2:
                continue

            diagnostics[
                "pass_two_of_three"
            ] += 1

            survivors.append(
                {
                    "Ticker": symbol,
                    "Market": market,
                    "Session Date": target_session_date,
                    "Previous Session Date": previous_session_date,

                    "Price": current_price,
                    "Day Change %": day_change,
                    "RVOL": rvol,

                    "Volume": current_volume,
                    "20D Avg Volume": average_volume,

                    "Current Turnover": current_turnover,
                    "20D Avg Turnover": average_turnover,

                    "RVOL Quality": rvol_quality,
                    "Liquidity Quality": liquidity_quality,

                    "Change Pass": passes_change,
                    "RVOL Pass": passes_rvol,
                    "Price Pass": passes_price,

                    "Stage 1 Score": stage_one_score,
                }
            )

    dataframe = pd.DataFrame(
        survivors
    )

    # --------------------------------------------------------
    # PRINT DIAGNOSTIC SUMMARY
    # --------------------------------------------------------

    print(
        "\n"
        + "-" * 78
    )

    print(
        "STAGE 1 DIAGNOSTICS"
    )

    print(
        "-" * 78
    )

    print(
        f"Symbols requested:        "
        f"{diagnostics['symbols_requested']:,}"
    )

    print(
        f"Histories extracted:      "
        f"{diagnostics['histories_extracted']:,}"
    )

    print(
        f"Target session missing:   "
        f"{diagnostics['target_missing']:,}"
    )

    print(
        f"Previous session missing: "
        f"{diagnostics['previous_missing']:,}"
    )

    print(
        f"Insufficient history:     "
        f"{diagnostics['insufficient_history']:,}"
    )

    print(
        f"Other data/extract errors:"
        f" {diagnostics['data_errors']:,}"
    )

    print(
        f"Stocks actually evaluated:"
        f" {diagnostics['evaluated']:,}"
    )

    print(
        f"Passed Day Change >= "
        f"{MIN_DAY_CHANGE:.0f}%: "
        f"{diagnostics['pass_change']:,}"
    )

    print(
        f"Passed RVOL >= "
        f"{MIN_RVOL:.1f}x: "
        f"{diagnostics['pass_rvol']:,}"
    )

    print(
        f"Passed market Price rule: "
        f"{diagnostics['pass_price']:,}"
    )

    print(
        f"Passed at least 2/3:      "
        f"{diagnostics['pass_two_of_three']:,}"
    )

    print(
        "-" * 78
    )

    print(
        f"\nStage 1 survivors: "
        f"{len(dataframe):,}\n"
    )

    return dataframe


def run_india_zero_survivor_diagnostic(
    target_session_date,
    previous_session_date
):
    """
    If India still returns zero candidates, independently download a
    few liquid NSE stocks one-by-one and print their most recent dates.

    This tells us whether the remaining problem is:
        - Yahoo bulk batching
        - Yahoo session freshness
        - ticker extraction
        - or simply the filter itself
    """

    test_symbols = [
        "RELIANCE.NS",
        "HDFCBANK.NS",
        "TCS.NS",
        "SBIN.NS",
    ]

    print(
        "\n"
        + "-" * 78
    )

    print(
        "INDIA SINGLE-TICKER DATE CHECK"
    )

    print(
        "-" * 78
    )

    for symbol in test_symbols:

        try:

            test_data = safe_yf_download(
                symbol,
                period="1mo",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                ignore_tz=True,
                multi_level_index=False
            )

            if (
                test_data is None
                or test_data.empty
            ):

                print(
                    f"{symbol}: NO DATA"
                )

                continue

            dates = [
                pd.Timestamp(value).date()
                for value in test_data.index
            ]

            recent_dates = dates[-5:]

            print(
                f"{symbol}: "
                f"recent dates = {recent_dates}"
            )

            print(
                f"    target {target_session_date} present = "
                f"{target_session_date in dates}"
            )

            print(
                f"    previous {previous_session_date} present = "
                f"{previous_session_date in dates}"
            )

        except Exception as error:

            print(
                f"{symbol}: diagnostic error = {error}"
            )

    print(
        "-" * 78
    )

# ============================================================
# SECTION 17: COMPANY-NAME ALIASES FOR NEWS MATCHING
# ============================================================

COMMON_COMPANY_WORDS = {
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "company",
    "co",
    "limited",
    "ltd",
    "plc",
    "holdings",
    "holding",
    "group",
    "sa",
    "ag",
    "nv",
    "se",
    "the",
}


NEWS_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by",
    "for", "from", "has", "have", "in", "into", "is",
    "it", "its", "of", "on", "or", "that", "the", "this",
    "to", "with", "stock", "stocks", "shares", "share",
    "why", "could", "may", "after", "before", "today", "new",
}


def meaningful_company_aliases(
    info
):
    """
    Build usable company-name phrases from Yahoo company info.
    """

    aliases = set()

    for key in [
        "longName",
        "shortName"
    ]:

        company_name = info.get(
            key
        )

        normalized = normalize_text(
            company_name
        )

        if not normalized:
            continue

        aliases.add(
            normalized
        )

        cleaned_words = [
            word
            for word in normalized.split()
            if word not in COMMON_COMPANY_WORDS
        ]

        cleaned = " ".join(
            cleaned_words
        ).strip()

        if cleaned:
            aliases.add(
                cleaned
            )

    return sorted(
        aliases,
        key=len,
        reverse=True
    )


def base_ticker_for_news(
    yahoo_symbol
):
    """
    Remove Yahoo exchange suffix before matching ticker text.

    Examples:
        AAPL       -> AAPL
        SHOP.TO    -> SHOP
        RELIANCE.NS -> RELIANCE
        500325.BO  -> 500325
    """

    symbol = str(
        yahoo_symbol
    ).upper()

    for suffix in [
        ".TO",
        ".V",
        ".NS",
        ".BO"
    ]:

        if symbol.endswith(
            suffix
        ):
            symbol = symbol[
                :-len(suffix)
            ]
            break

    return symbol.replace(
        "-",
        "."
    )


def contains_ticker(
    text,
    yahoo_symbol
):
    """
    Match ticker as a complete word.

    One-character tickers are ignored because they cause many
    accidental text matches.
    """

    ticker = base_ticker_for_news(
        yahoo_symbol
    )

    if len(ticker) < 2:
        return False

    return bool(
        re.search(
            rf"\b{re.escape(ticker)}\b",
            str(text).upper()
        )
    )


# ============================================================
# SECTION 18: CATALYST TYPES
# ============================================================

CATALYST_PATTERNS = {

    "EARNINGS / RESULTS / GUIDANCE": [
        r"\bearnings\b",
        r"\bquarterly results\b",
        r"\bfinancial results\b",
        r"\bguidance\b",
        r"\bforecast\b",
        r"\brevenue\b",
        r"\beps\b",
        r"\bprofit\b",
        r"\bsales\b",
    ],

    "CONTRACT / ORDER": [
        r"\bcontract\b",
        r"\bcontracts\b",
        r"\border\b",
        r"\borders\b",
        r"\baward\b",
        r"\bawarded\b",
    ],

    "M&A / STRATEGIC DEAL": [
        r"\bacquisition\b",
        r"\bacquire\b",
        r"\bacquires\b",
        r"\bmerger\b",
        r"\bbuyout\b",
        r"\bstrategic transaction\b",
    ],

    "FDA / REGULATORY / CLINICAL": [
        r"\bfda\b",
        r"\bapproval\b",
        r"\bapproved\b",
        r"\bregulatory\b",
        r"\bclinical trial\b",
        r"\btrial results\b",
        r"\bphase 1\b",
        r"\bphase 2\b",
        r"\bphase 3\b",
        r"\bdesignation\b",
    ],

    "PARTNERSHIP / AGREEMENT": [
        r"\bpartnership\b",
        r"\bpartner\b",
        r"\bagreement\b",
        r"\bcollaboration\b",
        r"\blicensing\b",
        r"\blicense agreement\b",
        r"\bdistribution agreement\b",
        r"\bjoint venture\b",
    ],

    "PRODUCT / LAUNCH": [
        r"\blaunch\b",
        r"\blaunched\b",
        r"\bproduct launch\b",
        r"\bcommercial launch\b",
        r"\bnew product\b",
    ],

    "CAPITAL RETURN": [
        r"\bbuyback\b",
        r"\brepurchase\b",
        r"\bdividend\b",
        r"\bspecial dividend\b",
    ],

    "FINANCING / CAPITAL": [
        r"\boffering\b",
        r"\bfinancing\b",
        r"\bprivate placement\b",
        r"\bpublic offering\b",
        r"\bdebt financing\b",
        r"\bfund raise\b",
        r"\bfundraise\b",
    ],

    "ANALYST ACTION": [
        r"\bupgrade\b",
        r"\bupgraded\b",
        r"\bdowngrade\b",
        r"\bdowngraded\b",
        r"\bprice target\b",
        r"\binitiates coverage\b",
        r"\binitiated coverage\b",
    ],

    "CORPORATE / FILING": [
        r"\bfiling\b",
        r"\bsec filing\b",
        r"\b8-k\b",
        r"\bmanagement change\b",
        r"\bceo\b",
        r"\bcfo\b",
        r"\brestructuring\b",
        r"\bboard meeting\b",
    ],
}


NO_CATALYST_PATTERNS = [
    r"\bno new company news\b",
    r"\bno company specific news\b",
    r"\bno company-specific news\b",
    r"\bno new company announcement\b",
    r"\bno apparent catalyst\b",
    r"\bno obvious catalyst\b",
    r"\bno clear catalyst\b",
    r"\bwithout a clear catalyst\b",
]


PRICE_ACTION_PATTERNS = [
    r"\bstock skyrockets\b",
    r"\bshares skyrocket\b",
    r"\bstock soars\b",
    r"\bshares soar\b",
    r"\bstock surges\b",
    r"\bshares surge\b",
    r"\bstock jumps\b",
    r"\bshares jump\b",
    r"\bstock rallies\b",
    r"\bshares rally\b",
    r"\bwhat s driving\b",
    r"\bwhat is driving\b",
    r"\bwhy .* stock is up\b",
    r"\bwhy .* stock is soaring\b",
]


def detect_catalyst(
    headline,
    summary
):
    """
    Identify an actual event, rather than merely an article
    describing a stock-price move.
    """

    headline_text = normalize_text(
        headline
    )

    summary_text = normalize_text(
        summary
    )

    combined = (
        headline_text
        + " "
        + summary_text
    )

    for pattern in NO_CATALYST_PATTERNS:

        if re.search(
            pattern,
            combined
        ):

            return (
                None,
                "Article explicitly says no clear catalyst",
                True
            )

    price_action_headline = any(
        re.search(
            pattern,
            headline_text
        )
        for pattern in PRICE_ACTION_PATTERNS
    )

    for catalyst_type, patterns in (
        CATALYST_PATTERNS.items()
    ):

        for pattern in patterns:

            match = re.search(
                pattern,
                combined
            )

            if match:

                return (
                    catalyst_type,
                    match.group(0),
                    price_action_headline
                )

    return (
        None,
        None,
        price_action_headline
    )


# ============================================================
# SECTION 19: PARSE YAHOO NEWS
# ============================================================

def parse_yahoo_news_item(
    item
):
    """
    Yahoo's returned news structure can change, so normalize it.
    """

    content = (
        item.get(
            "content",
            {}
        )
        or {}
    )

    headline = (
        item.get("title")
        or content.get("title")
    )

    summary = (
        item.get("summary")
        or content.get("summary")
        or content.get("description")
        or ""
    )

    provider = (
        content.get(
            "provider",
            {}
        )
        or {}
    )

    source = (
        item.get("publisher")
        or provider.get(
            "displayName"
        )
    )

    published = None

    timestamp = item.get(
        "providerPublishTime"
    )

    if timestamp:

        try:

            published = datetime.fromtimestamp(
                timestamp,
                tz=timezone.utc
            )

        except Exception:
            pass

    if published is None:

        date_string = content.get(
            "pubDate"
        )

        if date_string:

            try:

                published = (
                    datetime
                    .fromisoformat(
                        date_string.replace(
                            "Z",
                            "+00:00"
                        )
                    )
                )

            except Exception:
                pass

    url = item.get(
        "link"
    )

    if not url:

        click_url = content.get(
            "clickThroughUrl"
        )

        if isinstance(
            click_url,
            dict
        ):

            url = click_url.get(
                "url"
            )

    if not url:

        canonical_url = content.get(
            "canonicalUrl"
        )

        if isinstance(
            canonical_url,
            dict
        ):

            url = canonical_url.get(
                "url"
            )

    return {
        "headline": headline,
        "summary": summary,
        "source": source,
        "published": published,
        "url": url,
    }


# ============================================================
# SECTION 20: CLASSIFY NEWS RELEVANCE
# ============================================================

def classify_news_article(
    yahoo_symbol,
    headline,
    summary,
    aliases
):
    """
    To pass News, we want BOTH:

        1. direct relevance to the company/ticker
        2. an identifiable company event/catalyst
    """

    normalized_headline = normalize_text(
        headline
    )

    normalized_summary = normalize_text(
        summary
    )

    company_in_headline = any(
        alias in normalized_headline
        for alias in aliases
        if len(alias) >= 3
    )

    ticker_in_headline = contains_ticker(
        headline,
        yahoo_symbol
    )

    company_in_summary = any(
        alias in normalized_summary
        for alias in aliases
        if len(alias) >= 3
    )

    ticker_in_summary = contains_ticker(
        summary,
        yahoo_symbol
    )

    (
        catalyst_type,
        catalyst_evidence,
        price_action_headline

    ) = detect_catalyst(
        headline,
        summary
    )

    direct = (
        company_in_headline
        or ticker_in_headline
    )

    related = (
        company_in_summary
        or ticker_in_summary
    )

    if (
        direct
        and catalyst_type is not None
    ):

        return {
            "relevance": "DIRECT CATALYST",
            "catalyst_type": catalyst_type,
            "catalyst_evidence": catalyst_evidence,
            "price_action_headline": price_action_headline,
            "passes_direct_news": True,
        }

    if direct:

        return {
            "relevance": (
                "DIRECT ARTICLE - "
                "NO IDENTIFIED CATALYST"
            ),
            "catalyst_type": "UNIDENTIFIED",
            "catalyst_evidence": None,
            "price_action_headline": price_action_headline,
            "passes_direct_news": False,
        }

    if related:

        return {
            "relevance": "RELATED / MENTION",
            "catalyst_type": (
                catalyst_type
                if catalyst_type is not None
                else "UNIDENTIFIED"
            ),
            "catalyst_evidence": catalyst_evidence,
            "price_action_headline": price_action_headline,
            "passes_direct_news": False,
        }

    return {
        "relevance": "SECTOR / UNVERIFIED",
        "catalyst_type": (
            catalyst_type
            if catalyst_type is not None
            else "UNIDENTIFIED"
        ),
        "catalyst_evidence": catalyst_evidence,
        "price_action_headline": price_action_headline,
        "passes_direct_news": False,
    }


# ============================================================
# SECTION 21: NEWS FRESHNESS / REPEAT CHECK
# ============================================================

def headline_token_set(text):
    """
    Convert headline into meaningful words.
    """

    normalized = normalize_text(
        text
    )

    return {
        token
        for token in normalized.split()
        if (
            token not in NEWS_STOPWORDS
            and len(token) >= 3
        )
    }


def token_jaccard_similarity(
    text_a,
    text_b
):
    """
    Compare overlap between two headline word sets.
    """

    set_a = headline_token_set(
        text_a
    )

    set_b = headline_token_set(
        text_b
    )

    if not set_a or not set_b:
        return 0.0

    return (
        len(set_a & set_b)
        / len(set_a | set_b)
    )


def articles_look_similar(
    headline_a,
    headline_b
):
    """
    Flag possible repeated/follow-up stories using two methods.
    """

    sequence_score = SequenceMatcher(
        None,
        normalize_text(headline_a),
        normalize_text(headline_b)
    ).ratio()

    jaccard_score = (
        token_jaccard_similarity(
            headline_a,
            headline_b
        )
    )

    return (
        sequence_score
        >= NEWS_SIMILARITY_THRESHOLD

        or

        jaccard_score >= 0.50
    )



# ============================================================
# VERSION 9: OFFICIAL INDIA CORPORATE-ANNOUNCEMENT ENGINE
# ============================================================

# Reuse HTTP sessions so we do not create a brand-new TCP/cookie
# session for every Stage-2 survivor.
_NSE_ANNOUNCEMENT_SESSION = None
_BSE_ANNOUNCEMENT_SESSION = None


def get_nse_announcement_session(
    force_refresh=False
):
    """
    NSE normally expects browser-like headers and session cookies.

    We first visit the official announcement page, then use the same
    requests.Session for the JSON API.
    """

    global _NSE_ANNOUNCEMENT_SESSION

    if (
        _NSE_ANNOUNCEMENT_SESSION is not None
        and not force_refresh
    ):
        return _NSE_ANNOUNCEMENT_SESSION

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_ANNOUNCEMENT_PAGE,
            "Connection": "keep-alive",
        }
    )

    # Cookie warm-up.
    try:
        session.get(
            NSE_ANNOUNCEMENT_PAGE,
            timeout=20
        )
    except Exception:
        # The API itself may still work, so do not fail here.
        pass

    _NSE_ANNOUNCEMENT_SESSION = session

    return session


def get_bse_announcement_session():
    """
    Create/reuse a BSE API session with the exchange's expected
    Referer and browser-like headers.
    """

    global _BSE_ANNOUNCEMENT_SESSION

    if _BSE_ANNOUNCEMENT_SESSION is not None:
        return _BSE_ANNOUNCEMENT_SESSION

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bseindia.com/corporates/ann.html",
            "Connection": "keep-alive",
        }
    )

    _BSE_ANNOUNCEMENT_SESSION = session

    return session


def parse_india_exchange_datetime(
    value
):
    """
    Convert NSE/BSE announcement timestamps into timezone-aware
    India Standard Time.

    Exchange APIs use several date formats, so pandas is used as the
    tolerant parser.
    """

    if value is None:
        return None

    try:

        timestamp = pd.to_datetime(
            value,
            dayfirst=True,
            errors="coerce"
        )

        if pd.isna(timestamp):
            return None

        timestamp = pd.Timestamp(
            timestamp
        )

        india_tz = MARKET_CONFIG[
            "INDIA"
        ]["timezone"]

        if timestamp.tzinfo is None:

            timestamp = timestamp.tz_localize(
                india_tz
            )

        else:

            timestamp = timestamp.tz_convert(
                india_tz
            )

        return timestamp.to_pydatetime()

    except Exception:

        return None


def normalize_nse_announcement(
    item
):
    """
    Convert one raw NSE announcement dictionary into the same
    article-like structure used by the rest of the screener.
    """

    subject = (
        item.get("desc")
        or item.get("subject")
        or "Corporate Announcement"
    )

    details = (
        item.get("attchmntText")
        or item.get("details")
        or item.get("sm_name")
        or ""
    )

    published = None

    for key in [
        "an_dt",
        "sort_date",
        "dt",
        "exchdisstime"
    ]:

        published = parse_india_exchange_datetime(
            item.get(key)
        )

        if published is not None:
            break

    attachment = (
        item.get("attchmntFile")
        or item.get("attachment")
    )

    if (
        attachment
        and not str(attachment).lower().startswith(
            ("http://", "https://")
        )
    ):

        attachment = (
            "https://nsearchives.nseindia.com/corporate/"
            + str(attachment).lstrip("/")
        )

    if not attachment:

        symbol = str(
            item.get("symbol", "")
        ).strip().upper()

        attachment = (
            NSE_ANNOUNCEMENT_PAGE
            + (
                f"?symbol={symbol}&tabIndex=equity"
                if symbol
                else ""
            )
        )

    return {
        "headline": str(subject),
        "summary": str(details),
        "source": "NSE Corporate Announcement",
        "published": published,
        "url": attachment,
        "exchange": "NSE",
    }


def fetch_nse_official_announcements(
    yahoo_symbol,
    from_date,
    to_date
):
    """
    Fetch official NSE equity announcements for ONE NSE symbol.

    Yahoo:
        RELIANCE.NS

    NSE API symbol:
        RELIANCE
    """

    nse_symbol = str(
        yahoo_symbol
    ).upper()

    if nse_symbol.endswith(
        ".NS"
    ):
        nse_symbol = nse_symbol[:-3]

    params = {
        "index": "equities",
        "symbol": nse_symbol,
        "from_date": from_date.strftime(
            "%d-%m-%Y"
        ),
        "to_date": to_date.strftime(
            "%d-%m-%Y"
        ),
    }

    last_error = None

    for attempt in range(3):

        try:

            session = get_nse_announcement_session(
                force_refresh=(
                    attempt > 0
                )
            )

            response = session.get(
                NSE_ANNOUNCEMENT_API,
                params=params,
                timeout=25
            )

            # Refresh cookies if NSE blocks/invalidates the session.
            if response.status_code in {
                401,
                403
            }:

                last_error = RuntimeError(
                    f"NSE HTTP {response.status_code}"
                )

                pytime.sleep(
                    EXCHANGE_REQUEST_PAUSE_SECONDS
                )

                continue

            response.raise_for_status()

            data = response.json()

            if isinstance(
                data,
                list
            ):

                records = data

            elif isinstance(
                data,
                dict
            ):

                # Some wrappers/endpoints return {"data": [...]}
                records = (
                    data.get("data")
                    or data.get("Data")
                    or []
                )

                if not isinstance(
                    records,
                    list
                ):
                    records = []

            else:

                records = []

            announcements = []

            for item in records:

                if not isinstance(
                    item,
                    dict
                ):
                    continue

                item_symbol = str(
                    item.get(
                        "symbol",
                        nse_symbol
                    )
                ).strip().upper()

                # Keep only this company.
                if (
                    item_symbol
                    and item_symbol != nse_symbol
                ):
                    continue

                normalized = normalize_nse_announcement(
                    item
                )

                if normalized[
                    "published"
                ] is not None:

                    announcements.append(
                        normalized
                    )

            announcements.sort(
                key=lambda article:
                    article["published"],
                reverse=True
            )

            return announcements

        except Exception as error:

            last_error = error

            pytime.sleep(
                EXCHANGE_REQUEST_PAUSE_SECONDS
            )

    raise RuntimeError(
        "NSE announcement fetch failed: "
        f"{last_error}"
    )


def normalize_bse_announcement(
    item
):
    """
    Normalize one BSE announcement row.
    """

    subject = (
        item.get("NEWSSUB")
        or item.get("HEADLINE")
        or item.get("CATEGORYNAME")
        or "Corporate Announcement"
    )

    details_parts = [
        item.get("HEADLINE"),
        item.get("CATEGORYNAME"),
        item.get("MORE"),
        item.get("SLONGNAME"),
    ]

    details = " | ".join(
        str(value)
        for value in details_parts
        if value not in {
            None,
            "",
        }
    )

    published = None

    for key in [
        "DissemDT",
        "News_submission_dt",
        "NEWS_DT",
        "DT_TM",
        "NEWS_SUBMISSION_DT",
    ]:

        published = parse_india_exchange_datetime(
            item.get(key)
        )

        if published is not None:
            break

    attachment = (
        item.get("ATTACHMENTNAME")
        or item.get("AttachmentName")
    )

    if attachment:

        attachment = str(
            attachment
        ).strip()

        if not attachment.lower().startswith(
            ("http://", "https://")
        ):

            attachment = (
                BSE_ATTACHMENT_BASE
                + attachment.lstrip("/")
            )

    else:

        attachment = (
            "https://www.bseindia.com/corporates/ann.html"
        )

    return {
        "headline": str(subject),
        "summary": str(details),
        "source": "BSE Corporate Announcement",
        "published": published,
        "url": attachment,
        "exchange": "BSE",
    }


def fetch_bse_official_announcements(
    yahoo_symbol,
    from_date,
    to_date
):
    """
    Fetch official BSE announcements for ONE BSE scrip code.

    Yahoo:
        543916.BO

    BSE scrip code:
        543916
    """

    bse_code = str(
        yahoo_symbol
    ).upper()

    if bse_code.endswith(
        ".BO"
    ):
        bse_code = bse_code[:-3]

    if not re.fullmatch(
        r"\d{6}",
        bse_code
    ):

        raise RuntimeError(
            f"Cannot derive BSE scrip code from {yahoo_symbol}"
        )

    session = get_bse_announcement_session()

    announcements = []

    for page_no in range(
        1,
        MAX_BSE_ANNOUNCEMENT_PAGES + 1
    ):

        params = {
            "pageno": page_no,
            "strCat": "-1",
            "subcategory": "-1",
            "strPrevDate": from_date.strftime(
                "%Y%m%d"
            ),
            "strToDate": to_date.strftime(
                "%Y%m%d"
            ),
            "strSearch": "P",
            "strscrip": bse_code,
            "strType": "C",
        }

        response = session.get(
            BSE_ANNOUNCEMENT_API,
            params=params,
            timeout=25
        )

        response.raise_for_status()

        data = response.json()

        records = (
            data.get("Table", [])
            if isinstance(
                data,
                dict
            )
            else []
        )

        if not records:
            break

        for item in records:

            if not isinstance(
                item,
                dict
            ):
                continue

            normalized = normalize_bse_announcement(
                item
            )

            if normalized[
                "published"
            ] is not None:

                announcements.append(
                    normalized
                )

        # If the page contains fewer than the normal page size,
        # there is usually nothing more to fetch.
        if len(records) < 50:
            break

        pytime.sleep(
            EXCHANGE_REQUEST_PAUSE_SECONDS
        )

    announcements.sort(
        key=lambda article:
            article["published"],
        reverse=True
    )

    return announcements


# ------------------------------------------------------------
# Material vs routine official filings
# ------------------------------------------------------------

INDIA_ROUTINE_ANNOUNCEMENT_PATTERNS = [
    r"\bcopy of newspaper publication\b",
    r"\banalysts institutional investor meet\b",
    r"\binvestor meet\b",
    r"\bconference call\b",
    r"\bshareholding pattern\b",
    r"\bloss of share certificate\b",
    r"\bduplicate share certificate\b",
    r"\bclosure of trading window\b",
    r"\bsecretarial compliance\b",
    r"\bcompliance certificate\b",
    r"\bvoting results\b",
    r"\bscrutinizer\b",
    r"\bpostal ballot\b",
    r"\bannual report\b",
    r"\bnotice of agm\b",
    r"\bnotice of egm\b",
    r"\bspurt in volume\b",
    r"\bclarification sought\b",
]


INDIA_MATERIAL_CATALYST_PATTERNS = {

    "EARNINGS / RESULTS / GUIDANCE": [
        r"\bfinancial results\b",
        r"\bquarterly results\b",
        r"\bunaudited results\b",
        r"\baudited results\b",
        r"\bearnings\b",
        r"\bguidance\b",
        r"\brevenue\b",
        r"\bprofit\b",
        r"\bsales\b",
    ],

    "ORDER / CONTRACT / TENDER": [
        r"\baward of order\b",
        r"\breceipt of order\b",
        r"\bwork order\b",
        r"\bpurchase order\b",
        r"\bletter of award\b",
        r"\bcontract\b",
        r"\border received\b",
        r"\border win\b",
    ],

    "M&A / INVESTMENT / RESTRUCTURING": [
        r"\bacquisition\b",
        r"\bmerger\b",
        r"\bamalgamation\b",
        r"\bdemerger\b",
        r"\bscheme of arrangement\b",
        r"\bstake acquisition\b",
        r"\bdivestment\b",
        r"\bsale of subsidiary\b",
    ],

    "FUND RAISING / CAPITAL ISSUE": [
        r"\bfund raising\b",
        r"\bfundraise\b",
        r"\bqip\b",
        r"\bqualified institutional placement\b",
        r"\bpreferential issue\b",
        r"\bright issue\b",
        r"\brights issue\b",
        r"\bprivate placement\b",
        r"\bissue of shares\b",
    ],

    "CORPORATE ACTION": [
        r"\bbonus issue\b",
        r"\bbonus shares\b",
        r"\bstock split\b",
        r"\bsub division\b",
        r"\bsubdivision\b",
        r"\bbuyback\b",
        r"\bspecial dividend\b",
        r"\bdividend\b",
    ],

    "BUSINESS / PARTNERSHIP / EXPANSION": [
        r"\bpartnership\b",
        r"\bcollaboration\b",
        r"\bjoint venture\b",
        r"\blicensing\b",
        r"\bdistribution agreement\b",
        r"\bcapacity expansion\b",
        r"\bcommercial production\b",
        r"\bcommissioning\b",
        r"\bnew facility\b",
        r"\bnew plant\b",
        r"\bproduct launch\b",
    ],

    "REGULATORY / APPROVAL / CLINICAL": [
        r"\busfda\b",
        r"\bfda\b",
        r"\bcdsco\b",
        r"\bregulatory approval\b",
        r"\bapproval received\b",
        r"\bpatent\b",
        r"\bclinical trial\b",
        r"\bphase 1\b",
        r"\bphase 2\b",
        r"\bphase 3\b",
    ],

    "CREDIT / RATING": [
        r"\bcredit rating\b",
        r"\brating upgrade\b",
        r"\brating downgrade\b",
    ],

    "KEY MANAGEMENT / MATERIAL EVENT": [
        r"\bappointment of.*(ceo|cfo|managing director|whole time director)\b",
        r"\bresignation of.*(ceo|cfo|managing director|whole time director)\b",
        r"\bmaterial event\b",
    ],
}


def classify_official_india_announcement(
    article
):
    """
    Decide whether an NSE/BSE filing is a plausible material catalyst.

    Official exchange filing != automatically market-moving.

    Routine compliance filings are rejected unless the text contains a
    stronger material-event pattern.
    """

    headline = str(
        article.get(
            "headline",
            ""
        )
    )

    summary = str(
        article.get(
            "summary",
            ""
        )
    )

    combined = normalize_text(
        headline
        + " "
        + summary
    )

    # First search for a material event.
    for catalyst_type, patterns in (
        INDIA_MATERIAL_CATALYST_PATTERNS.items()
    ):

        for pattern in patterns:

            match = re.search(
                pattern,
                combined
            )

            if match:

                return {
                    "passes": True,
                    "catalyst_type": catalyst_type,
                    "evidence": match.group(0),
                    "relevance": (
                        "DIRECT OFFICIAL EXCHANGE CATALYST"
                    ),
                }

    # No material event found. Reject obvious routine filings.
    for pattern in INDIA_ROUTINE_ANNOUNCEMENT_PATTERNS:

        if re.search(
            pattern,
            combined
        ):

            return {
                "passes": False,
                "catalyst_type": "ROUTINE / NON-CATALYST FILING",
                "evidence": None,
                "relevance": (
                    "OFFICIAL ROUTINE FILING - NOT A CATALYST"
                ),
            }

    # Last fallback: reuse the general catalyst detector, but do not
    # allow generic "board meeting" / "filing" wording alone to pass.
    (
        catalyst_type,
        evidence,
        _
    ) = detect_catalyst(
        headline,
        summary
    )

    if (
        catalyst_type is not None
        and not (
            catalyst_type == "CORPORATE / FILING"
            and str(evidence).lower() in {
                "board meeting",
                "filing",
            }
        )
    ):

        return {
            "passes": True,
            "catalyst_type": catalyst_type,
            "evidence": evidence,
            "relevance": (
                "DIRECT OFFICIAL EXCHANGE CATALYST"
            ),
        }

    return {
        "passes": False,
        "catalyst_type": "UNIDENTIFIED",
        "evidence": None,
        "relevance": (
            "OFFICIAL FILING - NO IDENTIFIED MATERIAL CATALYST"
        ),
    }


def official_announcements_look_similar(
    current_article,
    older_article
):
    """
    Compare both subject and the first part of the details.

    Exchange subjects such as "General Updates" are often generic, so
    using some detail text produces a better repeat/follow-up check.
    """

    current_text = (
        str(
            current_article.get(
                "headline",
                ""
            )
        )
        + " "
        + str(
            current_article.get(
                "summary",
                ""
            )
        )[:500]
    )

    older_text = (
        str(
            older_article.get(
                "headline",
                ""
            )
        )
        + " "
        + str(
            older_article.get(
                "summary",
                ""
            )
        )[:500]
    )

    sequence_score = SequenceMatcher(
        None,
        normalize_text(
            current_text
        ),
        normalize_text(
            older_text
        )
    ).ratio()

    jaccard_score = (
        token_jaccard_similarity(
            current_text,
            older_text
        )
    )

    return (
        sequence_score >= 0.62
        or jaccard_score >= 0.42
    )


def get_india_official_news_info(
    yahoo_symbol,
    target_session_date,
    previous_session_date
):
    """
    PRIMARY India catalyst check.

    Search official NSE/BSE corporate announcements over a 30-day
    lookback, then:
        - isolate the exact session catalyst window
        - choose a material filing
        - compare it with older filings
        - return the official filing link
    """

    (
        window_start_local,
        window_end_local,
        window_start_utc,
        window_end_utc

    ) = build_catalyst_window(
        "INDIA",
        previous_session_date,
        target_session_date
    )

    lookback_start = (
        target_session_date
        - timedelta(
            days=INDIA_OFFICIAL_NEWS_LOOKBACK_DAYS
        )
    )

    default_result = {
        "news_state": "FAIL",
        "headline": None,
        "summary": None,
        "source": None,
        "url": None,
        "published": None,
        "age_hours": None,
        "relevance": "NO OFFICIAL SESSION-MATCHED CATALYST",
        "catalyst_type": "NONE",
        "catalyst_evidence": None,
        "price_action_headline": False,
        "freshness": "NO OFFICIAL CATALYST",
        "older_headline": None,
        "older_url": None,
        "window_start": window_start_local,
        "window_end": window_end_local,
        "source_tier": "PRIMARY OFFICIAL EXCHANGE",
        "official_exchange_state": "FAIL",
        "secondary_news_state": "NOT CHECKED",
        "fallback_used": False,
    }

    try:

        if str(
            yahoo_symbol
        ).upper().endswith(
            ".NS"
        ):

            announcements = fetch_nse_official_announcements(
                yahoo_symbol,
                lookback_start,
                target_session_date
            )

        elif str(
            yahoo_symbol
        ).upper().endswith(
            ".BO"
        ):

            announcements = fetch_bse_official_announcements(
                yahoo_symbol,
                lookback_start,
                target_session_date
            )

        else:

            raise RuntimeError(
                "Ticker is not NSE (.NS) or BSE (.BO)."
            )

    except Exception as error:

        result = default_result.copy()

        result.update(
            {
                "news_state": "UNVERIFIED",
                "relevance": (
                    "OFFICIAL EXCHANGE ANNOUNCEMENT FETCH ERROR"
                ),
                "freshness": "UNVERIFIED",
                "official_exchange_state": "UNVERIFIED",
                "summary": str(error),
            }
        )

        return result

    # Convert exact IST catalyst window to UTC for comparison.
    # Parsed exchange dates are IST-aware, so direct timestamp
    # comparison also works; UTC makes the intent explicit.
    historical = [
        article
        for article in announcements
        if (
            article["published"] is not None
            and article["published"].astimezone(
                timezone.utc
            ) <= window_end_utc
        )
    ]

    session_articles = [
        article
        for article in historical
        if (
            window_start_utc
            <= article["published"].astimezone(
                timezone.utc
            )
            <= window_end_utc
        )
    ]

    if not session_articles:

        return default_result

    # Newest first.
    session_articles.sort(
        key=lambda article:
            article["published"],
        reverse=True
    )

    selected = None
    selected_classification = None

    for article in session_articles:

        classification = (
            classify_official_india_announcement(
                article
            )
        )

        if classification[
            "passes"
        ]:

            selected = article
            selected_classification = classification
            break

    # Exchange had a filing in the session, but nothing we could
    # identify as a material catalyst.
    if selected is None:

        newest = session_articles[0]

        routine_classification = (
            classify_official_india_announcement(
                newest
            )
        )

        result = default_result.copy()

        result.update(
            {
                "headline": newest["headline"],
                "summary": newest["summary"],
                "source": newest["source"],
                "url": newest["url"],
                "published": newest["published"],
                "relevance": (
                    routine_classification[
                        "relevance"
                    ]
                ),
                "catalyst_type": (
                    routine_classification[
                        "catalyst_type"
                    ]
                ),
            }
        )

        return result

    age_hours = (
        window_end_utc
        - selected[
            "published"
        ].astimezone(
            timezone.utc
        )
    ).total_seconds() / 3600

    freshness = "FRESH OFFICIAL FILING"
    older_headline = None
    older_url = None

    # Compare selected filing to older filings from the lookback.
    older_candidates = [
        article
        for article in historical
        if article[
            "published"
        ] < selected[
            "published"
        ]
    ]

    for older in older_candidates:

        if official_announcements_look_similar(
            selected,
            older
        ):

            freshness = (
                "POSSIBLE REPEAT / FOLLOW-UP"
            )

            older_headline = (
                older["headline"]
            )

            older_url = (
                older["url"]
            )

            break

    return {
        "news_state": "PASS",
        "headline": selected["headline"],
        "summary": selected["summary"],
        "source": selected["source"],
        "url": selected["url"],
        "published": selected["published"],
        "age_hours": round(
            max(
                age_hours,
                0
            ),
            2
        ),
        "relevance": (
            selected_classification[
                "relevance"
            ]
        ),
        "catalyst_type": (
            selected_classification[
                "catalyst_type"
            ]
        ),
        "catalyst_evidence": (
            selected_classification[
                "evidence"
            ]
        ),
        "price_action_headline": False,
        "freshness": freshness,
        "older_headline": older_headline,
        "older_url": older_url,
        "window_start": window_start_local,
        "window_end": window_end_local,
        "source_tier": "PRIMARY OFFICIAL EXCHANGE",
        "official_exchange_state": "PASS",
        "secondary_news_state": "NOT NEEDED",
        "fallback_used": False,
    }


# ============================================================
# SECTION 22: GET SESSION-MATCHED NEWS
# ============================================================

def get_news_info(
    stock,
    yahoo_symbol,
    info,
    market,
    target_session_date,
    previous_session_date
):
    """
    Get Yahoo news and match it to the same trading session
    as the price move.

    Catalyst window:
        previous session close
        ->
        target session close

    This prevents weekend/current-time mismatch.
    """

    aliases = meaningful_company_aliases(
        info
    )

    (
        window_start_local,
        window_end_local,
        window_start_utc,
        window_end_utc

    ) = build_catalyst_window(
        market,
        previous_session_date,
        target_session_date
    )

    default_result = {
        "news_state": "FAIL",
        "headline": None,
        "summary": None,
        "source": None,
        "url": None,
        "published": None,
        "age_hours": None,
        "relevance": "NO SESSION-MATCHED NEWS",
        "catalyst_type": "NONE",
        "catalyst_evidence": None,
        "price_action_headline": False,
        "freshness": "NO SESSION-MATCHED NEWS",
        "older_headline": None,
        "older_url": None,
        "window_start": window_start_local,
        "window_end": window_end_local,
    }

    try:

        raw_news = safe_get_yahoo_news(stock)

    except Exception:

        result = default_result.copy()

        result.update(
            {
                "news_state": "UNVERIFIED",
                "relevance": "NEWS FETCH ERROR",
                "freshness": "UNVERIFIED",
            }
        )

        return result

    if not raw_news:
        return default_result

    articles = []

    for item in raw_news:

        parsed = parse_yahoo_news_item(
            item
        )

        if parsed["published"] is not None:

            articles.append(
                parsed
            )

    if not articles:

        result = default_result.copy()

        result.update(
            {
                "news_state": "UNVERIFIED",
                "relevance": "NEWS DATE UNVERIFIED",
                "freshness": "UNVERIFIED",
            }
        )

        return result

    articles.sort(
        key=lambda article:
            article["published"],
        reverse=True
    )

    # Ignore news published AFTER the target trading session.
    historical_articles = [
        article
        for article in articles
        if article["published"] <= window_end_utc
    ]

    if not historical_articles:

        result = default_result.copy()

        result.update(
            {
                "news_state": "UNVERIFIED",
                "relevance": (
                    "NEWS HISTORY DOES NOT "
                    "REACH TARGET SESSION"
                ),
                "freshness": "UNVERIFIED",
            }
        )

        return result

    session_articles = [
        article
        for article in historical_articles
        if (
            window_start_utc
            <= article["published"]
            <= window_end_utc
        )
    ]

    if not session_articles:

        oldest_returned = min(
            article["published"]
            for article in articles
        )

        # Yahoo may simply not have returned enough history.
        if oldest_returned > window_start_utc:

            result = default_result.copy()

            result.update(
                {
                    "news_state": "UNVERIFIED",
                    "relevance": (
                        "YAHOO NEWS HISTORY TOO SHORT"
                    ),
                    "freshness": "UNVERIFIED",
                }
            )

            return result

        # Yahoo history reaches before our window, therefore absence
        # of a session article is a meaningful failure.
        context_article = historical_articles[0]

        result = default_result.copy()

        result.update(
            {
                "headline": context_article["headline"],
                "summary": context_article["summary"],
                "source": context_article["source"],
                "url": context_article["url"],
                "published": context_article["published"],
                "relevance": (
                    "NO CATALYST IN TARGET "
                    "SESSION WINDOW"
                ),
                "freshness": (
                    "OUTSIDE TARGET WINDOW"
                ),
            }
        )

        return result

    # Look for the newest article that actually passes our
    # direct-catalyst requirement.
    selected = None
    classification = None

    for article in session_articles:

        candidate = classify_news_article(
            yahoo_symbol,
            article["headline"],
            article["summary"],
            aliases
        )

        if candidate[
            "passes_direct_news"
        ]:

            selected = article
            classification = candidate
            break

    # No direct catalyst found; use newest article for explanation.
    if selected is None:

        selected = session_articles[0]

        classification = (
            classify_news_article(
                yahoo_symbol,
                selected["headline"],
                selected["summary"],
                aliases
            )
        )

    age_hours = (
        window_end_utc
        - selected["published"]
    ).total_seconds() / 3600

    # -------------------- Fresh vs repeat --------------------

    freshness = "FRESH CANDIDATE"
    older_headline = None
    older_url = None

    for older in historical_articles:

        if (
            older["published"]
            >= selected["published"]
        ):
            continue

        if articles_look_similar(
            selected["headline"],
            older["headline"]
        ):

            freshness = (
                "POSSIBLE REPEAT / FOLLOW-UP"
            )

            older_headline = (
                older["headline"]
            )

            older_url = (
                older["url"]
            )

            break

    if REQUIRE_DIRECT_NEWS:

        news_passes = (
            classification[
                "passes_direct_news"
            ]
        )

    else:
        news_passes = True

    if (
        REQUIRE_FRESH_CATALYST
        and freshness
        != "FRESH CANDIDATE"
    ):

        news_passes = False

    news_state = (
        "PASS"
        if news_passes
        else "FAIL"
    )

    return {
        "news_state": news_state,
        "headline": selected["headline"],
        "summary": selected["summary"],
        "source": selected["source"],
        "url": selected["url"],
        "published": selected["published"],
        "age_hours": round(
            max(age_hours, 0),
            2
        ),
        "relevance": classification["relevance"],
        "catalyst_type": classification["catalyst_type"],
        "catalyst_evidence": classification["catalyst_evidence"],
        "price_action_headline": classification["price_action_headline"],
        "freshness": freshness,
        "older_headline": older_headline,
        "older_url": older_url,
        "window_start": window_start_local,
        "window_end": window_end_local,
    }



# ============================================================
# VERSION 9: CHOOSE THE BEST NEWS SOURCE
# ============================================================

def get_best_news_info(
    stock,
    yahoo_symbol,
    info,
    market,
    target_session_date,
    previous_session_date
):
    """
    Source priority:

    INDIA:
        1. Official NSE/BSE announcement
        2. Yahoo/direct-news fallback

    US / CANADA:
        Yahoo/direct-news logic from the working earlier versions.

    The result always contains source-quality metadata.
    """

    if market != "INDIA":

        yahoo_result = get_news_info(
            stock=stock,
            yahoo_symbol=yahoo_symbol,
            info=info,
            market=market,
            target_session_date=target_session_date,
            previous_session_date=previous_session_date
        )

        yahoo_result.update(
            {
                "source_tier": "SECONDARY NEWS / YAHOO",
                "official_exchange_state": "N/A",
                "secondary_news_state": (
                    yahoo_result[
                        "news_state"
                    ]
                ),
                "fallback_used": False,
            }
        )

        return yahoo_result

    # --------------------------------------------------------
    # INDIA: PRIMARY OFFICIAL EXCHANGE CHECK
    # --------------------------------------------------------

    official_result = (
        get_india_official_news_info(
            yahoo_symbol=yahoo_symbol,
            target_session_date=target_session_date,
            previous_session_date=previous_session_date
        )
    )

    if official_result[
        "news_state"
    ] == "PASS":

        return official_result

    # --------------------------------------------------------
    # SECONDARY FALLBACK
    # --------------------------------------------------------

    yahoo_result = get_news_info(
        stock=stock,
        yahoo_symbol=yahoo_symbol,
        info=info,
        market=market,
        target_session_date=target_session_date,
        previous_session_date=previous_session_date
    )

    # Yahoo found a direct catalyst that the exchange feed did not.
    # This can happen with analyst actions or other external news.
    if yahoo_result[
        "news_state"
    ] == "PASS":

        yahoo_result.update(
            {
                "source_tier": (
                    "SECONDARY NEWS FALLBACK"
                ),
                "official_exchange_state": (
                    official_result[
                        "news_state"
                    ]
                ),
                "secondary_news_state": "PASS",
                "fallback_used": True,
            }
        )

        return yahoo_result

    # If the PRIMARY exchange source could not be reached and Yahoo
    # also did not positively verify a catalyst, do NOT force a FAIL.
    if official_result[
        "news_state"
    ] == "UNVERIFIED":

        official_result.update(
            {
                "secondary_news_state": (
                    yahoo_result[
                        "news_state"
                    ]
                ),
                "fallback_used": True,
            }
        )

        return official_result

    # Official feed worked and found no qualifying catalyst.
    # If secondary Yahoo is itself unavailable/unverified, overall
    # news becomes UNVERIFIED rather than falsely definitive.
    if yahoo_result[
        "news_state"
    ] == "UNVERIFIED":

        official_result.update(
            {
                "news_state": "UNVERIFIED",
                "relevance": (
                    "NO OFFICIAL CATALYST; "
                    "SECONDARY NEWS UNVERIFIED"
                ),
                "secondary_news_state": "UNVERIFIED",
                "fallback_used": True,
            }
        )

        return official_result

    # Both checks completed and neither found a direct catalyst.
    official_result.update(
        {
            "secondary_news_state": (
                yahoo_result[
                    "news_state"
                ]
            ),
            "fallback_used": True,
        }
    )

    return official_result


# ============================================================
# SECTION 23: FLOAT
# ============================================================

def get_float_state(
    info
):
    """
    Float state:
        PASS
        FAIL
        UNVERIFIED

    Missing float is never silently treated as a confirmed failure.
    """

    float_shares = info.get(
        "floatShares"
    )

    if (
        float_shares is None
        or pd.isna(float_shares)
    ):

        # ----------------------------------------------------
        # FINAL SAFE FALLBACK:
        #
        # If TOTAL shares outstanding are <= 20M, then public
        # float MUST also be <= 20M because:
        #
        #       float <= total shares outstanding
        #
        # This confirms a PASS without pretending we know the exact
        # float. We display the total-share value as an upper bound.
        # ----------------------------------------------------

        shares_outstanding = info.get(
            "sharesOutstanding"
        )

        try:

            if (
                shares_outstanding is not None
                and not pd.isna(
                    shares_outstanding
                )
            ):

                shares_outstanding = float(
                    shares_outstanding
                )

                if (
                    shares_outstanding
                    <= MAX_FLOAT
                ):

                    return (
                        shares_outstanding,
                        "PASS",
                        (
                            "SAFE UPPER-BOUND PASS: "
                            f"total shares outstanding "
                            f"({shares_outstanding:,.0f}) "
                            f"<= {MAX_FLOAT:,}; actual float "
                            "cannot be higher than total shares"
                        )
                    )

        except Exception:

            pass

        return (
            None,
            "UNVERIFIED",
            (
                "Yahoo did not provide floatShares "
                "and no safe <=20M total-share upper bound "
                "was available"
            )
        )

    try:

        float_shares = float(
            float_shares
        )

    except Exception:

        return (
            None,
            "UNVERIFIED",
            "Float could not be converted to a number"
        )

    if float_shares <= MAX_FLOAT:

        return (
            float_shares,
            "PASS",
            f"Float <= {MAX_FLOAT:,}"
        )

    return (
        float_shares,
        "FAIL",
        f"Float > {MAX_FLOAT:,}"
    )


# ============================================================
# SECTION 24: SCORING
# ============================================================

def bool_to_state(value):
    """
    Convert True/False to PASS/FAIL.
    """

    return (
        "PASS"
        if bool(value)
        else "FAIL"
    )


def calculate_scores(
    change_state,
    rvol_state,
    news_state,
    price_state,
    float_state
):
    """
    Confirmed Score:
        confirmed PASS rules only.

    Potential Score:
        confirmed PASS
        +
        UNVERIFIED rules.
    """

    states = [
        change_state,
        rvol_state,
        news_state,
        price_state,
        float_state,
    ]

    confirmed_score = sum(
        state == "PASS"
        for state in states
    )

    unknown_count = sum(
        state == "UNVERIFIED"
        for state in states
    )

    potential_score = (
        confirmed_score
        + unknown_count
    )

    return (
        confirmed_score,
        potential_score,
        unknown_count
    )


def get_failed_and_unknown_rules(
    change_state,
    rvol_state,
    news_state,
    price_state,
    float_state
):
    """
    Return readable lists of failed and missing-data rules.
    """

    rule_states = {
        "Day Change": change_state,
        "RVOL": rvol_state,
        "News": news_state,
        "Price": price_state,
        "Float": float_state,
    }

    failed = [
        rule
        for rule, state in rule_states.items()
        if state == "FAIL"
    ]

    unknown = [
        rule
        for rule, state in rule_states.items()
        if state == "UNVERIFIED"
    ]

    return failed, unknown


# ============================================================
# SECTION 25: SMART NEAR-MATCH RULE
# ============================================================

def failed_rule_is_close(
    failed_rule,
    market,
    day_change,
    rvol,
    price,
    float_shares
):
    """
    A 4/5 stock is shown only if the failed criterion is close
    enough to the target.

    This prevents a -18% stock from appearing as a momentum
    near-match merely because it passed the other four rules.
    """

    config = MARKET_CONFIG[
        market
    ]

    if failed_rule == "Day Change":

        return (
            day_change
            >= config[
                "near_day_change_min"
            ]
        )

    if failed_rule == "RVOL":

        return (
            rvol
            >= config[
                "near_rvol_min"
            ]
        )

    if failed_rule == "Price":

        return (
            config["near_price_min"]
            <= price
            <= config["near_price_max"]
        )

    if failed_rule == "Float":

        if float_shares is None:
            return False

        return (
            float_shares
            <= NEAR_FLOAT_MAX
        )

    if failed_rule == "News":

        return (
            SHOW_NEAR_MATCH_IF_NEWS_FAILS
        )

    return False


def determine_candidate_status(
    market,
    confirmed_score,
    potential_score,
    unknown_count,
    failed_rules,
    day_change,
    rvol,
    price,
    float_shares
):
    """
    Final category:
        QUALIFIED
        NEAR MATCH
        REVIEW / UNVERIFIED
        HIDDEN
    """

    if confirmed_score == 5:
        return "QUALIFIED"

    if (
        confirmed_score == 4
        and unknown_count == 0
        and len(failed_rules) == 1
    ):

        if failed_rule_is_close(
            failed_rules[0],
            market,
            day_change,
            rvol,
            price,
            float_shares
        ):

            return "NEAR MATCH"

        return "HIDDEN"

    if (
        unknown_count > 0
        and potential_score
        >= MINIMUM_SCORE_TO_SHOW
    ):

        if len(failed_rules) == 0:
            return "REVIEW / UNVERIFIED"

        if len(failed_rules) == 1:

            if failed_rule_is_close(
                failed_rules[0],
                market,
                day_change,
                rvol,
                price,
                float_shares
            ):

                return "REVIEW / UNVERIFIED"

    return "HIDDEN"


# ============================================================
# SECTION 26: STAGE 2 - FLOAT + CATALYST
# ============================================================

def run_stage_two(
    stage_one,
    market
):
    """
    Expensive Stage 2 runs only on Stage-1 survivors.
    """

    if stage_one.empty:
        return pd.DataFrame()

    print(
        "STAGE 2: Checking Float, News, "
        "Catalyst and Freshness..."
    )

    final_results = []

    total = len(
        stage_one
    )

    for number, (_, row) in enumerate(
        stage_one.iterrows(),
        start=1
    ):

        symbol = row["Ticker"]

        print(
            f"  {number}/{total}: "
            f"{symbol}"
        )

        try:

            stock = yf.Ticker(
                symbol
            )

            # -------------------- Company info --------------------

            info = safe_get_ticker_info(
                stock
            )

            # -------------------- Float --------------------

            (
                float_shares,
                float_state,
                float_explanation

            ) = get_float_state(
                info
            )

            # -------------------- News --------------------

            news_info = get_best_news_info(
                stock=stock,
                yahoo_symbol=symbol,
                info=info,
                market=market,
                target_session_date=row[
                    "Session Date"
                ],
                previous_session_date=row[
                    "Previous Session Date"
                ],
            )

            news_state = news_info[
                "news_state"
            ]

            if not REQUIRE_NEWS:
                news_state = "PASS"

            # -------------------- Other rule states --------------------

            change_state = bool_to_state(
                row["Change Pass"]
            )

            rvol_state = bool_to_state(
                row["RVOL Pass"]
            )

            price_state = bool_to_state(
                row["Price Pass"]
            )

            # -------------------- Scores --------------------

            (
                confirmed_score,
                potential_score,
                unknown_count

            ) = calculate_scores(
                change_state,
                rvol_state,
                news_state,
                price_state,
                float_state
            )

            failed_rules, unknown_rules = (
                get_failed_and_unknown_rules(
                    change_state,
                    rvol_state,
                    news_state,
                    price_state,
                    float_state
                )
            )

            status = determine_candidate_status(
                market=market,
                confirmed_score=confirmed_score,
                potential_score=potential_score,
                unknown_count=unknown_count,
                failed_rules=failed_rules,
                day_change=float(
                    row["Day Change %"]
                ),
                rvol=float(
                    row["RVOL"]
                ),
                price=float(
                    row["Price"]
                ),
                float_shares=float_shares
            )

            if status == "HIDDEN":
                continue

            result = row.to_dict()

            result.update(
                {
                    "Company Name": (
                        info.get("longName")
                        or info.get("shortName")
                    ),

                    "Change State": change_state,
                    "RVOL State": rvol_state,
                    "Price State": price_state,

                    "Float": float_shares,
                    "Float State": float_state,
                    "Float Explanation": float_explanation,

                    "News State": news_state,

                    "Confirmed Score": confirmed_score,
                    "Potential Score": potential_score,
                    "Unknown Count": unknown_count,

                    "Failed Rules": (
                        ", ".join(failed_rules)
                        if failed_rules
                        else "None"
                    ),

                    "Unknown Rules": (
                        ", ".join(unknown_rules)
                        if unknown_rules
                        else "None"
                    ),

                    "Status": status,

                    "News Headline": news_info["headline"],
                    "News Summary": news_info["summary"],
                    "News Source": news_info["source"],
                    "News Published": news_info["published"],
                    "News Age Hours": news_info["age_hours"],
                    "News Relevance": news_info["relevance"],
                    "Catalyst Type": news_info["catalyst_type"],
                    "Catalyst Evidence": news_info["catalyst_evidence"],
                    "News Freshness": news_info["freshness"],
                    "News Link": news_info["url"],
                    "Catalyst Window Start": news_info["window_start"],
                    "Catalyst Window End": news_info["window_end"],
                    "Previous Similar News": news_info["older_headline"],
                    "Previous News Link": news_info["older_url"],

                    "News Source Tier": news_info.get(
                        "source_tier",
                        "UNKNOWN"
                    ),

                    "Official Exchange State": news_info.get(
                        "official_exchange_state",
                        "N/A"
                    ),

                    "Secondary News State": news_info.get(
                        "secondary_news_state",
                        "N/A"
                    ),

                    "News Fallback Used": news_info.get(
                        "fallback_used",
                        False
                    ),
                }
            )

            final_results.append(
                result
            )

        except Exception as error:

            print(
                f"    Error analyzing "
                f"{symbol}: {error}"
            )

    return pd.DataFrame(
        final_results
    )


# ============================================================
# SECTION 27: PREPARE RESULTS
# ============================================================

def build_review_note(row):
    """
    Examples:
        FAIL: News
        FAIL: Float
        UNVERIFIED: Float
    """

    parts = []

    if row[
        "Failed Rules"
    ] != "None":

        parts.append(
            "FAIL: "
            + row["Failed Rules"]
        )

    if row[
        "Unknown Rules"
    ] != "None":

        parts.append(
            "UNVERIFIED: "
            + row["Unknown Rules"]
        )

    return (
        " | ".join(parts)
        if parts
        else "None"
    )


def prepare_final_results(
    results,
    market
):
    """
    Add readable columns and sort strong tradable candidates first.
    """

    if results.empty:
        return results

    results = results.copy()

    results["Review Note"] = (
        results.apply(
            build_review_note,
            axis=1
        )
    )

    results["Volume Display"] = (
        results["Volume"]
        .apply(format_large_number)
    )

    results["Avg Volume Display"] = (
        results["20D Avg Volume"]
        .apply(format_large_number)
    )

    def make_float_display(row):
        """
        Exact Yahoo float -> normal number.

        Safe total-share fallback -> <= number, clearly marked as
        an upper bound rather than an exact public float.
        """

        value = row["Float"]

        explanation = str(
            row.get(
                "Float Explanation",
                ""
            )
        )

        if (
            "SAFE UPPER-BOUND PASS"
            in explanation
            and value is not None
            and not pd.isna(value)
        ):

            return (
                "<="
                + format_large_number(
                    value
                )
            )

        return format_large_number(
            value
        )

    results["Float Display"] = (
        results.apply(
            make_float_display,
            axis=1
        )
    )

    results["Turnover Display"] = (
        results["Current Turnover"]
        .apply(
            lambda value:
                format_money(
                    value,
                    market
                )
        )
    )

    results["Liquidity Sort"] = (
        results["Liquidity Quality"]
        .map(
            {
                "GOOD": 1,
                "LOW TURNOVER": 0,
            }
        )
        .fillna(0)
    )

    results["RVOL Quality Sort"] = (
        results["RVOL Quality"]
        .map(
            {
                "GOOD": 1,
                "LOW BASE": 0,
            }
        )
        .fillna(0)
    )

    return results.sort_values(
        by=[
            "Confirmed Score",
            "Liquidity Sort",
            "RVOL Quality Sort",
            "Current Turnover",
            "RVOL",
            "Day Change %"
        ],
        ascending=[
            False,
            False,
            False,
            False,
            False,
            False
        ]
    )


# ============================================================
# SECTION 28: PRINT COMPACT TABLES
# ============================================================

def print_candidate_table(
    dataframe,
    title
):
    """
    Long news details are printed separately.
    """

    print(
        "\n"
        + "=" * 108
    )

    print(title)

    print(
        "=" * 108
    )

    if dataframe.empty:

        print(
            "\nNone found."
        )

        return

    display = dataframe.copy()

    display["Price"] = (
        display["Price"]
        .round(2)
    )

    display["Day Change %"] = (
        display["Day Change %"]
        .round(2)
    )

    display["RVOL"] = (
        display["RVOL"]
        .round(2)
    )

    columns = [
        "Ticker",
        "Session Date",
        "Price",
        "Day Change %",
        "RVOL",
        "Avg Volume Display",
        "RVOL Quality",
        "Turnover Display",
        "Liquidity Quality",
        "Float Display",
        "Confirmed Score",
        "Potential Score",
        "Review Note",
    ]

    print(
        "\n"
        + display[
            columns
        ].to_string(
            index=False
        )
    )


# ============================================================
# SECTION 29: PRINT NEWS / CATALYST DETAILS
# ============================================================

def print_news_details(
    dataframe,
    market
):
    """
    Show links and freshness separately from the compact table.
    """

    if dataframe.empty:
        return

    currency = MARKET_CONFIG[
        market
    ]["currency"]

    print(
        "\n"
        + "=" * 108
    )

    print(
        "NEWS / CATALYST / DATA QUALITY DETAILS"
    )

    print(
        "=" * 108
    )

    for _, row in dataframe.iterrows():

        print(
            f"\n{row['Ticker']} "
            f"| {row['Status']} "
            f"| Confirmed "
            f"{int(row['Confirmed Score'])}/5 "
            f"| Potential "
            f"{int(row['Potential Score'])}/5"
        )

        print(
            "-" * 108
        )

        print(
            f"Company: "
            f"{row.get('Company Name') or 'N/A'}"
        )

        print(
            f"Target Session: "
            f"{row['Session Date']}"
        )

        print(
            "Catalyst Window: "
            f"{format_market_datetime(row['Catalyst Window Start'], market)}"
            " -> "
            f"{format_market_datetime(row['Catalyst Window End'], market)}"
        )

        print(
            f"Price: "
            f"{currency}"
            f"{float(row['Price']):.2f}"
        )

        print(
            f"Day Change: "
            f"{float(row['Day Change %']):.2f}%"
        )

        print(
            f"RVOL: "
            f"{float(row['RVOL']):.2f}x"
        )

        print(
            f"20D Avg Volume: "
            f"{row['Avg Volume Display']} "
            f"| RVOL Quality: "
            f"{row['RVOL Quality']}"
        )

        print(
            f"Session Turnover: "
            f"{row['Turnover Display']} "
            f"| Liquidity: "
            f"{row['Liquidity Quality']}"
        )

        print(
            f"Float: "
            f"{row['Float Display']} "
            f"| State: "
            f"{row['Float State']}"
        )

        print(
            f"News State: "
            f"{row['News State']}"
        )

        print(
            f"News Source Tier: "
            f"{row.get('News Source Tier', 'N/A')}"
        )

        print(
            f"Official Exchange Check: "
            f"{row.get('Official Exchange State', 'N/A')}"
        )

        print(
            f"Secondary News Check: "
            f"{row.get('Secondary News State', 'N/A')}"
        )

        print(
            f"News Relevance: "
            f"{row['News Relevance']}"
        )

        print(
            f"Catalyst Type: "
            f"{row['Catalyst Type']}"
        )

        print(
            f"Catalyst Evidence: "
            f"{row['Catalyst Evidence'] or 'N/A'}"
        )

        print(
            f"Freshness: "
            f"{row['News Freshness']}"
        )

        print(
            "News Published: "
            f"{format_market_datetime(row['News Published'], market)}"
        )

        print(
            f"Headline: "
            f"{row['News Headline'] or 'N/A'}"
        )

        print(
            f"Source: "
            f"{row['News Source'] or 'N/A'}"
        )

        print(
            f"Current News Link: "
            f"{row['News Link'] or 'N/A'}"
        )

        older_headline = row[
            "Previous Similar News"
        ]

        older_url = row[
            "Previous News Link"
        ]

        if pd.notna(
            older_headline
        ):

            print(
                f"Earlier Similar Story: "
                f"{older_headline}"
            )

            print(
                f"Earlier Story Link: "
                f"{older_url or 'N/A'}"
            )

        else:

            print(
                "Earlier Similar Story: "
                "None found in the source history checked"
            )


# ============================================================
# SECTION 30: SAVE CSV
# ============================================================

def save_results(
    results,
    market
):
    """
    Save visible candidates next to this Python file.
    """

    if results.empty:
        return

    filename = MARKET_CONFIG[
        market
    ]["output_csv"]

    output_path = (
        Path(__file__)
        .resolve()
        .parent
        / filename
    )

    save_copy = results.drop(
        columns=[
            "Liquidity Sort",
            "RVOL Quality Sort"
        ],
        errors="ignore"
    )

    save_copy.to_csv(
        output_path,
        index=False
    )

    print(
        "\nResults saved to:"
    )

    print(
        output_path
    )


# ============================================================
# SECTION 31: PRINT RULES FOR SELECTED MARKET
# ============================================================

def print_selected_market_rules(
    market
):
    """
    Show exactly which settings Python is about to use.
    """

    config = MARKET_CONFIG[
        market
    ]

    currency = config[
        "currency"
    ]

    print(
        "\n"
        + "=" * 108
    )

    print(
        "STOCK SCREENER - VERSION 9.1 - MULTI-MARKET"
    )

    print(
        f"SELECTED MARKET: "
        f"{config['label']}"
    )

    print(
        "=" * 108
    )

    print(
        "\nMAIN 5 RULES:"
        f"\n  1. Day Change >= {MIN_DAY_CHANGE:.0f}%"
        f"\n  2. RVOL >= {MIN_RVOL:.1f}x"
        "\n  3. Identifiable direct recent catalyst required"
        f"\n  4. Price = "
        f"{currency}{config['min_price']:.2f}"
        f" to "
        f"{currency}{config['max_price']:.2f}"
        f"\n  5. Float <= {MAX_FLOAT:,}"
    )

    print(
        "\nNEAR-MATCH LIMITS:"
        f"\n  Day Change miss -> still >= "
        f"+{config['near_day_change_min']:.0f}%"
        f"\n  RVOL miss       -> still >= "
        f"{config['near_rvol_min']:.1f}x"
        f"\n  Price miss      -> still between "
        f"{currency}{config['near_price_min']:.2f}"
        f" and "
        f"{currency}{config['near_price_max']:.2f}"
        f"\n  Float miss      -> still <= "
        f"{NEAR_FLOAT_MAX:,}"
    )

    print(
        "\nQUALITY WARNINGS:"
        f"\n  Good RVOL base >= "
        f"{MIN_AVG_VOLUME_FOR_GOOD_RVOL:,} "
        "average shares/day"
        f"\n  Good session turnover >= "
        f"{currency}"
        f"{config['min_turnover_for_good_liquidity']:,.0f}"
        "\n"
    )



    if market == "INDIA":

        print(
            "INDIA DATA SOURCE:"
        )

        print(
            "  Stage 1 price/volume/RVOL = official NSE/BSE bhavcopy"
        )

        print(
            "  Stage 2 catalyst          = official NSE/BSE announcements FIRST"
        )

        print(
            "  Secondary catalyst        = Yahoo/direct-news fallback"
        )

        print(
            "  Float                     = Yahoo floatShares; safe total-share"
        )

        print(
            "                              upper-bound fallback when possible"
        )

        print(
            f"  Freshness lookback        = "
            f"{INDIA_OFFICIAL_NEWS_LOOKBACK_DAYS} calendar days"
        )

        print()

# ============================================================
# SECTION 32: MAIN PROGRAM
# ============================================================

def run_market_screener(market):
    """
    Run ONE selected market.

    The persistent controller calls this function repeatedly, so a
    return here ends only the current market -- not the whole program.
    """

    print_selected_market_rules(
        market
    )

    # --------------------------------------------------------
    # STEP 2: IDENTIFY CORRECT TRADING SESSION
    # --------------------------------------------------------

    try:

        (
            target_session_date,
            previous_session_date,
            benchmark

        ) = get_target_sessions(
            market
        )

    except Exception as error:

        print(
            "\nCould not determine market session."
        )

        print(
            f"Error: {error}"
        )

        return False

    (
        window_start,
        window_end,
        _,
        _

    ) = build_catalyst_window(
        market,
        previous_session_date,
        target_session_date
    )

    print(
        "SESSION ALIGNMENT:"
    )

    print(
        f"  Benchmark used: "
        f"{benchmark}"
    )

    print(
        f"  Previous session: "
        f"{previous_session_date}"
    )

    print(
        f"  TARGET SESSION:   "
        f"{target_session_date}"
    )

    print(
        "  Catalyst window:  "
        f"{format_market_datetime(window_start, market)}"
        " -> "
        f"{format_market_datetime(window_end, market)}"
    )

    print()

    # --------------------------------------------------------
    # STEP 3: LOAD OFFICIAL MARKET UNIVERSE
    # --------------------------------------------------------

    try:

        symbols = load_market_universe_resilient(
            market
        )

    except Exception as error:

        print(
            "\nCould not load the selected market universe."
        )

        print(
            f"Error: {error}"
        )

        if market == "CANADA":

            print(
                "\nTMX sometimes blocks automated page requests. "
                "If this happens, send me this error and we will "
                "switch the Canada loader to a local TMX export."
            )

        return False

    # --------------------------------------------------------
    # STEP 4: FAST STAGE 1
    # --------------------------------------------------------

    stage_one = run_stage_one(
        symbols=symbols,
        market=market,
        target_session_date=target_session_date,
        previous_session_date=previous_session_date
    )

    if stage_one.empty:

        print(
            "\nNo stocks survived Stage 1."
        )

        return False

    # --------------------------------------------------------
    # STEP 5: FLOAT + NEWS STAGE 2
    # --------------------------------------------------------

    results = run_stage_two(
        stage_one,
        market
    )

    results = prepare_final_results(
        results,
        market
    )

    if results.empty:

        print(
            "\n"
            + "=" * 108
        )

        print(
            "NO USEFUL 4/5 OR 5/5 CANDIDATES FOUND"
        )

        print(
            "=" * 108
        )

        return False

    # --------------------------------------------------------
    # STEP 6: SPLIT RESULTS
    # --------------------------------------------------------

    qualified = results[
        results["Status"]
        == "QUALIFIED"
    ].copy()

    near_matches = results[
        results["Status"]
        == "NEAR MATCH"
    ].copy()

    review = results[
        results["Status"]
        == "REVIEW / UNVERIFIED"
    ].copy()

    # --------------------------------------------------------
    # STEP 7: DISPLAY
    # --------------------------------------------------------

    print_candidate_table(
        qualified,
        "QUALIFIED STOCKS - CONFIRMED 5/5"
    )

    print_candidate_table(
        near_matches,
        (
            "NEAR MATCHES - CONFIRMED 4/5 "
            "+ CLOSE TO FAILED RULE"
        )
    )

    print_candidate_table(
        review,
        (
            "REVIEW / UNVERIFIED - "
            "POTENTIAL >= 4/5"
        )
    )

    print_news_details(
        results,
        market
    )

    # --------------------------------------------------------
    # STEP 8: SUMMARY
    # --------------------------------------------------------

    print(
        "\n"
        + "=" * 108
    )

    print(
        "SCREENING SUMMARY"
    )

    print(
        "=" * 108
    )

    print(
        f"\nMarket: "
        f"{MARKET_CONFIG[market]['label']}"
    )

    print(
        f"Target trading session: "
        f"{target_session_date}"
    )

    print(
        f"Symbols loaded: "
        f"{len(symbols):,}"
    )

    print(
        f"Stage 1 survivors: "
        f"{len(stage_one):,}"
    )

    print(
        f"Qualified 5/5: "
        f"{len(qualified):,}"
    )

    print(
        f"Near Matches 4/5: "
        f"{len(near_matches):,}"
    )

    print(
        f"Review / Unverified: "
        f"{len(review):,}"
    )

    # --------------------------------------------------------
    # STEP 9: SAVE CSV
    # --------------------------------------------------------

    save_results(
        results,
        market
    )

    print(
        "\nDone.\n"
    )

    return True


# ============================================================
# SECTION 33: MULTI-MARKET CONTROLLER
# ============================================================

def run_market_safely(market):
    """
    Isolate one market from the next.

    If US fails, Canada and India can still be run. If India created
    exchange-cookie sessions, they are closed before the next market.
    """

    reset_runtime_state()

    print(
        "\n"
        + "#" * 108
    )

    print(
        f"STARTING MARKET: "
        f"{MARKET_CONFIG[market]['label']}"
    )

    print(
        "#" * 108
    )

    try:
        completed = run_market_screener(
            market
        )

        return bool(completed)

    except KeyboardInterrupt:

        print(
            f"\n{market} scan cancelled by user."
        )

        return False

    except Exception as error:

        print(
            "\n"
            + "!" * 88
        )

        print(
            f"{market} encountered an unexpected error."
        )

        print(
            f"Error: {error}"
        )

        print(
            "The other markets can still be run. "
            "Full traceback saved to stock_screener_error.log."
        )

        print(
            "!" * 88
        )

        write_error_log(
            market,
            "UNHANDLED MARKET ERROR",
            error
        )

        return False

    finally:
        reset_runtime_state()


def print_all_markets_summary(statuses):
    """Compact controller-level status after option 4."""

    print(
        "\n"
        + "=" * 72
    )

    print(
        "ALL-MARKET RUN STATUS"
    )

    print(
        "=" * 72
    )

    for market in [
        "US",
        "CANADA",
        "INDIA"
    ]:

        state = (
            "COMPLETED"
            if statuses.get(market)
            else "ERROR / CANCELLED"
        )

        print(
            f"{market:<8} : {state}"
        )

def main():
    """
    Persistent menu.

    You can now run:
        US -> Canada -> India -> US ...

    without restarting IDLE, or select option 4 to run all three.
    """

    print(
        "\nSTOCK SCREENER - VERSION 9.1 MULTI-MARKET STABLE"
    )

    print(
        "You can switch markets repeatedly without restarting Python."
    )

    while True:

        selection = choose_market()

        if selection == "EXIT":

            reset_runtime_state()

            print(
                "\nStock screener closed cleanly.\n"
            )

            break

        if selection == "ALL":

            statuses = {}

            markets_to_run = [
                "US",
                "CANADA",
                "INDIA"
            ]

            for index, market in enumerate(
                markets_to_run
            ):

                statuses[market] = run_market_safely(
                    market
                )

                if index < len(markets_to_run) - 1:

                    print(
                        f"\nWaiting {MARKET_SWITCH_PAUSE_SECONDS:.0f} seconds "
                        "before the next market..."
                    )

                    pytime.sleep(
                        MARKET_SWITCH_PAUSE_SECONDS
                    )

            print_all_markets_summary(
                statuses
            )

        else:

            run_market_safely(
                selection
            )

        print(
            "\nReturning to the market menu. "
            "You do NOT need to restart IDLE.\n"
        )


# Backward-compatible name: manually calling run_screener() from
# the IDLE Shell now opens the persistent controller.
def run_screener():
    return main()


# ============================================================
# SECTION 34: START THE PROGRAM
# ============================================================

if __name__ == "__main__":

    main()
