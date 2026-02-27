import time
import sys
import traceback
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional
import pytz
import pandas as pd
import statistics
import os
from django.utils import timezone
from django.contrib.auth.models import User
from django.db import transaction
from trading.models import Trade, DailyPnL, BotStatus, LogEntry
from .auth import generate_and_set_access_token_db
from kiteconnect import KiteConnect
import holidays
from decimal import Decimal
from kiteconnect.exceptions import TokenException

# ── Gevent-safe lock ──────────────────────────────────────────────────────────
# With --pool=gevent --concurrency=20, all 20 coroutines share the same process
# memory. A plain bool flag (_entry_attempted) is NOT coroutine-safe because
# gevent can switch coroutines between the flag check and the flag set.
# We use gevent.lock.BoundedSemaphore(1) which is coroutine-safe: only one
# coroutine can hold it at a time. If gevent is not installed, we fall back to
# a plain threading.Lock (safe for thread pools / prefork workers).
try:
    from gevent.lock import BoundedSemaphore as GeventSemaphore
    _GEVENT_AVAILABLE = True
except ImportError:
    from threading import Lock as GeventSemaphore
    _GEVENT_AVAILABLE = False

# One lock per user_id — created on first bot start, reused on restarts.
# Stored at module level so it survives TradingApplication re-instantiation.
_USER_ENTRY_LOCKS: Dict[int, object] = {}


# ===================== CONFIGURATION =====================
class Config:
    SPOT_SYMBOL                      = "NSE:NIFTY 50"
    VIX_SYMBOL                       = "NSE:INDIA VIX"
    EXCHANGE                         = "NFO"
    UNDERLYING                       = "NIFTY"
    LOT_SIZE                         = 65
    ENTRY_START                      = dtime(13, 29, 0)
    ENTRY_END                        = dtime(13, 30, 59)          # 60-second window
    TOKEN_REFRESH_TIME               = dtime(8, 30)
    EXIT_TIME                        = dtime(15, 0)
    MARKET_CLOSE                     = dtime(15, 30)
    MAIN_DISTANCE                    = 150
    HEDGE_PREMIUM_RATIO              = 0.10
    MAX_OVERPAY_MULT                 = 1.25
    MAX_CAPITAL_USAGE                = 1.0
    MIN_CAPITAL_FOR_1LOT             = 120000
    MAX_LOTS                         = 50
    VIX_EXIT_ABS                     = 18.0
    VIX_SPIKE_MULTIPLIER             = 1.20
    VIX_MIN                          = 7.0
    VIX_MAX                          = 30.0
    VIX_THRESHOLD_FOR_PERCENT_TARGET = 12
    PERCENT_TARGET_WHEN_VIX_HIGH     = 0.020
    ADJUSTMENT_TRIGGER_POINTS        = 50
    ADJUSTMENT_CUTOFF_TIME           = dtime(13, 30)
    MAX_ADJUSTMENTS_PER_SIDE_PER_DAY = 1
    MIN_HEDGE_GAP                    = 300
    TIMEZONE                         = pytz.timezone("Asia/Kolkata")
    EMERGENCY_STOP_FILE              = "EMERGENCY_STOP.txt"
    MAX_TOKEN_ATTEMPTS               = 10
    MAX_RETRIES                      = 3
    RETRY_DELAY                      = 2
    ORDER_TIMEOUT                    = 10
    PNL_CHECK_INTERVAL_SECONDS       = 1
    MIN_HOLD_SECONDS_FOR_PROFIT      = 1800
    HEARTBEAT_INTERVAL               = 5
    PNL_CACHE_TTL                    = 3
    PERIODIC_PNL_SNAPSHOT_INTERVAL   = 30


INDIA_HOLIDAYS     = holidays.India()
EXTRA_NSE_HOLIDAYS = set()


# ===================== SAFE JSON SERIALIZATION =====================
def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return str(obj)
    else:
        return obj


# ===================== LOGGING (DB-BASED) =====================
class DBLogger:
    def __init__(self, user):
        self.user = user

    def _write(self, level: str, msg: str, details: dict = None):
        ts           = datetime.now(Config.TIMEZONE)
        safe_details = make_json_safe(details) if details else {}
        line         = f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}"
        if safe_details:
            line += f" | {json.dumps(safe_details)}"
        print(line, flush=True)
        try:
            LogEntry.objects.create(
                user=self.user,
                level=level,
                message=msg,
                details=safe_details or {}
            )
        except Exception as e:
            print(f"[DB_LOG_FAIL] {str(e)}", flush=True)

    def info(self, msg: str, details: dict = None):
        self._write("INFO", msg, details)

    def warning(self, msg: str, details: dict = None):
        self._write("WARNING", msg, details)

    def error(self, msg: str, details: dict = None):
        self._write("ERROR", msg, details)

    def critical(self, msg: str, details: dict = None):
        self._write("CRITICAL", msg, details)

    # ── FIX: trade() only logs to LogEntry — does NOT create Trade records.
    #         Trade record creation is handled exclusively by _get_or_create_trade().
    #         Previously this method also called Trade.objects.create(), causing
    #         3 duplicate Trade rows per leg (one here + one in _get_or_create_trade
    #         + one from the logger.trade() call in order_basket()).
    def trade(self, action: str, symbol="", qty=0, price=0.0, comment=""):
        self.info(f"ORDER FILLED: {action} {symbol} qty={qty} @ {price}", {
            'action':  action,
            'symbol':  symbol,
            'quantity': qty,
            'price':   price,
            'comment': comment,
        })

    def big_banner(self, msg: str):
        print("\n" + "=" * 80, flush=True)
        print(f"*** {msg} ***".center(80), flush=True)
        print("=" * 80 + "\n", flush=True)
        self.info(f"BANNER: {msg}")


# ===================== STATE (DB-BASED) =====================
DEFAULT_STATE = {
    "trade_active":               False,
    "trade_taken_today":          False,
    "entry_date":                 None,
    "trade_symbols":              [],
    "positions":                  {},
    "position_qty":               {},
    "algo_legs":                  {},
    "margin_used":                0,
    "realistic_margin":           90000,
    "exact_margin_used_by_trade": 0,
    "final_margin_used":          0.0,
    "margin_per_lot":             0,
    "entry_vix":                  None,
    "entry_spot":                 None,
    "entry_atm":                  None,
    "entry_premiums":             {},
    "last_reset":                 None,
    "profit_target_rupee":        0.0,
    "target_frozen":              False,
    "qty":                        0,
    "adjustments_today":          {"ce": 0, "pe": 0},
    "last_adjustment_date":       None,
    "realized_pnl":               0.0,
    "last_spot":                  None,
    "bot_order_ids":              [],
    "entry_time":                 None,
    "exit_final_pnl":             0.0,
}


class DBState:
    def __init__(self, user):
        self.user          = user
        self.bot_status, _ = BotStatus.objects.get_or_create(user=user)
        self.data          = self.load()

    def load(self):
        # Use DEFAULT_STATE as single source of truth — no duplication
        state_data = {}
        if hasattr(self.bot_status, 'load_state'):
            state_data = self.bot_status.load_state() or {}
        else:
            state_data = getattr(self.bot_status, 'state_json', {}) or {}

        # Start from a clean default, then overlay saved values
        result = dict(DEFAULT_STATE)
        for k in DEFAULT_STATE:
            if k in state_data:
                result[k] = state_data[k]
        return result

    def save(self):
        data_to_save = make_json_safe(self.data)
        if hasattr(self.bot_status, 'save_state'):
            self.bot_status.save_state(data_to_save)
        else:
            self.bot_status.state_json = data_to_save
            self.bot_status.save(update_fields=['state_json'])
        self.bot_status.current_margin = Decimal(str(self.data.get("exact_margin_used_by_trade", 0)))
        self.bot_status.save(update_fields=['current_margin'])

    def daily_reset(self):
        today     = datetime.now(Config.TIMEZONE).date()
        today_str = str(today)
        if self.bot_status.entry_attempted_date != today:
            self.bot_status.entry_attempted_date  = None
            self.bot_status.last_successful_entry = None
            self.bot_status.save(update_fields=['entry_attempted_date', 'last_successful_entry'])
            print(f"[DAILY RESET] Cleared entry lock for new day: {today_str}", flush=True)
        if self.data.get("last_reset") != today_str:
            self.data.update({
                "trade_taken_today":    False,
                "adjustments_today":    {"ce": 0, "pe": 0},
                "last_adjustment_date": None,
                "exit_final_pnl":       0.0,
            })
            self.data["last_reset"] = today_str
            self.save()

    def full_reset(self):
        # Use DEFAULT_STATE — single source of truth, no duplication
        for k, v in DEFAULT_STATE.items():
            # Preserve daily tracking fields that should survive a full_reset
            if k not in ("last_reset", "last_spot"):
                self.data[k] = v
        self.save()
        # Do NOT clear entry_attempted_date here — prevents same-day re-entry.
        # Cleared by daily_reset() on the next calendar day.


# ===================== HELPER =====================
def create_leg(symbol: str, side: str, qty: int, entry_price: float):
    return {
        "symbol":         symbol,
        "side":           side,
        "qty":            qty,
        "entry_price":    entry_price,
        "exit_price":     0.0,
        "status":         "OPEN",
        "last_known_ltp": entry_price,
    }


# ===================== ENGINE =====================
class Engine:
    def __init__(self, user, broker, logger):
        self.user        = user
        self.broker      = broker
        self.logger      = logger
        self.kite        = KiteConnect(api_key=broker.api_key)
        self.state       = DBState(user)
        self.instruments = None
        self.weekly_df   = None
        self._last_valid_vix = None
        self.ltp_cache   = {}
        self.last_pnl    = 0.0

        self.logger.critical("ENGINE INIT - Starting authentication...")
        auth_success = self._authenticate()
        if auth_success and self.kite.access_token:
            self.logger.critical("ENGINE INIT - AUTH SUCCESS")
        else:
            self.logger.critical("ENGINE INIT - AUTH FAILED")

        today = datetime.now(Config.TIMEZONE).date()
        if self.state.bot_status.entry_attempted_date != today:
            self.state.bot_status.entry_attempted_date  = None
            self.state.bot_status.last_successful_entry = None
            self.state.bot_status.save(update_fields=['entry_attempted_date', 'last_successful_entry'])

    # ── AUTH ──────────────────────────────────────────────────────────────
    def _authenticate(self):
        try:
            access_token = generate_and_set_access_token_db(kite=self.kite, broker=self.broker)
            if access_token:
                self.kite.set_access_token(access_token)
                self.logger.info("Authentication successful")
                return True
            if self.broker.access_token:
                self.kite.set_access_token(self.broker.access_token)
                self.logger.info("Authentication using existing stored token")
                return True
            return False
        except Exception as e:
            self.logger.error("Auth error", {"error": str(e)})
            try:
                if self.broker.access_token:
                    self.kite.set_access_token(self.broker.access_token)
                    self.logger.info("Auth fallback to stored token after exception")
                    return True
            except Exception:
                pass
            return False

    # ── TRADE RECORD HELPERS ──────────────────────────────────────────────
    def _get_or_create_trade(self, symbol, side, qty, price, order_id=None):
        """
        Single authoritative method for creating/finding a Trade DB record.
        Called ONLY from order() and order_basket() after a fill is confirmed.
        logger.trade() no longer creates Trade records — it only logs to LogEntry.
        """
        direction   = 'SHORT' if side == 'SELL' else 'LONG'
        option_type = 'CE' if 'CE' in symbol else 'PE' if 'PE' in symbol else None

        # Primary match: by order_id (unique, most reliable)
        if order_id:
            existing = Trade.objects.filter(
                user=self.user,
                order_id=str(order_id)
            ).first()
            if existing:
                return existing

        # Secondary match: open record for same symbol with no exit
        existing = Trade.objects.filter(
            user=self.user,
            symbol=symbol,
            status='EXECUTED',
            exit_time__isnull=True,
            direction=direction,
        ).order_by('-created_at').first()
        if existing:
            return existing

        # Create new record — this is the ONLY place Trade records are created
        trade_id = f"{self.user.id}_{int(time.time())}_{symbol}"
        trade = Trade.objects.create(
            user=self.user,
            trade_id=trade_id,
            symbol=symbol,
            direction=direction,
            option_type=option_type,
            quantity=qty if direction == 'LONG' else -qty,
            entry_price=Decimal(str(price)),
            entry_time=timezone.now(),
            status='EXECUTED',
            broker='ZERODHA',
            order_id=str(order_id) if order_id else None,
            metadata={'order_id': order_id, 'side': side},
            algorithm_name='Hedged Short Strangle'
        )
        self.logger.info(
            f"Trade record created: {side} {symbol} {qty} @ {price}",
            {'trade_id': trade.trade_id, 'order_id': order_id}
        )
        return trade

    def _close_trade_record(self, symbol, exit_price):
        """Close an open Trade record by symbol."""
        try:
            trade = Trade.objects.filter(
                user=self.user,
                symbol=symbol,
                status='EXECUTED',
                exit_time__isnull=True
            ).order_by('-created_at').first()

            if trade is None:
                self.logger.warning(f"No open Trade record to close for {symbol}")
                return Decimal('0.00')

            trade.close_trade(Decimal(str(exit_price)), timezone.now())
            self.logger.info(f"Closed trade {trade.trade_id} | PnL: {trade.pnl}")
            return trade.pnl
        except Exception as e:
            self.logger.error(f"Failed to close trade for {symbol}", {"error": str(e)})
            return Decimal('0.00')

    # ── CAPITAL ───────────────────────────────────────────────────────────
    def capital_available(self) -> float:
        try:
            margins      = self.kite.margins()["equity"]
            live_balance = margins["available"].get("live_balance", 0)
            collateral   = margins["available"].get("collateral", 0)
            total_usable = live_balance + collateral
            self.logger.info("TOTAL USABLE CAPITAL FOR LOT SIZING", {
                "live_balance":         round(live_balance),
                "collateral_component": round(collateral),
                "total_usable_for_mis": round(total_usable)
            })
            return total_usable
        except Exception as e:
            self.logger.warning("Failed to fetch capital - using 0", {"error": str(e)})
            return 0.0

    def actual_used_capital(self) -> float:
        try:
            margins = self.kite.margins()["equity"]["utilised"]
            return margins["span"] + margins["exposure"]
        except Exception as e:
            self.logger.warning("Failed to fetch used capital", {"error": str(e)})
            return 0.0

    # ── TRADING DAY CHECK ─────────────────────────────────────────────────
    def is_trading_day(self) -> tuple:
        today = datetime.now(Config.TIMEZONE).date()
        if today.weekday() >= 5:
            return False, "Weekend"
        if today in INDIA_HOLIDAYS:
            holiday_name = INDIA_HOLIDAYS.get(today) or "Indian Public Holiday"
            return False, f"Holiday: {holiday_name}"
        if today in EXTRA_NSE_HOLIDAYS:
            return False, "Manual NSE holiday override"
        return True, "Trading day"

    # ── INSTRUMENTS ───────────────────────────────────────────────────────
    def load_instruments(self):
        try:
            df = pd.DataFrame(self.kite.instruments(Config.EXCHANGE))
            df = df[df["name"] == Config.UNDERLYING]
            df = df[df["instrument_type"].isin(["CE", "PE"])]
            df['strike'] = df['tradingsymbol'].str.extract(r'(\d{4,5})(CE|PE)$')[0].astype(int)
            df['expiry'] = pd.to_datetime(df['expiry']).dt.date
            self.instruments = df
            self.logger.info(f"Loaded {len(df)} options")
        except Exception as e:
            self.logger.critical("Instrument load failed", {
                "error": str(e), "trace": traceback.format_exc()
            })
            raise RuntimeError("Instruments load failed")

    def load_weekly_df(self):
        if self.instruments is None or self.instruments.empty:
            self.weekly_df = pd.DataFrame()
            return
        try:
            today              = datetime.now(Config.TIMEZONE).date()
            available_expiries = sorted(self.instruments["expiry"].unique())
            target_expiry      = None
            for exp in available_expiries:
                if exp >= today:
                    target_expiry = exp
                    break
            if target_expiry is None:
                self.logger.error("No future expiry found in instruments")
                self.weekly_df = pd.DataFrame()
                return
            self.weekly_df = self.instruments[
                self.instruments["expiry"] == target_expiry
            ].copy()
            self.logger.info("Weekly cache updated", {
                "expiry":  target_expiry.strftime("%Y-%m-%d"),
                "count":   len(self.weekly_df),
            })
        except Exception as e:
            self.logger.error("Weekly cache failed", {
                "error": str(e), "trace": traceback.format_exc()
            })
            self.weekly_df = pd.DataFrame()

    def get_current_expiry_date(self) -> Optional[object]:
        if self.weekly_df is None or self.weekly_df.empty:
            return None
        return self.weekly_df["expiry"].iloc[0]

    def calculate_trading_days_including_today(self, start_date) -> int:
        expiry = self.get_current_expiry_date()
        if not expiry:
            return 1
        current = start_date
        count   = 0
        while current <= expiry:
            if (current.weekday() < 5 and
                    current not in INDIA_HOLIDAYS and
                    current not in EXTRA_NSE_HOLIDAYS):
                count += 1
            current += timedelta(days=1)
        return max(count, 1)

    # ── MARKET DATA ───────────────────────────────────────────────────────
    def spot(self) -> Optional[float]:
        time.sleep(0.3)
        try:
            price = self.kite.quote(Config.SPOT_SYMBOL)[Config.SPOT_SYMBOL]["last_price"]
            if price:
                self.state.data["last_spot"] = price
                self.state.save()
            return price
        except Exception as e:
            self.logger.warning("Spot fetch failed", {
                "error_type": type(e).__name__, "error": str(e)
            })
            return self.state.data.get("last_spot")

    def vix(self) -> Optional[float]:
        time.sleep(0.3)
        try:
            quote = self.kite.quote(Config.VIX_SYMBOL)
            v     = quote[Config.VIX_SYMBOL]["last_price"]
            if v and v > 0:
                self._last_valid_vix = v
                return v
        except Exception as e:
            self.logger.warning("VIX fetch failed", {"error": str(e)})
        return self._last_valid_vix

    def bulk_ltp(self, symbols: List[str]) -> Dict[str, float]:
        if not symbols:
            return {}
        now      = time.time()
        result   = {}
        to_fetch = []
        for sym in symbols:
            if sym is None:
                continue
            cache_entry = self.ltp_cache.get(sym.strip())
            if cache_entry and now - cache_entry['ts'] < Config.PNL_CACHE_TTL:
                result[sym] = cache_entry['price']
            else:
                to_fetch.append(sym.strip())
        if not to_fetch:
            return result
        for attempt in range(3):
            try:
                time.sleep(0.3 * attempt)
                quotes      = self.kite.quote([f"{Config.EXCHANGE}:{s}" for s in to_fetch])
                valid_count = 0
                for full, data in quotes.items():
                    sym   = full.split(':')[1]
                    price = data.get('last_price', 0.0)
                    if price > 0:
                        result[sym]         = price
                        self.ltp_cache[sym] = {'price': price, 'ts': now}
                        valid_count        += 1
                    else:
                        last_good = self.ltp_cache.get(sym, {'price': 0.0})['price']
                        if last_good > 0:
                            result[sym] = last_good
                        else:
                            for leg in self.state.data.get("algo_legs", {}).values():
                                if leg.get("symbol") == sym:
                                    result[sym] = leg.get("entry_price", 0.0)
                                    break
                            else:
                                result[sym] = 0.0
                self.logger.info("bulk_ltp success", {
                    "attempt":      attempt + 1,
                    "fetched":      len(to_fetch),
                    "valid_prices": valid_count
                })
                return result
            except Exception as e:
                self.logger.warning(f"bulk_ltp attempt {attempt+1} failed", {
                    "error": str(e), "symbols_count": len(to_fetch)
                })
                if attempt == 2:
                    self.logger.error("bulk_ltp failed after 3 attempts - using fallbacks")
        return result

    def get_ltp_with_retry(self, symbol: str, retries: int = 3, delay: float = 0.3) -> float:
        if not symbol:
            return 0.0
        prices = []
        for _ in range(retries):
            val = self.bulk_ltp([symbol]).get(symbol, 0.0)
            if val > 0:
                prices.append(val)
            time.sleep(delay)
        return sum(prices) / len(prices) if prices else 0.0

    # ── STRIKE FINDERS ────────────────────────────────────────────────────
    def find_option_symbol(self, strike: int, cp: str) -> Optional[str]:
        if self.weekly_df is None or self.weekly_df.empty:
            return None
        df = self.weekly_df[
            (self.weekly_df["strike"] == strike) &
            (self.weekly_df["instrument_type"] == cp)
        ]
        return df.iloc[0]["tradingsymbol"] if not df.empty else None

    def find_short_strike(self, atm_strike: int, cp: str) -> int:
        direction = 1 if cp == "CE" else -1
        target    = atm_strike + direction * Config.MAIN_DISTANCE
        target    = int(round(target / 50) * 50)

        sym = self.find_option_symbol(target, cp)
        if sym:
            prem = self.bulk_ltp([sym]).get(sym, 0)
            if prem > 0.5:
                self.logger.info(f"Short {cp} found at exact target {target} (premium {prem:.2f})")
                return target
            else:
                self.logger.warning(
                    f"Short {cp} target {target} has low/zero premium {prem:.2f} — trying offsets"
                )
        else:
            self.logger.warning(
                f"Short {cp} symbol not found for strike {target} — trying offsets"
            )

        for offset in [50, -50, 100, -100, 150, -150, 200, -200]:
            test_strike = target + offset
            if (cp == "CE" and test_strike <= atm_strike) or \
               (cp == "PE" and test_strike >= atm_strike):
                continue
            sym = self.find_option_symbol(test_strike, cp)
            if sym:
                prem = self.bulk_ltp([sym]).get(sym, 0)
                if prem > 0:
                    self.logger.info(
                        f"Short {cp} fallback to {test_strike} (premium {prem:.2f})"
                    )
                    return test_strike

        self.logger.warning(
            f"No tradeable short {cp} found near {target} — using rounded target anyway"
        )
        return target

    def find_hedge_strike(
        self, short_strike: int, cp: str,
        common_target_prem: float, simulate: bool = False
    ) -> int:
        direction = 1 if cp == "CE" else -1
        df        = self.weekly_df[(self.weekly_df["instrument_type"] == cp)].copy()
        df        = df[df["strike"] > short_strike] if cp == "CE" \
                    else df[df["strike"] < short_strike]
        df        = df.sort_values("strike", ascending=(cp == "PE"))

        best_strike = None
        best_diff   = float('inf')
        best_actual = 0.0
        symbols     = [row["tradingsymbol"] for _, row in df.iterrows()]
        ltps        = self.bulk_ltp(symbols)

        for _, row in df.iterrows():
            sym  = row["tradingsymbol"]
            prem = ltps.get(sym, 0.0)
            if prem <= 0.5:
                continue
            if prem > common_target_prem * Config.MAX_OVERPAY_MULT * 2:
                continue
            diff = abs(prem - common_target_prem)
            if diff < best_diff:
                best_diff   = diff
                best_strike = row["strike"]
                best_actual = prem

        if best_strike is not None:
            if not simulate:
                self.logger.info("Symmetric hedge selected", {
                    "side":          cp,
                    "common_target": round(common_target_prem, 2),
                    "strike":        best_strike,
                    "actual":        round(best_actual, 2),
                    "diff":          round(best_actual - common_target_prem, 2)
                })
            return best_strike

        # Fallback: first strike with any positive premium
        fallback_strike = None
        candidates = sorted(df["strike"].unique().tolist()) if cp == "CE" \
                     else sorted(df["strike"].unique().tolist(), reverse=True)
        for s in candidates:
            sym  = self.find_option_symbol(s, cp)
            if sym:
                prem = ltps.get(sym, 0.0)
                if prem > 0:
                    fallback_strike = s
                    break

        if fallback_strike is None:
            fallback_strike = short_strike + direction * 1000

        if not simulate:
            self.logger.warning("Hedge fallback used", {
                "side":            cp,
                "short_strike":    short_strike,
                "fallback_strike": fallback_strike
            })
        return fallback_strike

    # ── MARGIN ────────────────────────────────────────────────────────────
    def exact_margin_for_basket(self, legs: List[dict]) -> Tuple[float, float]:
        formatted_legs = []
        for leg in legs:
            formatted_legs.append({
                "exchange":         leg.get("exchange", Config.EXCHANGE),
                "tradingsymbol":    leg["tradingsymbol"],
                "transaction_type": leg["transaction_type"],
                "variety":          "regular",
                "product":          "MIS",
                "order_type":       "MARKET",
                "quantity":         int(leg["quantity"]),
                "price":            0.0,
                "trigger_price":    0.0
            })
        try:
            response = self.kite.basket_order_margins(
                formatted_legs, consider_positions=True
            )
            initial = response["initial"]["total"]
            final   = response["final"]["total"]
            self.logger.info("Margin API Success", {
                "initial_margin": round(initial),
                "final_margin":   round(final),
                "legs":           len(formatted_legs)
            })
            return initial, final
        except Exception as e:
            self.logger.warning(
                "Margin API Failed - using conservative fallback", {"error": str(e)}
            )
            fallback = Config.MIN_CAPITAL_FOR_1LOT * 1.8
            return fallback, fallback * 0.85

    def calculate_lots(self, legs: List[dict]) -> int:
        # ── Step 1: Read hard cap from dashboard (BotStatus.max_lots_hard_cap) ─────────
        try:
            hard_cap = int(self.state.bot_status.max_lots_hard_cap)
            if hard_cap < 1:
                hard_cap = 1
        except (AttributeError, TypeError, ValueError, BotStatus.DoesNotExist):
            hard_cap = 1

        self.logger.info("Lot sizing: hard cap from dashboard", {
            "max_lots_hard_cap": hard_cap
        })

        # ── Step 2: Check minimum capital ────────────────────────────────────────────
        capital = self.capital_available()
        if capital < Config.MIN_CAPITAL_FOR_1LOT:
            self.logger.warning("Capital too low for even 1 lot", {
                "available_capital": round(capital),
                "minimum_required":  Config.MIN_CAPITAL_FOR_1LOT
            })
            return 0

        # ── Step 3: Get margin for 1 lot from basket API ─────────────────────────
        try:
            one_lot_legs           = [dict(l, quantity=Config.LOT_SIZE) for l in legs]
            initial_margin_1lot, _ = self.exact_margin_for_basket(one_lot_legs)
        except Exception as e:
            self.logger.error("Margin calculation failed", {"error": str(e)})
            return 0

        if initial_margin_1lot <= 0:
            self.logger.error("Margin API returned 0 — cannot size lots safely")
            return 0

        # ── Step 4: How many lots can capital afford? ───────────────────────────
        affordable_lots = int((capital * Config.MAX_CAPITAL_USAGE) // initial_margin_1lot)

        # ── Step 5: Apply all caps in order of priority ─────────────────────────
        final_lots = min(affordable_lots, hard_cap, Config.MAX_LOTS)
        final_lots = max(final_lots, 0)

        self.logger.info("Lot sizing result", {
            "available_capital":  round(capital),
            "margin_per_lot":     round(initial_margin_1lot),
            "affordable_lots":    affordable_lots,
            "dashboard_hard_cap": hard_cap,
            "config_max_lots":    Config.MAX_LOTS,
            "final_lots":         final_lots,
            "final_qty":          final_lots * Config.LOT_SIZE
        })
        return final_lots

    # ── SINGLE ORDER ─────────────────────────────────────────────────────
    def order(self, symbol: str, side: str, qty: int) -> Tuple[bool, str, float]:
        """Place a single market order and wait for fill."""
        filled_price = 0.0
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.logger.info(
                    f"PLACING ORDER attempt {attempt+1}/{Config.MAX_RETRIES}", {
                        "symbol": symbol, "side": side, "qty": qty
                    }
                )
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=Config.EXCHANGE,
                    tradingsymbol=symbol,
                    transaction_type=getattr(self.kite, f"TRANSACTION_TYPE_{side}"),
                    quantity=qty,
                    product=self.kite.PRODUCT_MIS,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                self.logger.info(f"Order placed successfully - ID: {order_id}")
                start_time = time.time()
                while time.time() - start_time < Config.ORDER_TIMEOUT:
                    history     = self.kite.order_history(order_id)
                    if not history:
                        time.sleep(0.5)
                        continue
                    last_status = history[-1]['status']
                    self.logger.info(f"Order status: {last_status}")
                    if last_status == 'COMPLETE':
                        filled_price = history[-1].get('average_price', 0.0)
                        if filled_price == 0.0:
                            self.logger.warning(
                                f"Order completed but average_price 0 for {symbol} - using LTP fallback"
                            )
                            filled_price = self.bulk_ltp([symbol]).get(symbol, 0.0)

                        self._get_or_create_trade(symbol, side, qty, filled_price, order_id)
                        self.logger.trade(
                            f"{side}_{symbol}", symbol,
                            qty if side == "BUY" else -qty,
                            filled_price, f"order_id:{order_id}"
                        )
                        return True, order_id, filled_price
                    elif last_status in ['REJECTED', 'CANCELLED']:
                        reason = history[-1].get('status_message', 'No reason provided')
                        self.logger.critical(
                            f"ORDER {last_status} for {symbol} - Reason: {reason}"
                        )
                        raise Exception(f"Order {last_status}: {reason}")
                    time.sleep(0.5)
                self.logger.error(
                    f"Order timeout - not completed within {Config.ORDER_TIMEOUT}s"
                )
                raise Exception("Order timeout")
            except Exception as e:
                error_msg = str(e)
                self.logger.error(
                    f"Order placement failed for {symbol} ({side}) attempt {attempt+1}", {
                        "error": error_msg, "trace": traceback.format_exc()
                    }
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                else:
                    self.logger.critical(
                        f"FINAL FAILURE: Giving up on {symbol} after {Config.MAX_RETRIES} attempts"
                    )
                    return False, "", 0.0
        return False, "", 0.0

    # ── BASKET ORDER ──────────────────────────────────────────────────────
    def order_basket(
        self, legs: List[Tuple[str, str]], qty: int
    ) -> Tuple[bool, List[str], Dict[str, float]]:
        """
        Place multiple legs simultaneously using ThreadPoolExecutor.
        Returns (success, order_ids_list, filled_prices_dict).
        """
        order_ids     = []
        filled_prices = {}

        for attempt in range(Config.MAX_RETRIES):
            try:
                order_ids     = []
                filled_prices = {}
                placed        = {}   # sym -> (side, order_id)

                self.logger.info(
                    f"PLACING BASKET attempt {attempt+1}/{Config.MAX_RETRIES}", {
                        "legs":        [{"symbol": s, "side": sd} for s, sd in legs],
                        "qty_per_leg": qty,
                        "total_legs":  len(legs)
                    }
                )

                # ── Fire all legs concurrently ────────────────────────────
                futures_map = {}
                with ThreadPoolExecutor(max_workers=len(legs)) as executor:
                    for sym, side in legs:
                        future = executor.submit(
                            self.kite.place_order,
                            variety=self.kite.VARIETY_REGULAR,
                            exchange=Config.EXCHANGE,
                            tradingsymbol=sym,
                            transaction_type=getattr(
                                self.kite, f"TRANSACTION_TYPE_{side}"
                            ),
                            quantity=qty,
                            product=self.kite.PRODUCT_MIS,
                            order_type=self.kite.ORDER_TYPE_MARKET
                        )
                        futures_map[future] = (sym, side)

                    for future in as_completed(futures_map):
                        sym, side = futures_map[future]
                        try:
                            oid = future.result()
                            placed[sym] = (side, oid)
                            self.logger.info(
                                f"Basket leg accepted: {side} {sym} → order_id={oid}"
                            )
                        except Exception as place_err:
                            self.logger.critical(
                                f"Basket leg FAILED at placement: {side} {sym}", {
                                    "error": str(place_err)
                                }
                            )
                            for s2, (sd2, oid2) in placed.items():
                                opp = "SELL" if sd2 == "BUY" else "BUY"
                                self.logger.critical(
                                    f"Basket placement cleanup: reversing {sd2} {s2}"
                                )
                                try:
                                    self.order(s2, opp, qty)
                                except Exception as cleanup_err:
                                    self.logger.critical(
                                        f"Basket cleanup also failed for {s2}",
                                        {"error": str(cleanup_err)}
                                    )
                            raise place_err

                # ── Poll all placed orders until COMPLETE ─────────────────
                start_time = time.time()
                pending    = dict(placed)

                while pending and time.time() - start_time < Config.ORDER_TIMEOUT:
                    for sym in list(pending.keys()):
                        side, oid = pending[sym]
                        try:
                            history = self.kite.order_history(oid)
                        except Exception:
                            continue
                        if not history:
                            continue
                        last_status = history[-1]['status']
                        self.logger.info(
                            f"Basket leg status: {side} {sym} → {last_status}"
                        )
                        if last_status == 'COMPLETE':
                            fp = history[-1].get('average_price', 0.0)
                            if fp == 0.0:
                                self.logger.warning(
                                    f"Basket leg average_price=0 for {sym} — using LTP fallback"
                                )
                                fp = self.bulk_ltp([sym]).get(sym, 0.0)

                            order_ids.append(oid)
                            filled_prices[sym] = fp

                            self._get_or_create_trade(sym, side, qty, fp, oid)
                            self.logger.trade(
                                f"{side}_{sym}", sym,
                                qty if side == "BUY" else -qty,
                                fp, f"order_id:{oid}"
                            )
                            del pending[sym]
                        elif last_status in ['REJECTED', 'CANCELLED']:
                            reason = history[-1].get('status_message', 'No reason')
                            raise Exception(
                                f"Basket leg {last_status}: {sym} — {reason}"
                            )
                    if pending:
                        time.sleep(0.5)

                if pending:
                    raise Exception(
                        f"Basket order timeout — still pending: {list(pending.keys())}"
                    )

                self.logger.info(
                    f"Basket order fully complete — {len(filled_prices)} legs filled", {
                        "filled_prices": {
                            k: round(v, 2) for k, v in filled_prices.items()
                        }
                    }
                )
                return True, order_ids, filled_prices

            except Exception as e:
                self.logger.error(
                    f"Basket order attempt {attempt+1} failed", {
                        "error": str(e), "trace": traceback.format_exc()
                    }
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                else:
                    self.logger.critical(
                        f"Basket order FINAL FAILURE after {Config.MAX_RETRIES} attempts"
                    )
                    return False, [], {}

        return False, [], {}

    def cleanup(self, executed: List[Tuple[str, str, str]], qty: int):
        """Reverse already-filled legs on partial entry failure."""
        for sym, side, _ in executed:
            opp = "SELL" if side == "BUY" else "BUY"
            self.logger.critical(f"CLEANUP: reversing {side} on {sym}")
            self.order(sym, opp, qty)

    # ── PNL ───────────────────────────────────────────────────────────────
    def algo_pnl(self) -> float:
        legs = self.state.data.get("algo_legs", {})
        if not legs:
            self.logger.info("No active trade - PnL = 0")
            return 0.0

        open_symbols = [
            leg["symbol"].strip()
            for leg in legs.values()
            if leg.get("symbol") and leg.get("status") != "CLOSED"
        ]
        ltps           = self.bulk_ltp(open_symbols) if open_symbols else {}
        total          = 0.0
        fallback_count = 0

        for leg in legs.values():
            sym = leg.get("symbol", "").strip()
            if not sym:
                continue

            if leg.get("status") == "CLOSED":
                price = leg.get("exit_price", 0.0)
            else:
                ltp_val = ltps.get(sym)
                if ltp_val is None or ltp_val <= 0:
                    fallback_count += 1
                    ltp_val = leg.get("last_known_ltp", leg["entry_price"])
                price = ltp_val
                if price > 0 and price != leg["entry_price"]:
                    leg["last_known_ltp"] = price

            qty_abs = abs(leg["qty"])
            if leg["side"] == "SELL":
                total += (leg["entry_price"] - price) * qty_abs
            else:
                total += (price - leg["entry_price"]) * qty_abs

        if fallback_count > 0:
            self.logger.warning(
                f"PNL CALC USED FALLBACK FOR {fallback_count}/{len(legs)} legs", {
                    "fallback_count": fallback_count,
                    "total_legs":     len(legs)
                }
            )
        return total

    # ── TARGET ────────────────────────────────────────────────────────────
    def lock_target(self, target_rupee: float):
        rounded_target = round(target_rupee)
        self.state.data["profit_target_rupee"] = rounded_target
        self.state.data["target_frozen"]        = True
        self.state.save()
        bot_status                     = self.state.bot_status
        bot_status.daily_profit_target = Decimal(str(rounded_target))
        bot_status.daily_stop_loss     = Decimal(str(-rounded_target))
        bot_status.save(update_fields=['daily_profit_target', 'daily_stop_loss'])
        self.logger.info("DAILY TARGET LOCKED", {
            "target_₹":    rounded_target,
            "stop_loss_₹": -rounded_target,
        })

    def update_daily_profit_target(self, force: bool = False):
        if self.state.data["target_frozen"] and not force:
            self.logger.info("Target already frozen today - skipping recalculation")
            return
        if not self.state.data["trade_active"]:
            return
        entry_vix = self.state.data.get("entry_vix")
        if entry_vix is None:
            self.logger.warning("Missing entry_vix - cannot calculate target")
            return
        if entry_vix <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            entry_prem  = self.state.data["entry_premiums"]
            net_per_lot = (
                entry_prem.get("ce_short", 0) + entry_prem.get("pe_short", 0) -
                entry_prem.get("ce_hedge", 0) - entry_prem.get("pe_hedge", 0)
            )
            qty            = self.state.data.get("qty", 0)
            total_credit   = net_per_lot * qty
            today          = datetime.now(Config.TIMEZONE).date()
            remaining_days = self.calculate_trading_days_including_today(today)
            today_target   = total_credit / remaining_days if remaining_days > 0 else total_credit
            self.logger.info("Low VIX mode - using Net Credit ÷ Days", {
                "entry_vix":     round(entry_vix, 2),
                "total_credit":  round(total_credit),
                "remaining_days": remaining_days,
            })
        else:
            margin_for_target = self.state.data.get("final_margin_used", 0.0)
            if margin_for_target <= 0:
                margin_for_target = self.actual_used_capital()
                self.logger.warning(
                    "final_margin_used not available → fallback to actual used capital"
                )
            today_target = margin_for_target * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            self.logger.info("High VIX mode - using 2.0% of final_margin", {
                "entry_vix":    round(entry_vix, 2),
                "final_margin": round(margin_for_target),
            })
        today_target *= 0.97
        today_target  = round(today_target)
        self.logger.info(
            "Target adjusted to 97%", {"final_daily_target_₹": today_target}
        )
        self.lock_target(today_target)

    # ── PREVIEW / BANNER ──────────────────────────────────────────────────
    def preview_profit_calculation(self):
        try:
            live_spot = self.spot()
            spot = live_spot or (
                self.state.data.get("last_spot") or
                self.state.data.get("entry_spot") or 24500
            )
            atm_strike = self.atm(spot)
            vix_val    = self.vix()

            ce_short     = self.find_short_strike(atm_strike, "CE")
            pe_short     = self.find_short_strike(atm_strike, "PE")
            ce_short_sym = self.find_option_symbol(ce_short, "CE")
            pe_short_sym = self.find_option_symbol(pe_short, "PE")
            ltps         = self.bulk_ltp([s for s in [ce_short_sym, pe_short_sym] if s])
            ce_short_p   = ltps.get(ce_short_sym, 0.0) if ce_short_sym else 0.0
            pe_short_p   = ltps.get(pe_short_sym, 0.0) if pe_short_sym else 0.0

            if ce_short_p <= 0 or pe_short_p <= 0:
                self.logger.warning("Short leg premiums not available - skipping preview")
                return

            ce_target     = ce_short_p * Config.HEDGE_PREMIUM_RATIO
            pe_target     = pe_short_p * Config.HEDGE_PREMIUM_RATIO
            common_target = min(ce_target, pe_target)
            ce_hedge      = self.find_hedge_strike(ce_short, "CE", common_target, simulate=True)
            pe_hedge      = self.find_hedge_strike(pe_short, "PE", common_target, simulate=True)
            ce_hedge_sym  = self.find_option_symbol(ce_hedge, "CE")
            pe_hedge_sym  = self.find_option_symbol(pe_hedge, "PE")
            ltps_hedge    = self.bulk_ltp([s for s in [ce_hedge_sym, pe_hedge_sym] if s])
            ce_hedge_p    = ltps_hedge.get(ce_hedge_sym, 0.0) if ce_hedge_sym else 0.0
            pe_hedge_p    = ltps_hedge.get(pe_hedge_sym, 0.0) if pe_hedge_sym else 0.0

            legs = []
            if pe_hedge_sym:
                legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym,
                             "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
            if ce_hedge_sym:
                legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym,
                             "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
            if pe_short_sym:
                legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym,
                             "transaction_type": "SELL", "quantity": Config.LOT_SIZE})
            if ce_short_sym:
                legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym,
                             "transaction_type": "SELL", "quantity": Config.LOT_SIZE})

            actual_lots  = self.calculate_lots(legs) if legs else 0
            preview_mode = actual_lots == 0
            if preview_mode:
                actual_lots = 1

            qty                = actual_lots * Config.LOT_SIZE
            net_credit_per_lot = ce_short_p + pe_short_p - ce_hedge_p - pe_hedge_p
            net_credit_total   = net_credit_per_lot * qty
            today              = datetime.now(Config.TIMEZONE).date()
            remaining_days     = self.calculate_trading_days_including_today(today)

            if vix_val and vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
                credit_based_today = (
                    net_credit_total / remaining_days if remaining_days > 0
                    else net_credit_total
                )
                projected_target = round(credit_based_today * 0.97)
                target_mode      = "theta ÷ days"
                target_display   = f"+₹{projected_target:,}"
            else:
                _, estimated_final = (
                    self.exact_margin_for_basket([dict(l, quantity=qty) for l in legs])
                    if legs else (0, 0)
                )
                capital_based_today = estimated_final * Config.PERCENT_TARGET_WHEN_VIX_HIGH
                projected_target    = round(capital_based_today * 0.97)
                target_mode         = "2.0% of final margin"
                target_display      = (
                    f"+₹{projected_target:,} "
                    f"(final margin ≈ ₹{round(estimated_final):,})"
                )

            mode      = "PREVIEW (ASSUMING 1 LOT)" if preview_mode else "LIVE PREVIEW"
            vix_note  = f" | VIX: {vix_val:.2f} → {target_mode}" if vix_val else ""
            self.logger.big_banner(
                f"{mode}{vix_note} | PROJECTED DAILY TARGET: {target_display} | "
                f"STOP LOSS: -{target_display.replace('+', '')}"
            )
            print(f"Remaining Trading Days till Expiry: {remaining_days}\n", flush=True)
            print("Proposed Legs (Live Premiums):", flush=True)
            print(f"  Sell CE {ce_short:5} → ₹{ce_short_p:.2f}", flush=True)
            print(f"  Sell PE {pe_short:5} → ₹{pe_short_p:.2f}", flush=True)
            print(f"  Buy  CE Hedge {ce_hedge or 'N/A'} → ₹{ce_hedge_p:.2f}", flush=True)
            print(f"  Buy  PE Hedge {pe_hedge or 'N/A'} → ₹{pe_hedge_p:.2f}", flush=True)
            print(
                f"\nNet Credit per lot: ₹{net_credit_per_lot:.2f} × "
                f"{actual_lots} lot(s) × {Config.LOT_SIZE} = ₹{net_credit_total:,.0f}",
                flush=True
            )
            print(f"Projected Daily Target: {target_display}", flush=True)
            print("\n" + "=" * 80 + "\n", flush=True)
        except Exception as e:
            self.logger.warning("preview_profit_calculation failed (non-fatal)", {
                "error": str(e), "trace": traceback.format_exc()
            })

    def startup_banner(self):
        try:
            now         = datetime.now(Config.TIMEZONE)
            spot        = self.spot() or 0
            vix         = self.vix()
            atm_strike  = self.atm(spot) if spot else None
            if self.weekly_df is None or self.weekly_df.empty:
                self.load_weekly_df()
            expiry_date = self.get_current_expiry_date()
            expiry_str  = (
                expiry_date.strftime("%d %b %Y (%A)").upper()
                if expiry_date else "N/A"
            )
            today          = now.date()
            remaining_days = self.calculate_trading_days_including_today(today)

            ce_short = pe_short = ce_hedge = pe_hedge = None
            ce_short_ltp = pe_short_ltp = ce_hedge_ltp = pe_hedge_ltp = "N/A"

            if spot and self.weekly_df is not None and not self.weekly_df.empty:
                ce_short      = self.find_short_strike(atm_strike or 0, "CE")
                pe_short      = self.find_short_strike(atm_strike or 0, "PE")
                ce_sym        = self.find_option_symbol(ce_short, "CE")
                pe_sym        = self.find_option_symbol(pe_short, "PE")
                ltps_short    = self.bulk_ltp([s for s in [ce_sym, pe_sym] if s])
                ce_short_ltp  = f"{ltps_short.get(ce_sym, 0.0):.2f}" if ce_sym else "N/A"
                pe_short_ltp  = f"{ltps_short.get(pe_sym, 0.0):.2f}" if pe_sym else "N/A"
                ce_short_p    = ltps_short.get(ce_sym, 100.0) if ce_sym else 100.0
                pe_short_p    = ltps_short.get(pe_sym, 100.0) if pe_sym else 100.0
                ce_target     = ce_short_p * Config.HEDGE_PREMIUM_RATIO
                pe_target     = pe_short_p * Config.HEDGE_PREMIUM_RATIO
                common_target = min(ce_target, pe_target)
                ce_hedge      = self.find_hedge_strike(ce_short, "CE", common_target, simulate=True)
                pe_hedge      = self.find_hedge_strike(pe_short, "PE", common_target, simulate=True)
                ce_hedge_sym  = self.find_option_symbol(ce_hedge, "CE")
                pe_hedge_sym  = self.find_option_symbol(pe_hedge, "PE")
                ltps_hedge    = self.bulk_ltp([s for s in [ce_hedge_sym, pe_hedge_sym] if s])
                ce_hedge_ltp  = (
                    f"{ltps_hedge.get(ce_hedge_sym, 0.0):.2f}" if ce_hedge_sym else "N/A"
                )
                pe_hedge_ltp  = (
                    f"{ltps_hedge.get(pe_hedge_sym, 0.0):.2f}" if pe_hedge_sym else "N/A"
                )

            spot_str = f"{spot:.1f}" if spot else "N/A"
            vix_str  = f"{vix:.2f}" if vix else "N/A"
            self.logger.info("=" * 80)
            self.logger.info("HEDGED SHORT STRANGLE - DJANGO VERSION")
            self.logger.info(
                f"Date: {now.strftime('%A, %d %B %Y')} | "
                f"Time: {now.strftime('%H:%M:%S')} IST"
            )
            self.logger.info(
                f"Spot: {spot_str} | ATM: {atm_strike or 'N/A'} | VIX: {vix_str}"
            )
            self.logger.info(
                f"Weekly Expiry: {expiry_str} | Remaining Days: {remaining_days}"
            )
            self.logger.info(
                f"SHORT CE: {ce_short or 'N/A'} ({ce_short_ltp}) | "
                f"HEDGE CE: {ce_hedge or 'N/A'} ({ce_hedge_ltp})"
            )
            self.logger.info(
                f"SHORT PE: {pe_short or 'N/A'} ({pe_short_ltp}) | "
                f"HEDGE PE: {pe_hedge or 'N/A'} ({pe_hedge_ltp})"
            )
            self.logger.info("=" * 80)
            self.preview_profit_calculation()
        except Exception as e:
            self.logger.warning("startup_banner failed (non-fatal)", {
                "error": str(e), "trace": traceback.format_exc()
            })

    def atm(self, spot: float) -> int:
        return int(round(spot / 50) * 50)

    # ── ENTER ─────────────────────────────────────────────────────────────
    def enter(self) -> bool:
        now   = datetime.now(Config.TIMEZONE)
        today = now.date()

        # =====================================================================
        # PHASE 1: Atomic DB lock — held for < 100ms, no slow work inside.
        #
        # select_for_update() serialises concurrent gevent coroutines at the DB
        # level. The ONLY thing we do inside the transaction is check + set
        # entry_attempted_date, then commit and release the row lock immediately.
        # All slow API calls, order placement, and sleeps happen in Phase 2 after
        # the lock is released — so other coroutines are never blocked waiting.
        # =====================================================================
        already_attempted = False
        try:
            with transaction.atomic():
                bot_status = BotStatus.objects.select_for_update().get(user=self.user)

                if bot_status.entry_attempted_date == today:
                    already_attempted = True

                elif self.state.data.get("trade_active"):
                    self.logger.info("Trade already active — skipping entry")
                    return False

                else:
                    # Stamp the date NOW, before the lock is released.
                    # Any coroutine that arrives after this commit will see
                    # entry_attempted_date == today and bail out immediately.
                    bot_status.entry_attempted_date = today
                    bot_status.save(update_fields=["entry_attempted_date"])
                    self.logger.critical(
                        "ENTRY LOCK SET & COMMITTED — no other coroutine can enter today.",
                        {"date_locked": str(today)}
                    )
                # Transaction commits here → DB row lock released.

        except BotStatus.DoesNotExist:
            self.logger.critical("BotStatus row missing — cannot enter safely")
            return False
        except Exception as e:
            self.logger.critical("ENTRY DB GATE FAILED — aborting", {
                "error": str(e), "trace": traceback.format_exc()
            })
            return False

        # Bail out AFTER the transaction so we never hold the lock during a log call.
        if already_attempted:
            self.logger.critical(
                "ENTRY ALREADY ATTEMPTED TODAY (DB gate) — ABORTING",
                {"locked_date": str(today)}
            )
            return False

        # =====================================================================
        # PHASE 2: Lock is released. We are the ONE coroutine allowed to trade
        # today. All slow work (API calls, order placement, sleeps) happens here.
        # =====================================================================

        # In-memory fast-path flag so the loop skips without a DB hit each tick.
        self.state.data["trade_taken_today"] = True
        self.state.save()

        entry_call_time = datetime.now(Config.TIMEZONE)
        self.logger.critical("=== ENTER() PROCEEDING ===", {
            "exact_time": entry_call_time.strftime("%H:%M:%S.%f")[:-3]
        })

        self.state.daily_reset()

        # ── VALIDATIONS ───────────────────────────────────────────────────
        is_ok, reason = self.is_trading_day()
        if not is_ok:
            self.logger.info("Non-trading day - skipping", {"reason": reason})
            return False

        now_time = now.time()
        if not (Config.ENTRY_START <= now_time <= Config.ENTRY_END):
            self.logger.info("Outside entry window", {
                "time": now_time.strftime("%H:%M:%S")
            })
            return False

        expiry = self.get_current_expiry_date()
        if expiry == today:
            self.logger.info("Expiry day - skipping entry")
            return False

        if today.weekday() == 1:
            self.logger.critical("Tuesday — no entry")
            return False

        vix_val = self.vix()
        if not vix_val or not (Config.VIX_MIN <= vix_val <= Config.VIX_MAX):
            self.logger.critical("VIX out of range — entry blocked", {"vix": vix_val})
            return False

        if self.weekly_df is None or self.weekly_df.empty:
            self.logger.critical("No weekly options loaded — forcing reload")
            self.load_instruments()
            self.load_weekly_df()
            if self.weekly_df is None or self.weekly_df.empty:
                self.logger.critical("Still no weekly options — cannot enter")
                return False

        spot = self.spot()
        if not spot:
            self.logger.critical("Spot fetch failed — cannot enter")
            return False

        # ── STRIKE SELECTION ──────────────────────────────────────────────
        atm_strike   = self.atm(spot)
        ce_short     = self.find_short_strike(atm_strike, "CE")
        pe_short     = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")

        self.logger.critical("STRIKE SELECTION", {
            "atm":             atm_strike,
            "ce_short_strike": ce_short,  "ce_short_sym": ce_short_sym,
            "pe_short_strike": pe_short,  "pe_short_sym": pe_short_sym,
        })

        if not ce_short_sym or not pe_short_sym:
            self.logger.critical("Option symbols not found for short strikes — cannot enter")
            return False

        ltps_short = self.bulk_ltp([ce_short_sym, pe_short_sym])
        ce_short_p = ltps_short.get(ce_short_sym, 0)
        pe_short_p = ltps_short.get(pe_short_sym, 0)

        if ce_short_p <= 0 or pe_short_p <= 0:
            self.logger.warning("Short premiums not live — cannot enter", {
                "ce_short_p": ce_short_p, "pe_short_p": pe_short_p
            })
            return False

        ce_target     = ce_short_p * Config.HEDGE_PREMIUM_RATIO
        pe_target     = pe_short_p * Config.HEDGE_PREMIUM_RATIO
        common_target = min(ce_target, pe_target)

        ce_hedge     = self.find_hedge_strike(ce_short, "CE", common_target, simulate=False)
        pe_hedge     = self.find_hedge_strike(pe_short, "PE", common_target, simulate=False)
        ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
        pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")

        ltps_hedge = self.bulk_ltp([s for s in [ce_hedge_sym, pe_hedge_sym] if s])
        ce_hedge_p = ltps_hedge.get(ce_hedge_sym, 0.0) if ce_hedge_sym else 0.0
        pe_hedge_p = ltps_hedge.get(pe_hedge_sym, 0.0) if pe_hedge_sym else 0.0

        if not ce_hedge_sym:
            self.logger.critical("CE hedge symbol not found — proceeding without CE hedge")
        if not pe_hedge_sym:
            self.logger.critical("PE hedge symbol not found — proceeding without PE hedge")

        # ── LOT SIZING ────────────────────────────────────────────────────
        legs = []
        if pe_hedge_sym:
            legs.append({
                "exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym,
                "transaction_type": "BUY",  "quantity": Config.LOT_SIZE, "product": "MIS"
            })
        if ce_hedge_sym:
            legs.append({
                "exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym,
                "transaction_type": "BUY",  "quantity": Config.LOT_SIZE, "product": "MIS"
            })
        legs.append({
            "exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym,
            "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"
        })
        legs.append({
            "exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym,
            "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"
        })

        lots = self.calculate_lots(legs)
        if lots == 0:
            self.logger.critical("Lot calculation returned 0 — cannot enter")
            return False

        qty                          = lots * Config.LOT_SIZE
        initial_margin, final_margin = self.exact_margin_for_basket(
            [dict(l, quantity=qty) for l in legs]
        )
        self.state.data["final_margin_used"] = final_margin

        capital_before = self.capital_available()
        self.logger.info("CAPITAL BEFORE ENTRY", {"available_Rs": round(capital_before)})

        # ── ORDER EXECUTION ───────────────────────────────────────────────
        # BUY hedges first (protection before short exposure), then SELL shorts.
        buy_legs  = []
        sell_legs = []
        if pe_hedge_sym:
            buy_legs.append((pe_hedge_sym, "BUY"))
        if ce_hedge_sym:
            buy_legs.append((ce_hedge_sym, "BUY"))
        sell_legs.append((pe_short_sym, "SELL"))
        sell_legs.append((ce_short_sym, "SELL"))

        self.logger.critical(
            f"PLACING 2 BASKETS — "
            f"{len(buy_legs)} BUY hedge(s) + {len(sell_legs)} SELL short(s) | "
            f"qty={qty} | lots={lots}"
        )

        executed     = []
        entry_prices = {}

        # Step 1: BUY hedges
        if buy_legs:
            buy_ok, buy_ids, buy_prices = self.order_basket(buy_legs, qty)
            if not buy_ok:
                self.logger.critical("BUY hedge basket FAILED — aborting entry cleanly")
                return False
            for (sym, side), oid in zip(buy_legs, buy_ids):
                executed.append((sym, side, oid))
            entry_prices.update(buy_prices)
            self.logger.critical("BUY hedge basket FILLED", {
                "prices": {k: round(v, 2) for k, v in buy_prices.items()}
            })

        # Step 2: SELL shorts
        sell_ok, sell_ids, sell_prices = self.order_basket(sell_legs, qty)
        if not sell_ok:
            self.logger.critical(
                f"SELL short basket FAILED — reversing {len(executed)} BUY hedge(s)"
            )
            self.cleanup(executed, qty)
            return False
        for (sym, side), oid in zip(sell_legs, sell_ids):
            executed.append((sym, side, oid))
        entry_prices.update(sell_prices)
        self.logger.critical("SELL short basket FILLED", {
            "prices": {k: round(v, 2) for k, v in sell_prices.items()}
        })

        self.logger.critical(f"ALL {len(executed)} LEGS FILLED SUCCESSFULLY")

        for sym, side, order_id in executed:
            self.state.data.setdefault("bot_order_ids", []).append(order_id)

        # ── POST-ENTRY STATE ──────────────────────────────────────────────
        time.sleep(3.0)

        margin_samples = []
        for _ in range(4):
            try:
                avail = self.capital_available()
                if avail > 0:
                    margin_samples.append(avail)
            except Exception:
                pass
            time.sleep(1.0)

        time.sleep(5)
        actual_margin_used                            = self.actual_used_capital()
        self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        margin_per_lot = actual_margin_used / lots if lots > 0 else 0

        trade_symbols = [
            s for s in [ce_short_sym, pe_short_sym, ce_hedge_sym, pe_hedge_sym] if s
        ]
        algo_legs = {
            "CE_SHORT": create_leg(ce_short_sym, "SELL", qty, entry_prices.get(ce_short_sym, 0)),
            "PE_SHORT": create_leg(pe_short_sym, "SELL", qty, entry_prices.get(pe_short_sym, 0)),
        }
        if ce_hedge_sym:
            algo_legs["CE_HEDGE"] = create_leg(
                ce_hedge_sym, "BUY", qty, entry_prices.get(ce_hedge_sym, 0)
            )
        if pe_hedge_sym:
            algo_legs["PE_HEDGE"] = create_leg(
                pe_hedge_sym, "BUY", qty, entry_prices.get(pe_hedge_sym, 0)
            )

        entry_time_str = datetime.now(Config.TIMEZONE).isoformat()
        self.state.data.update({
            "trade_active":               True,
            "trade_taken_today":          True,
            "entry_date":                 str(today),
            "trade_symbols":              trade_symbols,
            "algo_legs":                  algo_legs,
            "positions": {
                "ce_short": ce_short, "pe_short": pe_short,
                "lots": lots, "qty": qty
            },
            "realistic_margin":           max(final_margin * 0.6, 90000),
            "exact_margin_used_by_trade": actual_margin_used,
            "final_margin_used":          final_margin,
            "margin_per_lot":             margin_per_lot,
            "entry_vix":                  vix_val,
            "entry_spot":                 spot,
            "entry_atm":                  atm_strike,
            "entry_premiums": {
                "ce_short": entry_prices.get(ce_short_sym, 0),
                "pe_short": entry_prices.get(pe_short_sym, 0),
                "ce_hedge": entry_prices.get(ce_hedge_sym, 0) if ce_hedge_sym else 0,
                "pe_hedge": entry_prices.get(pe_hedge_sym, 0) if pe_hedge_sym else 0,
            },
            "qty":        qty,
            "entry_time": entry_time_str,
        })
        if ce_hedge:
            self.state.data["positions"]["ce_hedge"] = ce_hedge
        if pe_hedge:
            self.state.data["positions"]["pe_hedge"] = pe_hedge

        position_qty = {ce_short_sym: -qty, pe_short_sym: -qty}
        if ce_hedge_sym:
            position_qty[ce_hedge_sym] = +qty
        if pe_hedge_sym:
            position_qty[pe_hedge_sym] = +qty
        self.state.data["position_qty"] = position_qty

        try:
            self.state.bot_status.refresh_from_db()
            self.state.bot_status.last_successful_entry = timezone.now()
            self.state.bot_status.save(update_fields=["last_successful_entry"])
        except Exception as e:
            self.logger.warning("Could not update last_successful_entry", {"error": str(e)})

        self.update_daily_profit_target()
        final_target = self.state.data["profit_target_rupee"]
        self.logger.big_banner(
            f"ENTRY SUCCESS | {lots} lots | "
            f"TARGET: +Rs{final_target:,} | SL: -Rs{final_target:,}"
        )

        self.state.save()
        self.last_pnl = self.algo_pnl()
        time.sleep(2)
        return True

    # ── CHECK EXISTING POSITIONS ──────────────────────────────────────────
    def check_existing_positions(self) -> bool:
        try:
            net                   = self.kite.positions()["net"]
            current_trade_symbols = set(self.state.data.get("trade_symbols", []))
            if self.state.data.get("trade_active"):
                return True
            if current_trade_symbols:
                existing_symbols = {
                    p["tradingsymbol"]
                    for p in net
                    if p["product"] == "MIS" and p["quantity"] != 0
                }
                if current_trade_symbols.issubset(existing_symbols):
                    self.logger.info("Recovering bot trade after restart/crash")
                    self.state.data["trade_active"] = True
                    self.state.save()
                    return True
            return False
        except Exception as e:
            self.logger.error(
                "Failed to check existing positions", {"error": str(e)}
            )
            return True

    # ── DEFENSIVE ADJUSTMENT ──────────────────────────────────────────────
    def check_and_adjust_defensive(self) -> bool:
        if not self.state.data["trade_active"]:
            return False
        now_time = datetime.now(Config.TIMEZONE).time()
        if now_time >= Config.ADJUSTMENT_CUTOFF_TIME:
            return False

        today = datetime.now(Config.TIMEZONE).date()
        if self.state.data.get("last_adjustment_date") != str(today):
            self.state.data["adjustments_today"]    = {"ce": 0, "pe": 0}
            self.state.data["last_adjustment_date"] = str(today)

        spot = self.spot()
        if not spot:
            return False

        current_atm = self.atm(spot)
        pos         = self.state.data["positions"]
        qty         = self.state.data["qty"]
        algo_legs   = self.state.data.get("algo_legs", {})
        ce_short    = pos.get("ce_short")
        pe_short    = pos.get("pe_short")
        if not ce_short or not pe_short:
            return False

        adjusted = False

        # ── CE adjustment ──────────────────────────────────────────────────
        if (spot >= ce_short + Config.ADJUSTMENT_TRIGGER_POINTS and
                self.state.data["adjustments_today"]["ce"] < Config.MAX_ADJUSTMENTS_PER_SIDE_PER_DAY):

            self.logger.info("DEFENSIVE ADJUSTMENT: CE STRUCK", {
                "spot": spot, "old_strike": ce_short
            })
            old_sym = self.find_option_symbol(ce_short, "CE")
            if not old_sym:
                self.logger.error("CE adjustment: old symbol not found, skipping")
            else:
                success, _, filled_p = self.order(old_sym, "BUY", qty)
                if not success:
                    self.logger.critical(
                        "CE adjustment: failed to buy back old short — position unchanged"
                    )
                else:
                    for leg in algo_legs.values():
                        if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                            leg["exit_price"] = filled_p
                            leg["status"]     = "CLOSED"
                            break

                    hedge_strike = pos.get("ce_hedge")
                    new_ce_short = self.find_short_strike(current_atm, "CE")

                    if hedge_strike and abs(new_ce_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                        self.logger.warning(
                            "CE adjustment: new strike too close to hedge — restoring old short"
                        )
                        restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                        if restore_ok:
                            for leg in algo_legs.values():
                                if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                    leg["status"]      = "OPEN"
                                    leg["exit_price"]  = 0.0
                                    leg["entry_price"] = restore_p
                                    break
                        else:
                            self.logger.critical(
                                "CE adjustment: restore failed — attempting emergency exit"
                            )
                            self.exit("CE adjustment restore failed — emergency exit")
                    else:
                        new_sym = self.find_option_symbol(new_ce_short, "CE")
                        if not new_sym:
                            restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                            if restore_ok:
                                for leg in algo_legs.values():
                                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                        leg["status"]      = "OPEN"
                                        leg["exit_price"]  = 0.0
                                        leg["entry_price"] = restore_p
                                        break
                            else:
                                self.logger.critical(
                                    "CE adjustment: new symbol not found AND restore failed — emergency exit"
                                )
                                self.exit("CE adjustment new symbol not found — emergency exit")
                        else:
                            sell_ok, _, sell_filled_p = self.order(new_sym, "SELL", qty)
                            if not sell_ok:
                                restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                                if restore_ok:
                                    for leg in algo_legs.values():
                                        if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                            leg["status"]      = "OPEN"
                                            leg["exit_price"]  = 0.0
                                            leg["entry_price"] = restore_p
                                            break
                                else:
                                    self.logger.critical(
                                        "CE adjustment: new sell + restore both failed — emergency exit"
                                    )
                                    self.exit("CE adjustment fully failed — emergency exit")
                            else:
                                for leg in algo_legs.values():
                                    if leg["symbol"] == old_sym:
                                        leg["status"] = "CLOSED"
                                        break
                                algo_legs["CE_SHORT"] = create_leg(
                                    new_sym, "SELL", qty, sell_filled_p
                                )
                                pos["ce_short"] = new_ce_short
                                if old_sym in self.state.data["trade_symbols"]:
                                    self.state.data["trade_symbols"].remove(old_sym)
                                self.state.data["trade_symbols"].append(new_sym)
                                self.state.data["position_qty"][new_sym] = -qty
                                if old_sym in self.state.data["position_qty"]:
                                    del self.state.data["position_qty"][old_sym]
                                self.state.data["adjustments_today"]["ce"] += 1
                                self.state.data["entry_premiums"]["ce_short"] = sell_filled_p
                                adjusted = True
                                time.sleep(5)
                                self.state.data["exact_margin_used_by_trade"] = (
                                    self.actual_used_capital()
                                )
                                self.last_pnl = self.algo_pnl()

        # ── PE adjustment ──────────────────────────────────────────────────
        if (spot <= pe_short - Config.ADJUSTMENT_TRIGGER_POINTS and
                self.state.data["adjustments_today"]["pe"] < Config.MAX_ADJUSTMENTS_PER_SIDE_PER_DAY):

            self.logger.info("DEFENSIVE ADJUSTMENT: PE STRUCK", {
                "spot": spot, "old_strike": pe_short
            })
            old_sym = self.find_option_symbol(pe_short, "PE")
            if not old_sym:
                self.logger.error("PE adjustment: old symbol not found, skipping")
            else:
                success, _, filled_p = self.order(old_sym, "BUY", qty)
                if not success:
                    self.logger.critical(
                        "PE adjustment: failed to buy back old short — position unchanged"
                    )
                else:
                    for leg in algo_legs.values():
                        if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                            leg["exit_price"] = filled_p
                            leg["status"]     = "CLOSED"
                            break

                    hedge_strike = pos.get("pe_hedge")
                    new_pe_short = self.find_short_strike(current_atm, "PE")

                    if hedge_strike and abs(new_pe_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                        self.logger.warning(
                            "PE adjustment: new strike too close to hedge — restoring old short"
                        )
                        restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                        if restore_ok:
                            for leg in algo_legs.values():
                                if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                    leg["status"]      = "OPEN"
                                    leg["exit_price"]  = 0.0
                                    leg["entry_price"] = restore_p
                                    break
                        else:
                            self.logger.critical(
                                "PE adjustment: restore failed — attempting emergency exit"
                            )
                            self.exit("PE adjustment restore failed — emergency exit")
                    else:
                        new_sym = self.find_option_symbol(new_pe_short, "PE")
                        if not new_sym:
                            restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                            if restore_ok:
                                for leg in algo_legs.values():
                                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                        leg["status"]      = "OPEN"
                                        leg["exit_price"]  = 0.0
                                        leg["entry_price"] = restore_p
                                        break
                            else:
                                self.logger.critical(
                                    "PE adjustment: new symbol not found AND restore failed — emergency exit"
                                )
                                self.exit("PE adjustment new symbol not found — emergency exit")
                        else:
                            sell_ok, _, sell_filled_p = self.order(new_sym, "SELL", qty)
                            if not sell_ok:
                                restore_ok, _, restore_p = self.order(old_sym, "SELL", qty)
                                if restore_ok:
                                    for leg in algo_legs.values():
                                        if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                                            leg["status"]      = "OPEN"
                                            leg["exit_price"]  = 0.0
                                            leg["entry_price"] = restore_p
                                            break
                                else:
                                    self.logger.critical(
                                        "PE adjustment: new sell + restore both failed — emergency exit"
                                    )
                                    self.exit("PE adjustment fully failed — emergency exit")
                            else:
                                for leg in algo_legs.values():
                                    if leg["symbol"] == old_sym:
                                        leg["status"] = "CLOSED"
                                        break
                                algo_legs["PE_SHORT"] = create_leg(
                                    new_sym, "SELL", qty, sell_filled_p
                                )
                                pos["pe_short"] = new_pe_short
                                if old_sym in self.state.data["trade_symbols"]:
                                    self.state.data["trade_symbols"].remove(old_sym)
                                self.state.data["trade_symbols"].append(new_sym)
                                self.state.data["position_qty"][new_sym] = -qty
                                if old_sym in self.state.data["position_qty"]:
                                    del self.state.data["position_qty"][old_sym]
                                self.state.data["adjustments_today"]["pe"] += 1
                                self.state.data["entry_premiums"]["pe_short"] = sell_filled_p
                                adjusted = True
                                time.sleep(5)
                                self.state.data["exact_margin_used_by_trade"] = (
                                    self.actual_used_capital()
                                )
                                self.last_pnl = self.algo_pnl()

        if adjusted:
            self.state.data["algo_legs"] = algo_legs
            self.update_daily_profit_target(force=True)
            self.state.save()
        return adjusted

    # ── EXIT CHECK ────────────────────────────────────────────────────────
    def check_exit(self) -> Optional[str]:
        now_t = datetime.now(Config.TIMEZONE).time()
        if now_t >= Config.EXIT_TIME:
            return "Scheduled exit time reached"

        now        = datetime.now(Config.TIMEZONE)
        entry_time = self.state.data.get("entry_time")
        if isinstance(entry_time, str):
            try:
                entry_time = datetime.fromisoformat(entry_time)
                if entry_time.tzinfo is None:
                    entry_time = Config.TIMEZONE.localize(entry_time)
            except Exception as parse_err:
                self.logger.warning(
                    f"Could not parse entry_time: {entry_time} | {parse_err}"
                )
                entry_time = None

        time_since_entry = (now - entry_time).total_seconds() if entry_time else float('inf')

        # Average 3 LTP snapshots to avoid triggering on a single bad tick
        exit_legs    = self.state.data.get("algo_legs", {})
        open_syms    = [
            leg["symbol"].strip()
            for leg in exit_legs.values()
            if leg.get("symbol") and leg.get("status") != "CLOSED"
        ]
        ltp_samples: Dict[str, list] = {s: [] for s in open_syms}
        for _ in range(3):
            snap = self.bulk_ltp(open_syms) if open_syms else {}
            for s in open_syms:
                v = snap.get(s, 0.0)
                if v > 0:
                    ltp_samples[s].append(v)
            time.sleep(0.3)
        avg_ltps = {
            s: (sum(vs) / len(vs)) if vs else 0.0
            for s, vs in ltp_samples.items()
        }
        pnl_val = 0.0
        for leg in exit_legs.values():
            sym = leg.get("symbol", "").strip()
            if not sym:
                continue
            if leg.get("status") == "CLOSED":
                price = leg.get("exit_price", 0.0)
            else:
                price = avg_ltps.get(sym) or leg.get("last_known_ltp", leg["entry_price"])
            qty_abs = abs(leg["qty"])
            if leg["side"] == "SELL":
                pnl_val += (leg["entry_price"] - price) * qty_abs
            else:
                pnl_val += (price - leg["entry_price"]) * qty_abs

        target_rupee = self.state.data.get("profit_target_rupee", 0.0)

        if target_rupee > 0 and abs(pnl_val - self.last_pnl) > target_rupee * 0.2:
            self.logger.critical("P&L SPIKE DETECTED - IGNORING FOR SAFETY", {
                "current": pnl_val,
                "last":    self.last_pnl,
                "jump":    pnl_val - self.last_pnl
            })
            return None

        self.last_pnl = pnl_val

        if time_since_entry < 300 and abs(pnl_val) > 2500:
            self.logger.critical("EXTREME P&L GLITCH DETECTED - IGNORED", {
                "pnl":                 pnl_val,
                "seconds_since_entry": time_since_entry
            })
            return None

        if target_rupee > 0:
            if pnl_val >= target_rupee and time_since_entry >= Config.MIN_HOLD_SECONDS_FOR_PROFIT:
                return f"Profit target reached ₹{pnl_val:,.0f}"
            if pnl_val <= -target_rupee:
                return f"Stop loss hit ₹{pnl_val:,.0f}"

        vix_now   = self.vix()
        entry_vix = self.state.data.get("entry_vix")
        if vix_now and vix_now >= Config.VIX_EXIT_ABS:
            return "VIX absolute exit"
        if entry_vix and vix_now and vix_now >= entry_vix * Config.VIX_SPIKE_MULTIPLIER:
            return "VIX spike detected"
        if os.path.exists(Config.EMERGENCY_STOP_FILE):
            return "Emergency stop file detected"
        return None

    # ── EXIT ──────────────────────────────────────────────────────────────
    def exit(self, reason: str):
        trade_syms = self.state.data.get("trade_symbols", [])
        if not trade_syms:
            self.logger.warning("State empty — refusing exit to protect account")
            return
        if not self.state.data.get("trade_active", False):
            self.logger.info("No active bot trade detected - skipping exit")
            return

        self.logger.critical(f"EXIT TRIGGERED: {reason}", {"symbols": trade_syms})
        try:
            net_positions = self.kite.positions()["net"]
            shorts        = []
            hedges        = []
            pos_qty_map   = self.state.data.get("position_qty", {})
            bot_symbols   = set(trade_syms)
            total_leg_pnl = Decimal('0.00')

            for pos in net_positions:
                sym = pos["tradingsymbol"]
                if (pos["product"] != "MIS" or
                        pos["quantity"] == 0 or
                        sym not in bot_symbols):
                    continue
                expected_qty = pos_qty_map.get(sym)
                if expected_qty is None or abs(pos["quantity"]) != abs(expected_qty):
                    self.logger.warning("Qty mismatch - skipping", {"sym": sym})
                    continue
                exit_price = self.bulk_ltp([sym]).get(sym, 0.0)
                leg_pnl    = self._close_trade_record(sym, exit_price)
                total_leg_pnl += leg_pnl

                for leg in self.state.data.get("algo_legs", {}).values():
                    if leg.get("symbol") == sym and leg.get("status") == "OPEN":
                        leg["exit_price"] = exit_price
                        leg["status"]     = "CLOSED"
                        break

                if pos["quantity"] < 0:
                    shorts.append((sym, "BUY",  abs(pos["quantity"])))
                else:
                    hedges.append((sym, "SELL", pos["quantity"]))

            self.logger.info("Closing shorts first")
            for sym, side, qty in shorts:
                self.order(sym, side, qty)
            time.sleep(1.5)

            self.logger.info("Closing hedges")
            for sym, side, qty in hedges:
                self.order(sym, side, qty)
            time.sleep(5)

            still_open = self._get_still_open_mis_positions()

            if still_open:
                residual_pnl = self.algo_pnl()
                final_pnl    = float(total_leg_pnl) + residual_pnl
                self.logger.warning("PARTIAL EXIT — residual PnL included", {
                    "db_leg_pnl":   float(total_leg_pnl),
                    "residual_pnl": residual_pnl,
                    "final_pnl":    final_pnl
                })
            else:
                final_pnl = float(total_leg_pnl)

            self.state.data["exit_final_pnl"] = final_pnl
            self.state.data["realized_pnl"]   = float(total_leg_pnl)
            self.state.save()

            self.logger.critical("EXIT SUMMARY", {
                "final_pnl":  round(final_pnl, 2),
                "still_open": len(still_open),
                "reason":     reason
            })

            if still_open:
                self.logger.critical("PARTIAL EXIT - emergency flatten")
                self._emergency_square_off("Partial exit")

            # ── Save final authoritative PnL to DailyPnL ─────────────────
            today = datetime.now(Config.TIMEZONE).date()
            with transaction.atomic():
                daily, created = DailyPnL.objects.get_or_create(
                    user=self.user,
                    date=today,
                    defaults={
                        'pnl':          Decimal('0.00'),
                        'total_trades': 0,
                        'win_trades':   0,
                        'loss_trades':  0,
                    }
                )
                daily.pnl          = Decimal(str(final_pnl)).quantize(Decimal('0.00'))
                daily.total_trades = (daily.total_trades or 0) + 1
                if final_pnl > 0:
                    daily.win_trades  = (daily.win_trades  or 0) + 1
                else:
                    daily.loss_trades = (daily.loss_trades or 0) + 1
                daily.save()

            self.logger.critical("DailyPnL UPDATED (FINAL)", {
                "date":         today,
                "final_pnl":    round(final_pnl, 2),
                "total_trades": daily.total_trades
            })

            self.state.full_reset()
            self.last_pnl = 0.0

        except Exception as e:
            self.logger.critical("EXIT CRASHED", {
                "error": str(e), "trace": traceback.format_exc()
            })
            try:
                self._emergency_square_off("Exit crashed")
            except Exception as eq_err:
                self.logger.critical(
                    "Emergency square-off also failed", {"error": str(eq_err)}
                )
            self.state.full_reset()
            self.last_pnl = 0.0

    # ── EMERGENCY HELPERS ─────────────────────────────────────────────────
    def _get_still_open_mis_positions(self):
        try:
            pos = self.kite.positions()["net"]
            return [
                p["tradingsymbol"]
                for p in pos
                if p["product"] == "MIS" and p["quantity"] != 0
            ]
        except Exception:
            return ["fetch_failed"]

    def _emergency_square_off(self, reason="Emergency"):
        self.logger.critical(f"EMERGENCY FLATTEN: {reason}")
        try:
            for pos in self.kite.positions()["net"]:
                if pos["product"] == "MIS" and pos["quantity"] != 0:
                    qty  = abs(pos["quantity"])
                    side = "SELL" if pos["quantity"] > 0 else "BUY"
                    self.logger.critical(
                        f"Emergency squaring off {pos['tradingsymbol']} "
                        f"qty={qty} side={side}"
                    )
                    success, _, _ = self.order(pos["tradingsymbol"], side, qty)
                    if not success:
                        self.logger.critical(
                            f"Emergency order FAILED for {pos['tradingsymbol']} "
                            f"- manual intervention required"
                        )
        except Exception as e:
            self.logger.critical("EMERGENCY FLATTEN FAILED", {"error": str(e)})

    # ── PERIODIC PNL SNAPSHOT ─────────────────────────────────────────────
    def save_periodic_pnl_snapshot(self):
        """
        Upsert today's DailyPnL with current unrealized P&L for the calendar.
        Guard: total_trades == 0 means exit() hasn't finalized yet — safe to update.
        exit() always increments total_trades to >= 1, so this snapshot can never
        overwrite exit()'s final authoritative value.
        """
        if not self.state.data.get("trade_active"):
            return
        try:
            today       = datetime.now(Config.TIMEZONE).date()
            current_pnl = float(self.algo_pnl() or 0)
            with transaction.atomic():
                daily, created = DailyPnL.objects.get_or_create(
                    user=self.user,
                    date=today,
                    defaults={
                        'pnl':          Decimal(str(current_pnl)),
                        'total_trades': 0,
                        'win_trades':   0,
                        'loss_trades':  0,
                    }
                )
                if not created:
                    if daily.total_trades == 0:
                        daily.pnl = Decimal(str(current_pnl)).quantize(Decimal('0.00'))
                        daily.save(update_fields=['pnl'])
                    else:
                        self.logger.info(
                            "Periodic snapshot skipped — exit() already finalized today's record"
                        )
                        return
            self.logger.info("Periodic PnL snapshot → DailyPnL", {
                "date":    str(today),
                "pnl":     round(current_pnl, 2),
                "created": created
            })
        except Exception as e:
            self.logger.warning(
                "save_periodic_pnl_snapshot failed (non-fatal)", {"error": str(e)}
            )


# ===================== MAIN APPLICATION =====================
class TradingApplication:
    def __init__(self, user, broker):
        self.user   = user
        self.broker = broker
        self.logger = DBLogger(user)
        self.engine = Engine(user, broker, self.logger)

        self.running                   = False
        self.token_refreshed_today     = False
        self._vix_logged               = False
        self._snapshot_logged          = False
        self._early_0919_logged        = False
        self._manual_preview_triggered = False
        self._idle_logged_today        = False
        self._last_idle_date           = None
        self._last_hourly_log          = 0
        self._last_token_health_check  = time.time()
        self._daily_summary_saved      = False
        self._last_snapshot_time       = time.time()

        # ── Gevent-safe entry lock ────────────────────────────────────────────────────
        if self.user.id not in _USER_ENTRY_LOCKS:
            _USER_ENTRY_LOCKS[self.user.id] = GeventSemaphore(1)
        self._entry_lock              = _USER_ENTRY_LOCKS[self.user.id]
        self._entry_attempted         = False
        self._last_entry_attempt_date = None

    def _check_is_running(self) -> bool:
        try:
            bot_status = BotStatus.objects.get(user=self.user)
            return bot_status.is_running
        except Exception as e:
            self.logger.critical(
                "STOP-CHECK FAILED — assuming still running to be safe", {
                    "error": str(e)
                }
            )
            return True

    def run(self):
        self.running   = True
        last_heartbeat = time.time()
        self.logger.info("=== HEDGED STRANGLE BOT STARTED (DJANGO VERSION) ===")
        time.sleep(3)

        try:
            while self.running:
                try:
                    now_str = datetime.now(Config.TIMEZONE).strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )[:-3]
                    self.logger.critical(
                        f"[LOOP ALIVE] {now_str} | running={self.running} | "
                        f"trade_active={self.engine.state.data.get('trade_active', False)}"
                    )

                    # ── STOP SIGNAL CHECK ─────────────────────────────────
                    if not self._check_is_running():
                        self.logger.critical(
                            "STOP SIGNAL RECEIVED FROM DATABASE - shutting down gracefully"
                        )
                        if self.engine.state.data.get("trade_active"):
                            self.engine.exit("Manual stop from dashboard")
                        self.running = False
                        break

                    now           = datetime.now(Config.TIMEZONE)
                    current_time  = now.time()
                    today_date    = now.date()
                    today_weekday = now.weekday()

                    self.engine.state.daily_reset()

                    # ── RESET PER-DAY FLAGS ON CALENDAR DATE ROLLOVER ─────
                    if self._last_idle_date != today_date:
                        self._early_0919_logged   = False
                        self._snapshot_logged     = False
                        self._idle_logged_today   = False
                        self._daily_summary_saved = False
                        self.token_refreshed_today = False
                        self._last_idle_date      = today_date

                        self._entry_attempted         = False
                        self._last_entry_attempt_date = today_date
                        self.logger.info(
                            f"New trading day — entry guard reset for {today_date}"
                        )

                    # ── MANUAL CLOSE DETECTION ────────────────────────────
                    try:
                        net         = self.engine.kite.positions()["net"]
                        bot_symbols = set(
                            self.engine.state.data.get("trade_symbols", [])
                        )
                        if (bot_symbols and
                                self.engine.state.data.get("trade_active")):
                            current_qty = sum(
                                abs(p["quantity"])
                                for p in net
                                if p["product"] == "MIS"
                                and p["tradingsymbol"] in bot_symbols
                            )
                            expected_qty = self.engine.state.data.get("qty", 0)
                            if expected_qty > 0 and current_qty == 0:
                                self.logger.big_banner(
                                    "MANUAL CLOSE DETECTED - AUTO RECOVERING"
                                )
                                self.engine.state.full_reset()
                                self.engine.last_pnl = 0.0
                    except Exception as e:
                        self.logger.error(
                            "Periodic manual close check failed", {"error": str(e)}
                        )

                    # ── INSTRUMENT PRE-LOAD ───────────────────────────────
                    if dtime(6, 55) <= current_time < dtime(15, 30):
                        if (self.engine.instruments is None or
                                self.engine.weekly_df is None):
                            self.logger.info("PRE-LOADING instruments & weekly data")
                            try:
                                self.engine.load_instruments()
                                self.engine.load_weekly_df()
                            except Exception as e:
                                self.logger.error(
                                    "Instrument pre-load failed", {"error": str(e)}
                                )

                    # ── EARLY MARKET PREVIEW AT 09:19 ─────────────────────
                    if (dtime(9, 19) <= current_time < dtime(9, 20) and
                            not self._early_0919_logged):
                        self.logger.big_banner(
                            "EARLY MARKET PREVIEW - 09:19 IST (Pre-Entry Setup)"
                        )
                        try:
                            if (self.engine.instruments is None or
                                    self.engine.weekly_df is None):
                                self.engine.load_instruments()
                                self.engine.load_weekly_df()
                            self.engine.startup_banner()
                        except Exception as e:
                            self.logger.error("09:19 preview failed (non-fatal)", {
                                "error": str(e), "trace": traceback.format_exc()
                            })
                        finally:
                            self._early_0919_logged = True

                    # ── STARTUP BANNER AT 09:00 ───────────────────────────
                    if (dtime(9, 0) <= current_time < dtime(9, 30) and
                            not self._snapshot_logged):
                        try:
                            if self.engine.instruments is None:
                                self.engine.load_instruments()
                                self.engine.load_weekly_df()
                            self.engine.startup_banner()
                        except Exception as e:
                            self.logger.error(
                                "Startup banner failed (non-fatal)", {"error": str(e)}
                            )
                        finally:
                            self._snapshot_logged = True

                    # ── DAILY TOKEN REFRESH ───────────────────────────────
                    if (current_time >= Config.TOKEN_REFRESH_TIME and
                            not self.token_refreshed_today):
                        attempts = 0
                        while attempts < Config.MAX_TOKEN_ATTEMPTS:
                            attempts += 1
                            self.logger.info(f"Daily token refresh attempt {attempts}")
                            try:
                                access_token = generate_and_set_access_token_db(
                                    self.engine.kite, self.broker
                                )
                                if access_token:
                                    self.engine.load_instruments()
                                    self.engine.load_weekly_df()
                                    self.token_refreshed_today = True
                                    try:
                                        self.engine.startup_banner()
                                    except Exception as banner_err:
                                        self.logger.warning(
                                            "Startup banner failed after token refresh "
                                            "(non-fatal)", {"error": str(banner_err)}
                                        )
                                    break
                            except Exception as e:
                                self.logger.error(
                                    f"Token refresh attempt {attempts} failed",
                                    {"error": str(e)}
                                )
                            time.sleep(60)
                        else:
                            self.logger.critical(
                                "Token refresh failed after all attempts — "
                                "sleeping 1h before retry"
                            )
                            time.sleep(3600)

                    # ── TOKEN HEALTH CHECK (every 15 min) ─────────────────
                    if time.time() - self._last_token_health_check > 900:
                        try:
                            self.engine.kite.profile()
                        except TokenException:
                            self.logger.critical(
                                "TOKEN EXPIRED MID-DAY — attempting re-auth"
                            )
                            success = self.engine._authenticate()
                            if success:
                                self.logger.info("Mid-day token refresh succeeded")
                            else:
                                self.logger.critical(
                                    "Mid-day re-auth FAILED — sleeping 10min"
                                )
                                time.sleep(600)
                        except Exception as e:
                            self.logger.warning(
                                "Token health check failed (non-fatal)", {"error": str(e)}
                            )
                        self._last_token_health_check = time.time()

                    # ── MAIN TRADING LOGIC ────────────────────────────────
                    if (self.engine.instruments is not None and
                            self.engine.weekly_df is not None):

                        if self.engine.state.data["trade_active"]:
                            # ── EXIT CHECK ────────────────────────────────
                            reason = self.engine.check_exit()
                            if reason:
                                self.engine.exit(reason)
                            else:
                                # ── DEFENSIVE ADJUSTMENT ──────────────────
                                adjusted = self.engine.check_and_adjust_defensive()
                                if adjusted:
                                    self.engine.update_daily_profit_target(force=True)

                                # ── HOURLY STATUS LOG ──────────────────────
                                if time.time() - self._last_hourly_log >= 3600:
                                    current_pnl = self.engine.algo_pnl()
                                    actual_used = self.engine.actual_used_capital()
                                    self.logger.info("HOURLY STATUS", {
                                        "unrealized_pnl_₹":         round(current_pnl, 2),
                                        "target_₹":                 round(
                                            self.engine.state.data.get(
                                                "profit_target_rupee", 0
                                            )
                                        ),
                                        "actual_capital_blocked_₹": round(actual_used)
                                    })
                                    self._last_hourly_log = time.time()

                        else:
                            # ── ENTRY LOGIC ───────────────────────────────────
                            if Config.ENTRY_START <= current_time <= Config.ENTRY_END:

                                # Fast pre-checks before hitting the DB
                                if today_weekday == 1:
                                    self.logger.critical("TUESDAY SKIP — NO ENTRY TODAY")
                                    time.sleep(300)
                                    continue

                                if self.engine.state.data.get("trade_taken_today"):
                                    # In-memory fast path: DB gate already fired,
                                    # state persisted — skip without another DB hit
                                    time.sleep(5)
                                    continue

                                self.logger.critical("ENTRY WINDOW OPEN — CALLING enter()", {
                                    "current_time": current_time.strftime("%H:%M:%S"),
                                    "start":        Config.ENTRY_START.strftime("%H:%M:%S"),
                                    "end":          Config.ENTRY_END.strftime("%H:%M:%S"),
                                })

                                try:
                                    success = self.engine.enter()
                                    if success:
                                        self.logger.big_banner(
                                            "ENTRY SUCCESS — day permanently locked"
                                        )
                                        time.sleep(300)
                                    else:
                                        self.logger.warning(
                                            "enter() returned False — "
                                            "day locked, no retry today"
                                        )
                                        time.sleep(120)
                                except Exception as e:
                                    self.logger.error("Entry crashed", {
                                        "error": str(e),
                                        "trace": traceback.format_exc()
                                    })
                                    time.sleep(180)

                    # ── INSTRUMENTS NOT LOADED DURING MARKET HOURS ────────
                    elif (self.engine.instruments is None and
                          dtime(9, 0) <= current_time < dtime(15, 30)):
                        self.logger.warning(
                            "Instruments not loaded during trading hours — forcing reload"
                        )
                        try:
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                        except Exception as e:
                            self.logger.error(
                                "Forced instrument reload failed", {"error": str(e)}
                            )

                    # ── MARKET CLOSE SUMMARY (15:30) ──────────────────────
                    if (current_time >= Config.MARKET_CLOSE and
                            not self._daily_summary_saved):
                        try:
                            self.logger.big_banner(
                                "MARKET CLOSE — final PnL saved by exit(). "
                                "Bot idling until next trading day."
                            )
                            self._daily_summary_saved = True
                        except Exception as e:
                            self.logger.error(
                                "Market close summary failed", {"error": str(e)}
                            )
                            self._daily_summary_saved = True

                    # ── HEARTBEAT (every 5s) ──────────────────────────────
                    if time.time() - last_heartbeat >= Config.HEARTBEAT_INTERVAL:
                        try:
                            bot_status  = BotStatus.objects.get(user=self.user)
                            current_pnl = float(self.engine.algo_pnl() or 0)
                            bot_status.last_heartbeat         = timezone.now()
                            bot_status.current_unrealized_pnl = Decimal(str(current_pnl))
                            if self.engine.state.data.get("trade_active"):
                                target = self.engine.state.data.get(
                                    "profit_target_rupee", 0
                                )
                                print(
                                    f"[PnL] ₹{current_pnl:,.2f} | "
                                    f"Target: ₹{target:,} | "
                                    f"Stop: ₹{-target:,}",
                                    flush=True
                                )
                            bot_status.save(
                                update_fields=['last_heartbeat', 'current_unrealized_pnl']
                            )
                        except Exception as e:
                            self.logger.warning(
                                "Heartbeat save failed", {"error": str(e)}
                            )
                        last_heartbeat = time.time()

                    # ── PERIODIC PNL SNAPSHOT (every 30s) ─────────────────
                    if (self.engine.state.data.get("trade_active") and
                            time.time() - self._last_snapshot_time >=
                            Config.PERIODIC_PNL_SNAPSHOT_INTERVAL):
                        self.engine.save_periodic_pnl_snapshot()
                        self._last_snapshot_time = time.time()

                    time.sleep(1.0)

                except KeyboardInterrupt:
                    raise
                except Exception as loop_err:
                    self.logger.critical(
                        "LOOP ITERATION ERROR — recovering and continuing", {
                            "error":      str(loop_err),
                            "error_type": type(loop_err).__name__,
                            "trace":      traceback.format_exc()
                        }
                    )
                    time.sleep(5)

        except KeyboardInterrupt:
            self.logger.info("Bot stopped by user (KeyboardInterrupt)")
            if self.engine.state.data.get("trade_active"):
                self.engine.exit("Manual stop")
        except Exception as e:
            self.logger.critical("Fatal error in main loop", {
                "error":      str(e),
                "error_type": type(e).__name__,
                "trace":      traceback.format_exc()
            })
        finally:
            self.running = False
            self.logger.info("Bot loop exited")
            try:
                bot_status = BotStatus.objects.get(user=self.user)
                bot_status.last_heartbeat = timezone.now()
                bot_status.is_running     = False
                bot_status.save(update_fields=['last_heartbeat', 'is_running'])
                self.logger.info(
                    "Final heartbeat & is_running=False saved on shutdown"
                )
            except Exception as e:
                self.logger.warning(
                    "Failed to save final heartbeat on shutdown", {"error": str(e)}
                )