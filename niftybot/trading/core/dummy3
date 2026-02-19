import time
import sys
import traceback
import json
from datetime import datetime, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional
import pytz
import pandas as pd
import statistics
import os
import random
from django.utils import timezone
from django.contrib.auth.models import User
from django.db import transaction
from trading.models import Trade, DailyPnL, BotStatus, LogEntry
from .auth import generate_and_set_access_token_db
from kiteconnect import KiteConnect
import holidays
from decimal import Decimal
from kiteconnect.exceptions import TokenException

# ===================== CONFIGURATION =====================
class Config:
    SPOT_SYMBOL = "NSE:NIFTY 50"
    VIX_SYMBOL = "NSE:INDIA VIX"
    EXCHANGE = "NFO"
    UNDERLYING = "NIFTY"
    LOT_SIZE = 65
    ENTRY_START = dtime(10, 13, 0)
    ENTRY_END = dtime(10, 14, 30)
    TOKEN_REFRESH_TIME = dtime(8, 30)
    EXIT_TIME = dtime(10, 45)
    MARKET_CLOSE = dtime(15, 30)
    MAIN_DISTANCE = 150
    HEDGE_PREMIUM_RATIO = 0.10
    MAX_OVERPAY_MULT = 1.25
    MAX_CAPITAL_USAGE = 1.0
    MIN_CAPITAL_FOR_1LOT = 120000
    MAX_LOTS = 50
    VIX_EXIT_ABS = 18.0
    VIX_SPIKE_MULTIPLIER = 1.20
    VIX_MIN = 7.0
    VIX_MAX = 30.0
    VIX_THRESHOLD_FOR_PERCENT_TARGET = 12
    PERCENT_TARGET_WHEN_VIX_HIGH = 0.020
    ADJUSTMENT_TRIGGER_POINTS = 50
    ADJUSTMENT_CUTOFF_TIME = dtime(13, 30)
    MAX_ADJUSTMENTS_PER_SIDE_PER_DAY = 1
    MIN_HEDGE_GAP = 300
    TIMEZONE = pytz.timezone("Asia/Kolkata")
    EMERGENCY_STOP_FILE = "EMERGENCY_STOP.txt"
    MAX_TOKEN_ATTEMPTS = 10
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    ORDER_TIMEOUT = 10
    PNL_CHECK_INTERVAL_SECONDS = 1
    MIN_HOLD_SECONDS_FOR_PROFIT = 1800
    HEARTBEAT_INTERVAL = 5
    PERIODIC_PNL_SNAPSHOT_INTERVAL = 300

INDIA_HOLIDAYS = holidays.India()
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
        ts = datetime.now(Config.TIMEZONE)
        line = f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}"
        safe_details = make_json_safe(details) if details else {}
        if safe_details:
            line += f" | {json.dumps(safe_details)}"
        print(line)
        try:
            LogEntry.objects.create(
                user=self.user,
                level=level,
                message=msg,
                details=safe_details or {}
            )
        except Exception as e:
            print(f"DB log failed: {str(e)}")

    def info(self, msg: str, details: dict = None):
        self._write("INFO", msg, details)

    def warning(self, msg: str, details: dict = None):
        self._write("WARNING", msg, details)

    def error(self, msg: str, details: dict = None):
        self._write("ERROR", msg, details)

    def critical(self, msg: str, details: dict = None):
        self._write("CRITICAL", msg, details)

    def trade(self, action: str, symbol="", qty=0, price=0.0, comment=""):
        ts = datetime.now(Config.TIMEZONE)
        ms = ts.microsecond // 1000
        trade_id = f"{self.user.id}_{int(ts.timestamp())}_{ms}_{symbol}"
        status = 'EXECUTED' if "BUY" in action.upper() or "SELL" in action.upper() else 'PENDING'
        try:
            Trade.objects.create(
                user=self.user,
                trade_id=trade_id,
                symbol=symbol,
                quantity=qty,
                entry_price=Decimal(str(price)),
                entry_time=ts,
                status=status,
                broker='ZERODHA',
                metadata={'action': action, 'comment': comment}
            )
        except Exception as e:
            self.error(f"Trade save failed: {str(e)}")
        self.info(f"Trade logged: {action} {symbol} {qty} @ {price}", {
            'action': action,
            'symbol': symbol,
            'quantity': qty,
            'price': price,
            'comment': comment
        })

    def big_banner(self, msg: str):
        print("\n" + "="*80)
        print(f"*** {msg} ***".center(80))
        print("="*80 + "\n")
        self.info(f"BANNER: {msg}")

# ===================== STATE (DB-BASED) =====================
class DBState:
    def __init__(self, user):
        self.user = user
        self.bot_status, _ = BotStatus.objects.get_or_create(user=user)
        self.data = self.load()

    def load(self):
        if hasattr(self.bot_status, 'load_state'):
            state_data = self.bot_status.load_state()
        else:
            state_data = getattr(self.bot_status, 'state_json', {})
        if not state_data:
            state_data = {
                "trade_active": False,
                "trade_taken_today": False,
                "entry_date": None,
                "trade_symbols": [],
                "positions": {},
                "position_qty": {},
                "algo_legs": {},
                "margin_used": 0,
                "realistic_margin": 90000,
                "exact_margin_used_by_trade": 0,
                "final_margin_used": 0.0,
                "margin_per_lot": 0,
                "entry_vix": None,
                "entry_spot": None,
                "entry_atm": None,
                "entry_premiums": {},
                "last_reset": None,
                "profit_target_rupee": 0.0,
                "target_frozen": False,
                "qty": 0,
                "adjustments_today": {"ce": 0, "pe": 0},
                "last_adjustment_date": None,
                "realized_pnl": 0.0,
                "last_spot": None,
                "bot_order_ids": [],
                "entry_time": None,
                "exit_final_pnl": 0.0,
            }
        return state_data

    def save(self):
        data_to_save = make_json_safe(self.data)
        if hasattr(self.bot_status, 'save_state'):
            self.bot_status.save_state(data_to_save)
        else:
            self.bot_status.state_json = data_to_save
            self.bot_status.save(update_fields=['state_json'])
        self.bot_status.current_unrealized_pnl = Decimal(str(self.data.get("realized_pnl", 0)))
        self.bot_status.current_margin = Decimal(str(self.data.get("exact_margin_used_by_trade", 0)))
        self.bot_status.save()

    def daily_reset(self):
        today = datetime.now(Config.TIMEZONE).date()
        today_str = str(today)
        if self.bot_status.entry_attempted_date != today:
            self.bot_status.entry_attempted_date = None
            self.bot_status.last_successful_entry = None
            self.bot_status.save(update_fields=['entry_attempted_date', 'last_successful_entry'])
            print(f"[DAILY RESET] Cleared entry lock for new day: {today_str}")
        if self.data.get("last_reset") != today_str:
            self.data.update({
                "adjustments_today": {"ce": 0, "pe": 0},
                "last_adjustment_date": None,
                "exit_final_pnl": 0.0,
            })
            self.data["last_reset"] = today_str
            self.save()

    def full_reset(self):
        self.data.update({
            "trade_active": False,
            "trade_taken_today": False,
            "entry_date": None,
            "trade_symbols": [],
            "positions": {},
            "position_qty": {},
            "algo_legs": {},
            "margin_used": 0,
            "realistic_margin": 90000,
            "exact_margin_used_by_trade": 0,
            "final_margin_used": 0.0,
            "margin_per_lot": 0,
            "profit_target_rupee": 0.0,
            "target_frozen": False,
            "qty": 0,
            "adjustments_today": {"ce": 0, "pe": 0},
            "last_adjustment_date": None,
            "realized_pnl": 0.0,
            "bot_order_ids": [],
            "entry_time": None,
            "exit_final_pnl": 0.0,
        })
        self.save()
        today = datetime.now(Config.TIMEZONE).date()
        self.bot_status.entry_attempted_date = today
        self.bot_status.save(update_fields=['entry_attempted_date'])

# ===================== HELPER =====================
def create_leg(symbol: str, side: str, qty: int, entry_price: float):
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": 0.0,
        "status": "OPEN",
        "last_known_ltp": entry_price
    }

# ===================== ENGINE =====================
class Engine:
    def __init__(self, user, broker, logger):
        self.user = user
        self.broker = broker
        self.logger = logger
        self.kite = KiteConnect(api_key=broker.api_key)
        self.state = DBState(user)
        self.instruments = None
        self.weekly_df = None
        self._last_valid_vix = None
        self.ltp_cache = {}
        self.last_pnl = 0.0

        self.logger.critical("ENGINE INIT - Starting authentication...")
        auth_success = self._authenticate()
        if auth_success and self.kite.access_token:
            self.logger.critical("ENGINE INIT - AUTH SUCCESS")
        else:
            self.logger.critical("ENGINE INIT - AUTH FAILED")

        today = datetime.now(Config.TIMEZONE).date()
        if self.state.bot_status.entry_attempted_date != today:
            self.state.bot_status.entry_attempted_date = None
            self.state.bot_status.last_successful_entry = None
            self.state.bot_status.save(update_fields=['entry_attempted_date', 'last_successful_entry'])

    def _authenticate(self):
        try:
            access_token = generate_and_set_access_token_db(kite=self.kite, broker=self.broker)
            if access_token:
                self.kite.set_access_token(access_token)
                self.logger.info("Authentication successful")
                return True
            return False
        except Exception as e:
            self.logger.error("Auth error", {"error": str(e)})
            return False

    def _get_or_create_trade(self, symbol, side, qty, price, order_id=None):
        direction = 'SHORT' if side == 'SELL' else 'LONG'
        option_type = 'CE' if 'CE' in symbol else 'PE' if 'PE' in symbol else None

        # Try to find existing open trade
        trade = Trade.objects.filter(
            user=self.user,
            symbol=symbol,
            entry_price=Decimal(str(price)),
            quantity=qty if direction == 'LONG' else -qty,
            status='EXECUTED',
            exit_time__isnull=True
        ).first()

        if trade:
            return trade

        # Create new
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
            metadata={'order_id': order_id, 'side': side},
            algorithm_name='Hedged Short Strangle'
        )
        self.logger.trade(f"ENTRY {side} {symbol} {qty} @ {price}", symbol, qty, price, f"trade_id:{trade.trade_id}")
        return trade

    def _close_trade_record(self, symbol, exit_price):
        try:
            trade = Trade.objects.get(
                user=self.user,
                symbol=symbol,
                status='EXECUTED',
                exit_time__isnull=True
            )
            trade.close_trade(Decimal(str(exit_price)), timezone.now())
            self.logger.info(f"Closed trade {trade.trade_id} | PnL: {trade.pnl}")
            return trade.pnl
        except Trade.DoesNotExist:
            self.logger.warning(f"No open Trade record to close for {symbol}")
            return Decimal('0.00')
        except Exception as e:
            self.logger.error(f"Failed to close trade for {symbol}", {"error": str(e)})
            return Decimal('0.00')

    def capital_available(self) -> float:
        try:
            margins = self.kite.margins()["equity"]
            live_balance = margins["available"].get("live_balance", 0)
            cash = margins["available"].get("cash", 0)
            collateral = margins["available"].get("collateral", 0)
            total_usable = live_balance + collateral
            self.logger.info("TOTAL USABLE CAPITAL FOR LOT SIZING", {
                "live_balance": round(live_balance),
                "cash_component": round(cash),
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

    def is_trading_day(self) -> tuple[bool, str]:
        today = datetime.now(Config.TIMEZONE).date()
        if today.weekday() >= 5:
            return False, "Weekend"
        if today in INDIA_HOLIDAYS:
            holiday_name = INDIA_HOLIDAYS.get(today) or "Indian Public Holiday"
            return False, f"Holiday: {holiday_name}"
        if today in EXTRA_NSE_HOLIDAYS:
            return False, "Manual NSE holiday override"
        return True, "Trading day"

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
            self.logger.critical("Instrument load failed", {"error": str(e), "trace": traceback.format_exc()})
            raise RuntimeError("Instruments load failed")

    def load_weekly_df(self):
        if self.instruments is None or self.instruments.empty:
            self.weekly_df = pd.DataFrame()
            return
        try:
            today = datetime.now(Config.TIMEZONE).date()
            next_tue = today + timedelta(days=(1 - today.weekday()) % 7 or 7)
            self.weekly_df = self.instruments[self.instruments["expiry"] == next_tue].copy()
            self.logger.info("Weekly cache updated", {"expiry": next_tue.strftime("%Y-%m-%d"), "count": len(self.weekly_df)})
        except Exception as e:
            self.logger.error("Weekly cache failed", {"error": str(e)})
            self.weekly_df = pd.DataFrame()

    def get_current_expiry_date(self) -> Optional[datetime.date]:
        if self.weekly_df is None or self.weekly_df.empty:
            return None
        return self.weekly_df["expiry"].iloc[0]

    def calculate_trading_days_including_today(self, start_date: datetime.date) -> int:
        expiry = self.get_current_expiry_date()
        if not expiry:
            return 1
        current = start_date
        count = 0
        while current <= expiry:
            if current.weekday() < 5 and current not in INDIA_HOLIDAYS and current not in EXTRA_NSE_HOLIDAYS:
                count += 1
            current += timedelta(days=1)
        return max(count, 1)

    def spot(self) -> Optional[float]:
        time.sleep(0.5)
        try:
            price = self.kite.quote(Config.SPOT_SYMBOL)[Config.SPOT_SYMBOL]["last_price"]
            if price:
                self.state.data["last_spot"] = price
                self.state.save()
            return price
        except Exception as e:
            self.logger.warning("Spot fetch failed", {
                "error_type": type(e).__name__,
                "error": str(e)
            })
            return self.state.data.get("last_spot")

    def vix(self) -> Optional[float]:
        time.sleep(0.5)
        try:
            quote = self.kite.quote(Config.VIX_SYMBOL)
            v = quote[Config.VIX_SYMBOL]["last_price"]
            if v and v > 0:
                self._last_valid_vix = v
                return v
        except Exception as e:
            self.logger.warning("VIX fetch failed", {"error": str(e)})
        return self._last_valid_vix

    def bulk_ltp(self, symbols: List[str]) -> Dict[str, float]:
        if not symbols:
            return {}
        now = time.time()
        result = {}
        to_fetch = []
        for sym in symbols:
            cache_entry = self.ltp_cache.get(sym.strip())
            if cache_entry and now - cache_entry['ts'] < 8:
                result[sym] = cache_entry['price']
            else:
                to_fetch.append(sym.strip())
        if not to_fetch:
            return result
        for attempt in range(3):
            try:
                time.sleep(0.3 * attempt)
                quotes = self.kite.quote([f"{Config.EXCHANGE}:{s}" for s in to_fetch])
                valid_count = 0
                for full, data in quotes.items():
                    sym = full.split(':')[1]
                    price = data.get('last_price', 0.0)
                    if price > 0:
                        result[sym] = price
                        self.ltp_cache[sym] = {'price': price, 'ts': now}
                        valid_count += 1
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
                    "attempt": attempt + 1,
                    "fetched": len(to_fetch),
                    "valid_prices": valid_count
                })
                return result
            except Exception as e:
                self.logger.warning(f"bulk_ltp attempt {attempt+1} failed", {
                    "error": str(e),
                    "symbols_count": len(to_fetch)
                })
                if attempt == 2:
                    self.logger.error("bulk_ltp failed after 3 attempts - using fallbacks")
        return result

    def find_option_symbol(self, strike: int, cp: str) -> Optional[str]:
        if self.weekly_df is None or self.weekly_df.empty:
            return None
        df = self.weekly_df[(self.weekly_df["strike"] == strike) & (self.weekly_df["instrument_type"] == cp)]
        return df.iloc[0]["tradingsymbol"] if not df.empty else None

    def find_short_strike(self, atm_strike: int, cp: str) -> int:
        distance = Config.MAIN_DISTANCE
        direction = 1 if cp == "CE" else -1
        target = atm_strike + direction * distance
        target = int(round(target / 50) * 50)
        sym = self.find_option_symbol(target, cp)
        if sym:
            prem = self.bulk_ltp([sym])[sym]
            if prem > 2.0:
                self.logger.info(f"Short {cp} found at exact target {target} (premium {prem:.2f})")
                return target
        for offset in [50, -50, 100, -100, 150, -150]:
            test_strike = target + offset
            if (cp == "CE" and test_strike <= atm_strike) or (cp == "PE" and test_strike >= atm_strike):
                continue
            sym = self.find_option_symbol(test_strike, cp)
            if sym:
                prem = self.bulk_ltp([sym])[sym]
                if prem > 0:
                    self.logger.info(f"Short {cp} fallback to {test_strike} (premium {prem:.2f})")
                    return test_strike
        self.logger.warning(f"No good short {cp} found near {target} — using rounded target anyway")
        return target

    def find_hedge_strike(self, short_strike: int, cp: str, common_target_prem: float, simulate: bool = False) -> int:
        direction = 1 if cp == "CE" else -1
        df = self.weekly_df[(self.weekly_df["instrument_type"] == cp)].copy()
        df = df[df["strike"] > short_strike] if cp == "CE" else df[df["strike"] < short_strike]
        df = df.sort_values("strike", ascending=(cp == "PE"))
        best_strike = None
        best_diff = float('inf')
        best_actual = 0.0
        symbols = [row["tradingsymbol"] for _, row in df.iterrows()]
        ltps = self.bulk_ltp(symbols)
        for _, row in df.iterrows():
            sym = row["tradingsymbol"]
            prem = ltps.get(sym, 0.0)
            if prem <= 3.0:
                continue
            if prem > common_target_prem * Config.MAX_OVERPAY_MULT:
                continue
            diff = abs(prem - common_target_prem)
            if diff < best_diff:
                best_diff = diff
                best_strike = row["strike"]
                best_actual = prem
        if best_strike is not None:
            if not simulate:
                self.logger.info("Symmetric hedge selected", {
                    "side": cp,
                    "common_target": round(common_target_prem, 2),
                    "strike": best_strike,
                    "actual": round(best_actual, 2),
                    "diff": round(best_actual - common_target_prem, 2)
                })
            return best_strike
        fallback = short_strike + direction * 1000
        if not simulate:
            self.logger.warning("Symmetric hedge fallback used", {"side": cp, "strike": fallback})
        return fallback

    def exact_margin_for_basket(self, legs: List[dict]) -> Tuple[float, float]:
        formatted_legs = []
        for leg in legs:
            formatted_legs.append({
                "exchange": leg.get("exchange", Config.EXCHANGE),
                "tradingsymbol": leg["tradingsymbol"],
                "transaction_type": leg["transaction_type"],
                "variety": "regular",
                "product": "MIS",
                "order_type": "MARKET",
                "quantity": int(leg["quantity"]),
                "price": 0.0,
                "trigger_price": 0.0
            })
        try:
            response = self.kite.basket_order_margins(formatted_legs, consider_positions=True)
            initial = response["initial"]["total"]
            final = response["final"]["total"]
            self.logger.info("Margin API Success", {
                "initial_margin": round(initial),
                "final_margin": round(final),
                "legs": len(formatted_legs)
            })
            return initial, final
        except Exception as e:
            self.logger.warning("Margin API Failed - using fallback", {"error": str(e)})
            fallback = Config.MIN_CAPITAL_FOR_1LOT * 1.2
            return fallback, fallback * 0.85

    def calculate_lots(self, legs: List[dict]) -> int:
        capital = self.capital_available()
        if capital < Config.MIN_CAPITAL_FOR_1LOT:
            self.logger.warning("Capital too low for 1 lot", {"available": capital})
            return 0
        initial_margin, _ = self.exact_margin_for_basket(legs)
        try:
            user_hard_cap = self.state.bot_status.max_lots_hard_cap
            if not isinstance(user_hard_cap, int) or user_hard_cap < 1:
                user_hard_cap = 1
        except (AttributeError, BotStatus.DoesNotExist):
            user_hard_cap = 1
        lots = int((capital * Config.MAX_CAPITAL_USAGE) // initial_margin)
        lots = min(lots, Config.MAX_LOTS, user_hard_cap)
        lots = max(lots, 1) if lots >= 1 else 0
        self.logger.info("Lot calculation", {
            "capital": round(capital),
            "initial_margin": round(initial_margin),
            "calculated_lots_before_cap": lots,
            "user_hard_cap": user_hard_cap,
            "final_lots": lots
        })
        return lots

    def order(self, symbol: str, side: str, qty: int) -> Tuple[bool, str, float]:
        filled_price = 0.0
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.logger.info(f"PLACING ORDER attempt {attempt+1}/{Config.MAX_RETRIES}", {
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "product": "MIS",
                    "order_type": "MARKET"
                })
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
                    history = self.kite.order_history(order_id)
                    if not history:
                        time.sleep(0.5)
                        continue
                    last_status = history[-1]['status']
                    self.logger.info(f"Order status: {last_status}")
                    if last_status == 'COMPLETE':
                        filled_price = history[-1].get('average_price', 0.0)
                        if filled_price == 0.0:
                            self.logger.warning(f"Order completed but average_price 0 for {symbol} - using LTP fallback")
                            filled_price = self.bulk_ltp([symbol])[symbol]

                        # Create Trade record for entry
                        side_for_trade = "BUY" if side == "BUY" else "SELL"
                        trade = self._get_or_create_trade(symbol, side_for_trade, qty, filled_price, order_id)

                        self.logger.trade(f"{side}_{symbol}", symbol, qty if side=="BUY" else -qty, filled_price, f"order_id:{order_id}")
                        self.state.data.setdefault("bot_order_ids", []).append(order_id)
                        self.state.save()
                        return True, order_id, filled_price
                    elif last_status in ['REJECTED', 'CANCELLED']:
                        reason = history[-1].get('status_message', 'No reason provided')
                        self.logger.critical(f"ORDER {last_status} for {symbol} - Reason: {reason}")
                        raise Exception(f"Order {last_status}: {reason}")
                    time.sleep(0.5)
                self.logger.error(f"Order timeout - not completed within {Config.ORDER_TIMEOUT}s")
                raise Exception("Order timeout")
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"Order placement failed for {symbol} ({side}) attempt {attempt+1}", {
                    "error": error_msg,
                    "trace": traceback.format_exc()
                })
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY)
                else:
                    self.logger.critical(f"FINAL FAILURE: Giving up on {symbol} after {Config.MAX_RETRIES} attempts")
                    return False, "", 0.0
        return False, "", 0.0

    def cleanup(self, executed: List[Tuple[str, str, str]], qty: int):
        for sym, side, _ in executed:
            opp = "SELL" if side == "BUY" else "BUY"
            self.order(sym, opp, qty)

    def algo_pnl(self) -> float:
        legs = self.state.data.get("algo_legs", {})
        if not legs:
            self.logger.warning("algo_pnl: No legs found in state - returning 0")
            return 0.0
        symbols = [leg["symbol"].strip() for leg in legs.values() if leg.get("symbol")]
        if not symbols:
            self.logger.warning("algo_pnl: No valid symbols in legs - returning 0")
            return 0.0
        ltps = self.bulk_ltp(symbols)
        total = 0.0
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
            qty_abs = abs(leg["qty"])
            if leg["side"] == "SELL":
                total += (leg["entry_price"] - price) * qty_abs
            else:
                total += (price - leg["entry_price"]) * qty_abs
            if leg.get("status") != "CLOSED" and price > 0 and price != leg["entry_price"]:
                leg["last_known_ltp"] = price
        if fallback_count > 0:
            self.logger.warning(f"PNL CALC USED FALLBACK FOR {fallback_count}/{len(legs)} legs → possible stale LTP", {
                "fallback_count": fallback_count,
                "total_legs": len(legs)
            })
        return total

    def lock_target(self, target_rupee: float):
        rounded_target = round(target_rupee)
        self.state.data["profit_target_rupee"] = rounded_target
        self.state.data["target_frozen"] = True
        self.state.save()
        bot_status = self.state.bot_status
        bot_status.daily_profit_target = Decimal(str(rounded_target))
        bot_status.daily_stop_loss = Decimal(str(-rounded_target))
        bot_status.save(update_fields=['daily_profit_target', 'daily_stop_loss'])
        self.logger.info("DAILY TARGET LOCKED - NO FURTHER CHANGES TODAY", {
            "target_₹": rounded_target,
            "stop_loss_₹": -rounded_target,
            "saved_to_model": True
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
            entry_prem = self.state.data["entry_premiums"]
            net_per_lot = (
                entry_prem.get("ce_short", 0) + entry_prem.get("pe_short", 0) -
                entry_prem.get("ce_hedge", 0) - entry_prem.get("pe_hedge", 0)
            )
            qty = self.state.data.get("qty", 0)
            total_credit = net_per_lot * qty
            today = datetime.now(Config.TIMEZONE).date()
            remaining_days = self.calculate_trading_days_including_today(today)
            today_target = total_credit / remaining_days if remaining_days > 0 else total_credit
            self.logger.info("Low VIX mode - using Net Credit ÷ Days", {
                "entry_vix": round(entry_vix, 2),
                "total_credit": round(total_credit),
                "remaining_days": remaining_days,
                "daily_target_before_98%": round(today_target)
            })
        else:
            margin_for_target = self.state.data.get("final_margin_used", 0.0)
            if margin_for_target <= 0:
                margin_for_target = self.actual_used_capital()
                self.logger.warning("final_margin_used not available → fallback to actual used capital")
            else:
                self.logger.info("Using FINAL_MARGIN for high VIX target calculation")
            today_target = margin_for_target * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            self.logger.info("High VIX mode - using 2.0% of final_margin", {
                "entry_vix": round(entry_vix, 2),
                "final_margin": round(margin_for_target),
                "daily_target_before_98%": round(today_target)
            })
        today_target *= 0.97
        today_target = round(today_target)
        self.logger.info("Target adjusted to 98% of calculated", {"final_daily_target_₹": today_target})
        self.lock_target(today_target)

    def preview_profit_calculation(self):
        live_spot = self.spot()
        if live_spot:
            spot = live_spot
        else:
            self.logger.warning("Spot price temporarily unavailable - using last known values for preview")
            spot = self.state.data.get("last_spot") or self.state.data.get("entry_spot") or 24500
        atm_strike = self.atm(spot)
        self.logger.info("PREVIEW SNAPSHOT (NOT LOCKED)", {"Spot": spot, "ATM": atm_strike})
        vix_val = self.vix()
        if not vix_val:
            self.logger.warning("VIX not available - continuing preview")
        ce_short = self.find_short_strike(atm_strike or 0, "CE")
        pe_short = self.find_short_strike(atm_strike or 0, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ltps = self.bulk_ltp([ce_short_sym, pe_short_sym])
        ce_short_p = ltps.get(ce_short_sym, 0.0)
        pe_short_p = ltps.get(pe_short_sym, 0.0)
        if ce_short_p <= 0 or pe_short_p <= 0:
            self.logger.warning("Short leg premiums not reliably available - skipping preview")
            return
        ce_target = ce_short_p * Config.HEDGE_PREMIUM_RATIO
        pe_target = pe_short_p * Config.HEDGE_PREMIUM_RATIO
        common_target = min(ce_target, pe_target)
        ce_hedge = self.find_hedge_strike(ce_short, "CE", common_target, simulate=True)
        pe_hedge = self.find_hedge_strike(pe_short, "PE", common_target, simulate=True)
        ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
        pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")
        ltps_hedge = self.bulk_ltp([ce_hedge_sym, pe_hedge_sym])
        ce_hedge_p = ltps_hedge.get(ce_hedge_sym, 0.0)
        pe_hedge_p = ltps_hedge.get(pe_hedge_sym, 0.0)
        legs = []
        if pe_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
        if ce_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE})
        actual_lots = self.calculate_lots(legs)
        preview_mode = actual_lots == 0
        if preview_mode:
            actual_lots = 1
        qty = actual_lots * Config.LOT_SIZE
        net_credit_per_lot = ce_short_p + pe_short_p - ce_hedge_p - pe_hedge_p
        net_credit_total = net_credit_per_lot * qty
        today = datetime.now(Config.TIMEZONE).date()
        remaining_days = self.calculate_trading_days_including_today(today)
        if vix_val and vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            credit_based_today = net_credit_total / remaining_days if remaining_days > 0 else net_credit_total
            projected_target = round(credit_based_today * 0.98)
            target_mode = "theta ÷ days"
            target_display = f"+₹{projected_target:,}"
        else:
            _, estimated_final = self.exact_margin_for_basket([dict(l, quantity=qty) for l in legs])
            capital_based_today = estimated_final * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            projected_target = round(capital_based_today * 0.98)
            target_mode = "2.0% of final margin"
            target_display = f"+₹{projected_target:,} (final margin ≈ ₹{round(estimated_final):,})"
        mode = "PREVIEW (ASSUMING 1 LOT)" if preview_mode else "LIVE PREVIEW"
        hedge_note = " (some hedges unavailable → assuming ₹0 cost)" if (ce_hedge_p == 0 or pe_hedge_p == 0) else ""
        spot_note = " (Spot temporarily unavailable)" if live_spot is None else ""
        vix_note = f" | Current VIX: {vix_val:.2f} → using {target_mode} logic" if vix_val else ""
        self.logger.big_banner(f"{mode}{hedge_note}{spot_note}{vix_note} | PROJECTED DAILY TARGET: {target_display} | STOP LOSS: -{target_display.replace('+','')}")
        print(f"Remaining Trading Days till Expiry : {remaining_days}\n")
        print("Proposed Legs (Live Premiums):")
        print(f" Sell CE {ce_short:5} → ₹{ce_short_p:.2f}")
        print(f" Sell PE {pe_short:5} → ₹{pe_short_p:.2f}")
        print(f" Buy CE Hedge {ce_hedge or 'N/A':5} → ₹{ce_hedge_p:.2f}")
        print(f" Buy PE Hedge {pe_hedge or 'N/A':5} → ₹{pe_hedge_p:.2f}")
        print(f"\nNet Credit per lot : ₹{net_credit_per_lot:.2f} × {actual_lots} lot(s) × {Config.LOT_SIZE} = ₹{net_credit_total:,.0f}")
        if vix_val and vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            print(f"Projected Daily Target (Net Credit ÷ Days × 0.98) : +₹{projected_target:,}")
        else:
            print(f"Projected Daily Target (2.0% of estimated final margin × 0.98) : {target_display}")
        print("\n" + "="*80 + "\n")

    def startup_banner(self):
        now = datetime.now(Config.TIMEZONE)
        spot = self.spot() or 0
        vix = self.vix()
        atm_strike = self.atm(spot) if spot else None
        if self.weekly_df is None or self.weekly_df.empty:
            self.load_weekly_df()
        expiry_date = self.get_current_expiry_date()
        expiry_str = expiry_date.strftime("%d %b %Y (%A)").upper() if expiry_date else "N/A"
        today = now.date()
        remaining_days = self.calculate_trading_days_including_today(today)
        ce_short = pe_short = ce_hedge = pe_hedge = None
        ce_short_ltp = pe_short_ltp = ce_hedge_ltp = pe_hedge_ltp = "N/A"
        if spot and not self.weekly_df.empty:
            ce_short = self.find_short_strike(atm_strike or 0, "CE")
            pe_short = self.find_short_strike(atm_strike or 0, "PE")
            ce_sym = self.find_option_symbol(ce_short, "CE")
            pe_sym = self.find_option_symbol(pe_short, "PE")
            ltps_short = self.bulk_ltp([ce_sym, pe_sym])
            ce_short_ltp = f"{ltps_short.get(ce_sym, 0.0):.2f}" if ce_sym else "N/A"
            pe_short_ltp = f"{ltps_short.get(pe_sym, 0.0):.2f}" if pe_sym else "N/A"
            ce_short_p = ltps_short.get(ce_sym, 100.0)
            pe_short_p = ltps_short.get(pe_sym, 100.0)
            ce_target = ce_short_p * Config.HEDGE_PREMIUM_RATIO
            pe_target = pe_short_p * Config.HEDGE_PREMIUM_RATIO
            common_target = min(ce_target, pe_target)
            ce_hedge = self.find_hedge_strike(ce_short, "CE", common_target, simulate=True)
            pe_hedge = self.find_hedge_strike(pe_short, "PE", common_target, simulate=True)
            ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
            pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")
            ltps_hedge = self.bulk_ltp([ce_hedge_sym, pe_hedge_sym])
            ce_hedge_ltp = f"{ltps_hedge.get(ce_hedge_sym, 0.0):.2f}" if ce_hedge_sym else "N/A"
            pe_hedge_ltp = f"{ltps_hedge.get(pe_hedge_sym, 0.0):.2f}" if pe_hedge_sym else "N/A"
        spot_str = f"{spot:.1f}" if spot else "N/A"
        vix_str = f"{vix:.2f}" if vix else "N/A"
        self.logger.info("=" * 80)
        self.logger.info("HEDGED SHORT STRANGLE - DJANGO VERSION - LOGIC ALIGNED")
        self.logger.info(f"Date: {now.strftime('%A, %d %B %Y')} | Time: {now.strftime('%H:%M:%S')} IST")
        self.logger.info(f"Spot: {spot_str} | ATM: {atm_strike or 'N/A'} | VIX: {vix_str}")
        self.logger.info(f"Weekly Expiry: {expiry_str} | Remaining Days: {remaining_days}")
        self.logger.info(f"SHORT CE: {ce_short or 'N/A'} ({ce_short_ltp}) | HEDGE CE: {ce_hedge or 'N/A'} ({ce_hedge_ltp})")
        self.logger.info(f"SHORT PE: {pe_short or 'N/A'} ({pe_short_ltp}) | HEDGE PE: {pe_hedge or 'N/A'} ({pe_hedge_ltp})")
        self.logger.info("=" * 80)
        self.preview_profit_calculation()

    def atm(self, spot: float) -> int:
        return int(round(spot / 50) * 50)

    def enter(self) -> bool:
        from django.db import transaction

        with transaction.atomic():
            bot_status = BotStatus.objects.select_for_update().get(user=self.user)
            today = datetime.now(Config.TIMEZONE).date()
            today_str = str(today)
            if bot_status.entry_attempted_date == today:
                self.logger.info("ENTRY BLOCKED - already attempted (inner atomic check)")
                return False

        entry_call_time = datetime.now(Config.TIMEZONE)
        self.logger.info("=== ENTER() FUNCTION CALLED ===", {
            "exact_time": entry_call_time.strftime("%H:%M:%S.%f")[:-3],
            "iso_time": entry_call_time.isoformat()
        })

        self.state.daily_reset()

        today = datetime.now(Config.TIMEZONE).date()
        today_str = str(today)

        if self.state.bot_status.entry_attempted_date == today:
            self.logger.info("ENTRY BLOCKED — already traded today (final DB check)")
            return False

        is_ok, reason = self.is_trading_day()
        if not is_ok:
            self.logger.info("Non-trading day - skipping entry", {"reason": reason})
            return False

        now_time = datetime.now(Config.TIMEZONE).time()
        if not (Config.ENTRY_START <= now_time <= Config.ENTRY_END):
            self.logger.info("ENTRY SKIPPED", {"reason": "Outside entry window", "time": now_time.strftime("%H:%M:%S")})
            return False

        expiry = self.get_current_expiry_date()
        if expiry == today:
            self.logger.info("Expiry day - skipping entry")
            return False

        vix_val = self.vix()
        if not vix_val or not (Config.VIX_MIN <= vix_val <= Config.VIX_MAX):
            self.logger.info("VIX out of range", {"vix": vix_val})
            return False

        spot = self.spot()
        if not spot:
            return False

        atm_strike = self.atm(spot)
        ce_short = self.find_short_strike(atm_strike, "CE")
        pe_short = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ltps_short = self.bulk_ltp([ce_short_sym, pe_short_sym])
        ce_short_p = ltps_short.get(ce_short_sym, 0)
        pe_short_p = ltps_short.get(pe_short_sym, 0)
        if ce_short_p <= 0 or pe_short_p <= 0:
            self.logger.warning("Short premiums not live")
            return False

        ce_target = ce_short_p * Config.HEDGE_PREMIUM_RATIO
        pe_target = pe_short_p * Config.HEDGE_PREMIUM_RATIO
        common_target = min(ce_target, pe_target)
        ce_hedge = self.find_hedge_strike(ce_short, "CE", common_target, simulate=False)
        pe_hedge = self.find_hedge_strike(pe_short, "PE", common_target, simulate=False)
        ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
        pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")
        ltps_hedge = self.bulk_ltp([ce_hedge_sym, pe_hedge_sym])
        ce_hedge_p = ltps_hedge.get(ce_hedge_sym, 0.0)
        pe_hedge_p = ltps_hedge.get(pe_hedge_sym, 0.0)

        legs = []
        if pe_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE, "product": "MIS"})
        if ce_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE, "product": "MIS"})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"})

        lots = self.calculate_lots(legs)
        if lots == 0:
            return False

        qty = lots * Config.LOT_SIZE
        initial_margin, final_margin = self.exact_margin_for_basket([dict(l, quantity=qty) for l in legs])
        self.state.data["final_margin_used"] = final_margin

        capital_before = self.capital_available()
        self.logger.info("CAPITAL BEFORE ENTRY", {"available_₹": round(capital_before)})

        orders = []
        if pe_hedge_sym:
            orders.append((pe_hedge_sym, "BUY"))
        if ce_hedge_sym:
            orders.append((ce_hedge_sym, "BUY"))
        orders.append((pe_short_sym, "SELL"))
        orders.append((ce_short_sym, "SELL"))

        executed = []
        entry_prices = {}
        for sym, side in orders:
            success, order_id, filled_p = self.order(sym, side, qty)
            if success:
                executed.append((sym, side, order_id))
                entry_prices[sym] = filled_p
            else:
                self.cleanup(executed, qty)
                return False

        time.sleep(3.0)
        margin_samples = []
        for _ in range(4):
            try:
                avail = self.capital_available()
                if avail > 0:
                    margin_samples.append(avail)
            except:
                pass
            time.sleep(1.0)

        if margin_samples:
            capital_after = statistics.mean(margin_samples)
        else:
            capital_after = capital_before - 150000

        time.sleep(5)
        actual_margin_used = self.actual_used_capital()
        self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        margin_per_lot = actual_margin_used / lots if lots > 0 else 0

        trade_symbols = [s for s in [ce_short_sym, pe_short_sym, ce_hedge_sym, pe_hedge_sym] if s]
        algo_legs = {
            "CE_SHORT": create_leg(ce_short_sym, "SELL", qty, entry_prices.get(ce_short_sym, 0)),
            "PE_SHORT": create_leg(pe_short_sym, "SELL", qty, entry_prices.get(pe_short_sym, 0)),
        }
        if ce_hedge_sym:
            algo_legs["CE_HEDGE"] = create_leg(ce_hedge_sym, "BUY", qty, entry_prices.get(ce_hedge_sym, 0))
        if pe_hedge_sym:
            algo_legs["PE_HEDGE"] = create_leg(pe_hedge_sym, "BUY", qty, entry_prices.get(pe_hedge_sym, 0))

        self.state.data.update({
            "trade_active": True,
            "trade_taken_today": True,
            "entry_date": today_str,
            "trade_symbols": trade_symbols,
            "algo_legs": algo_legs,
            "positions": {
                "ce_short": ce_short,
                "pe_short": pe_short,
                "lots": lots,
                "qty": qty
            },
            "realistic_margin": max(final_margin * 0.6, 90000),
            "exact_margin_used_by_trade": actual_margin_used,
            "final_margin_used": final_margin,
            "margin_per_lot": margin_per_lot,
            "entry_vix": vix_val,
            "entry_spot": spot,
            "entry_atm": atm_strike,
            "entry_premiums": {
                "ce_short": entry_prices.get(ce_short_sym, 0),
                "pe_short": entry_prices.get(pe_short_sym, 0),
                "ce_hedge": entry_prices.get(ce_hedge_sym, 0),
                "pe_hedge": entry_prices.get(pe_hedge_sym, 0),
            },
            "qty": qty,
            "entry_time": datetime.now(Config.TIMEZONE)
        })

        if ce_hedge:
            self.state.data["positions"]["ce_hedge"] = ce_hedge
        if pe_hedge:
            self.state.data["positions"]["pe_hedge"] = pe_hedge

        position_qty = {
            ce_short_sym: -qty,
            pe_short_sym: -qty,
        }
        if ce_hedge_sym:
            position_qty[ce_hedge_sym] = +qty
        if pe_hedge_sym:
            position_qty[pe_hedge_sym] = +qty
        self.state.data["position_qty"] = position_qty

        self.logger.critical("=== POST-ENTRY STATE DEBUG SNAPSHOT ===")
        self.logger.critical(f"trade_active: {self.state.data.get('trade_active')}")
        self.logger.critical(f"qty / lots: {self.state.data.get('qty')} (lots: {self.state.data['positions'].get('lots')})")
        self.logger.critical(f"algo_legs keys: {list(self.state.data.get('algo_legs', {}).keys())}")
        self.logger.critical(f"algo_legs content: {self.state.data.get('algo_legs')}")
        self.logger.critical(f"entry_premiums: {self.state.data.get('entry_premiums')}")
        self.logger.critical(f"Initial net credit per lot approx: {self.state.data['entry_premiums'].get('ce_short', 0) + self.state.data['entry_premiums'].get('pe_short', 0) - self.state.data['entry_premiums'].get('ce_hedge', 0) - self.state.data['entry_premiums'].get('pe_hedge', 0):.2f}")
        current_pnl = self.algo_pnl()
        self.logger.critical(f"Current algo_pnl() immediately after entry: {current_pnl:.2f}")

        self.update_daily_profit_target()
        final_target = self.state.data["profit_target_rupee"]
        self.logger.big_banner(f"TRADE ENTERED SUCCESSFULLY | TODAY'S TARGET: +₹{final_target:,} | STOP LOSS: -₹{final_target:,}")

        self.state.save()
        time.sleep(2)
        return True

    def check_existing_positions(self) -> bool:
        try:
            net = self.kite.positions()["net"]
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
            self.logger.error("Failed to check existing positions", {"error": str(e)})
            return True

    def check_and_adjust_defensive(self) -> bool:
        if not self.state.data["trade_active"]:
            return False
        now_time = datetime.now(Config.TIMEZONE).time()
        if now_time >= Config.ADJUSTMENT_CUTOFF_TIME:
            return False
        today = datetime.now(Config.TIMEZONE).date()
        if self.state.data.get("last_adjustment_date") != str(today):
            self.state.data["adjustments_today"] = {"ce": 0, "pe": 0}
            self.state.data["last_adjustment_date"] = str(today)
        spot = self.spot()
        if not spot:
            return False
        current_atm = self.atm(spot)
        pos = self.state.data["positions"]
        qty = self.state.data["qty"]
        algo_legs = self.state.data.get("algo_legs", {})
        ce_short = pos.get("ce_short")
        pe_short = pos.get("pe_short")
        if not ce_short or not pe_short:
            return False
        adjusted = False
        if (spot >= ce_short + Config.ADJUSTMENT_TRIGGER_POINTS and
                self.state.data["adjustments_today"]["ce"] < Config.MAX_ADJUSTMENTS_PER_SIDE_PER_DAY):
            self.logger.info("DEFENSIVE ADJUSTMENT: CE STRUCK", {"spot": spot, "old_strike": ce_short})
            old_sym = self.find_option_symbol(ce_short, "CE")
            if not old_sym:
                return False
            current_pnl = self.algo_pnl()
            success, _, filled_p = self.order(old_sym, "BUY", qty)
            if not success:
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                    leg["exit_price"] = filled_p
                    leg["status"] = "CLOSED"
                    break
            pnl_after_close = self.algo_pnl()
            realized = current_pnl - pnl_after_close
            self.state.data["realized_pnl"] += realized
            new_ce_short = self.find_short_strike(current_atm, "CE")
            hedge_strike = pos.get("ce_hedge")
            if hedge_strike and abs(new_ce_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            new_sym = self.find_option_symbol(new_ce_short, "CE")
            if not new_sym:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            success, _, sell_filled_p = self.order(new_sym, "SELL", qty)
            if not success:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym:
                    leg["status"] = "CLOSED"
                    break
            algo_legs["CE_SHORT"] = create_leg(new_sym, "SELL", qty, sell_filled_p)
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
            actual_margin_used = self.actual_used_capital()
            self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        if (spot <= pe_short - Config.ADJUSTMENT_TRIGGER_POINTS and
                self.state.data["adjustments_today"]["pe"] < Config.MAX_ADJUSTMENTS_PER_SIDE_PER_DAY):
            self.logger.info("DEFENSIVE ADJUSTMENT: PE STRUCK", {"spot": spot, "old_strike": pe_short})
            old_sym = self.find_option_symbol(pe_short, "PE")
            if not old_sym:
                return False
            current_pnl = self.algo_pnl()
            success, _, filled_p = self.order(old_sym, "BUY", qty)
            if not success:
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                    leg["exit_price"] = filled_p
                    leg["status"] = "CLOSED"
                    break
            pnl_after_close = self.algo_pnl()
            realized = current_pnl - pnl_after_close
            self.state.data["realized_pnl"] += realized
            new_pe_short = self.find_short_strike(current_atm, "PE")
            hedge_strike = pos.get("pe_hedge")
            if hedge_strike and abs(new_pe_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            new_sym = self.find_option_symbol(new_pe_short, "PE")
            if not new_sym:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            success, _, sell_filled_p = self.order(new_sym, "SELL", qty)
            if not success:
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym:
                    leg["status"] = "CLOSED"
                    break
            algo_legs["PE_SHORT"] = create_leg(new_sym, "SELL", qty, sell_filled_p)
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
            actual_margin_used = self.actual_used_capital()
            self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        if adjusted:
            self.state.data["algo_legs"] = algo_legs
            self.update_daily_profit_target(force=True)
            self.state.save()
        return adjusted

    def check_exit(self) -> Optional[str]:
        now_t = datetime.now(Config.TIMEZONE).time()
        if now_t >= Config.EXIT_TIME:
            return "Scheduled exit time reached"
        now = datetime.now(Config.TIMEZONE)
        entry_time = self.state.data.get("entry_time")
        time_since_entry = (now - entry_time).total_seconds() if entry_time else float('inf')
        pnl_val = self.algo_pnl()
        target_rupee = self.state.data.get("profit_target_rupee", 0.0)
        if abs(pnl_val - self.last_pnl) > target_rupee * 0.2:
            self.logger.critical("P&L SPIKE DETECTED - IGNORING FOR SAFETY", {
                "current": pnl_val,
                "last": self.last_pnl,
                "jump": pnl_val - self.last_pnl
            })
            return None
        self.last_pnl = pnl_val
        if time_since_entry < 300 and abs(pnl_val) > 2500:
            self.logger.critical("EXTREME P&L GLITCH DETECTED - IGNORED", {
                "pnl": pnl_val,
                "seconds_since_entry": time_since_entry
            })
            return None
        if target_rupee > 0:
            if pnl_val >= target_rupee and time_since_entry >= Config.MIN_HOLD_SECONDS_FOR_PROFIT:
                return f"Profit target reached ₹{pnl_val:,.0f}"
            if pnl_val <= -target_rupee:
                return f"Stop loss hit ₹{pnl_val:,.0f}"
        vix_now = self.vix()
        if vix_now and vix_now >= Config.VIX_EXIT_ABS:
            return "VIX absolute exit"
        entry_vix = self.state.data.get("entry_vix")
        if entry_vix and vix_now and vix_now >= entry_vix * Config.VIX_SPIKE_MULTIPLIER:
            return "VIX spike detected"
        if os.path.exists(Config.EMERGENCY_STOP_FILE):
            return "Emergency stop file detected"
        return None

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
            unrealized_before = self.algo_pnl()
            realized_so_far = self.state.data.get("realized_pnl", 0.0)
            estimated = realized_so_far + unrealized_before
            self.logger.info("PRE-EXIT ESTIMATED PNL", {
                "estimated": round(estimated, 2),
                "realized": round(realized_so_far, 2),
                "unrealized": round(unrealized_before, 2)
            })

            net_positions = self.kite.positions()["net"]
            shorts = []
            hedges = []
            pos_qty_map = self.state.data.get("position_qty", {})
            bot_symbols = set(trade_syms)

            total_leg_pnl = Decimal('0.00')

            for pos in net_positions:
                sym = pos["tradingsymbol"]
                if pos["product"] != "MIS" or pos["quantity"] == 0 or sym not in bot_symbols:
                    continue
                expected_qty = pos_qty_map.get(sym)
                if expected_qty is None or abs(pos["quantity"]) != abs(expected_qty):
                    self.logger.warning("Qty mismatch - skipping", {"sym": sym})
                    continue
                exit_price = self.bulk_ltp([sym])[sym]
                # Close Trade record and accumulate PnL
                leg_pnl = self._close_trade_record(sym, exit_price)
                total_leg_pnl += leg_pnl
                if pos["quantity"] < 0:
                    shorts.append((sym, "BUY", abs(pos["quantity"])))
                else:
                    hedges.append((sym, "SELL", pos["quantity"]))

            # Execute close orders
            self.logger.info("Closing shorts first")
            for sym, side, qty in shorts:
                self.order(sym, side, qty)
            time.sleep(1.5)

            self.logger.info("Closing hedges")
            for sym, side, qty in hedges:
                self.order(sym, side, qty)
            time.sleep(5)

            # Final PnL
            still_open = self._get_still_open_mis_positions()
            unrealized_after = self.algo_pnl() if still_open else 0.0
            final_pnl = float(total_leg_pnl) + unrealized_after

            self.state.data["exit_final_pnl"] = final_pnl
            self.state.data["realized_pnl"] = float(total_leg_pnl)
            self.state.save()

            self.logger.critical("EXIT SUMMARY", {
                "estimated_before": round(estimated, 2),
                "final_after": round(final_pnl, 2),
                "still_open": len(still_open),
                "reason": reason
            })

            if still_open:
                self.logger.critical("PARTIAL EXIT - emergency flatten")
                self._emergency_square_off("Partial exit")

            # Update DailyPnL - accumulate
            today = datetime.now(Config.TIMEZONE).date()
            with transaction.atomic():
                daily, created = DailyPnL.objects.get_or_create(
                    user=self.user,
                    date=today,
                    defaults={
                        'pnl': Decimal('0.00'),
                        'total_trades': 0,
                        'win_trades': 0,
                        'loss_trades': 0,
                    }
                )
                # Add to existing
                daily.pnl = (daily.pnl or Decimal('0.00')) + Decimal(str(final_pnl)).quantize(Decimal('0.00'))
                if len(still_open) == 0:
                    daily.total_trades = (daily.total_trades or 0) + 1
                    if final_pnl > 0:
                        daily.win_trades = (daily.win_trades or 0) + 1
                    else:
                        daily.loss_trades = (daily.loss_trades or 0) + 1
                daily.save()

            self.logger.critical("DailyPnL UPDATED", {
                "date": today,
                "added_pnl": round(final_pnl, 2),
                "new_total": float(daily.pnl),
                "total_trades": daily.total_trades
            })

            self.state.full_reset()

        except Exception as e:
            self.logger.critical("EXIT CRASHED", {"error": str(e), "trace": traceback.format_exc()})
            self._emergency_square_off("Exit crashed")
            raise

    def _get_still_open_mis_positions(self):
        try:
            pos = self.kite.positions()["net"]
            return [
                p["tradingsymbol"]
                for p in pos
                if p["product"] == "MIS" and p["quantity"] != 0
            ]
        except:
            return ["fetch_failed"]

    def _emergency_square_off(self, reason="Emergency"):
        self.logger.critical(f"EMERGENCY FLATTEN: {reason}")
        try:
            for pos in self.kite.positions()["net"]:
                if pos["product"] == "MIS" and pos["quantity"] != 0:
                    qty = abs(pos["quantity"])
                    side = "SELL" if pos["quantity"] > 0 else "BUY"
                    for _ in range(2):
                        self.order(pos["tradingsymbol"], side, qty)
                        time.sleep(2)
        except Exception as e:
            self.logger.critical("EMERGENCY FLATTEN FAILED", {"error": str(e)})

    def save_periodic_pnl_snapshot(self):
        try:
            if not self.state.data.get("trade_active"):
                return
            unrealized = self.algo_pnl()
            today = datetime.now(Config.TIMEZONE).date()
            with transaction.atomic():
                daily, created = DailyPnL.objects.update_or_create(
                    user=self.user,
                    date=today,
                    defaults={
                        'pnl': Decimal(str(unrealized)).quantize(Decimal('0.00')),
                    }
                )
            self.logger.info("PERIODIC SNAPSHOT SAVED", {
                "pnl": round(unrealized, 2),
                "record_id": daily.id
            })
        except Exception as e:
            self.logger.warning("Periodic snapshot failed", {"error": str(e)})

# ===================== MAIN APPLICATION =====================
class TradingApplication:
    def __init__(self, user, broker):
        self.user = user
        self.broker = broker
        self.logger = DBLogger(user)
        self.engine = Engine(user, broker, self.logger)
        self.running = False
        self.token_refreshed_today = False
        self._vix_logged = False
        self._snapshot_logged = False
        self._early_0919_logged = False
        self._manual_preview_triggered = False
        self._idle_logged_today = False
        self._last_idle_date = None
        self._last_hourly_log = 0
        self._last_entry_check_time = 0.0
        self._entry_guard_seconds = 20.0
        self._last_token_health_check = time.time()
        self._daily_summary_saved = False
        self._last_snapshot_time = time.time()

    def run(self):
        self.running = True
        self.logger.info("=== HEDGED STRANGLE BOT STARTED (DJANGO VERSION) ===")
        last_pnl_print = time.time()
        last_heartbeat = time.time()

        try:
            while self.running:
                now_str = datetime.now(Config.TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                self.logger.critical(f"[LOOP ALIVE] {now_str} | running={self.running} | trade_active={self.engine.state.data.get('trade_active', False)}")

                try:
                    bot_status = BotStatus.objects.get(user=self.user)
                    if not bot_status.is_running:
                        self.logger.critical("STOP SIGNAL RECEIVED FROM DATABASE - shutting down gracefully")
                        if self.engine.state.data.get("trade_active"):
                            self.engine.exit("Manual stop from dashboard")
                        self.running = False
                        break
                except Exception as e:
                    self.logger.critical("STOP-CHECK FAILED ─ DB may be the problem", {
                        "error": str(e),
                        "trace": traceback.format_exc()[:800]
                    })

                now = datetime.now(Config.TIMEZONE)
                current_time = now.time()
                today_date = now.date()

                self.logger.info(f"[TIME CHECK] Current: {current_time.strftime('%H:%M:%S')} | Entry window: {Config.ENTRY_START} – {Config.ENTRY_END}")

                self.engine.state.daily_reset()

                if self._last_idle_date != today_date:
                    self._early_0919_logged = False
                    self._snapshot_logged = False
                    self._idle_logged_today = False
                    self._daily_summary_saved = False
                    self._last_idle_date = today_date

                try:
                    net = self.engine.kite.positions()["net"]
                    bot_symbols = set(self.engine.state.data.get("trade_symbols", []))
                    if bot_symbols:
                        current_qty = sum(
                            abs(p["quantity"])
                            for p in net
                            if p["product"] == "MIS" and p["tradingsymbol"] in bot_symbols
                        )
                        expected_qty = self.engine.state.data.get("qty", 0)
                        if expected_qty > 0 and current_qty == 0:
                            self.logger.big_banner("MANUAL CLOSE DETECTED - AUTO RECOVERING")
                            self.engine.state.full_reset()
                except Exception as e:
                    self.logger.error("Periodic manual close check failed", {"error": str(e)})

                if dtime(8, 55) <= current_time < dtime(10, 5):
                    if self.engine.instruments is None or self.engine.weekly_df is None:
                        self.logger.info("PRE-LOADING instruments & weekly data")
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()

                if dtime(9, 19) <= current_time < dtime(9, 20) and not self._early_0919_logged:
                    self.logger.big_banner("EARLY MARKET PREVIEW - 09:19 IST (Pre-Entry Setup)")
                    try:
                        if self.engine.instruments is None or self.engine.weekly_df is None:
                            self.logger.info("Loading fresh instruments & weekly data for 09:19 preview")
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                        spot_now = self.engine.spot()
                        vix_now = self.engine.vix()
                        if spot_now:
                            self.logger.info("Spot at 09:19", {"spot": spot_now})
                        if vix_now:
                            self.logger.info("VIX at 09:19", {"vix": vix_now})
                        self.engine.startup_banner()
                        self.engine.preview_profit_calculation()
                        self._early_0919_logged = True
                    except Exception as e:
                        self.logger.error("09:19 preview failed", {
                            "error": str(e),
                            "trace": traceback.format_exc()
                        })

                if dtime(9, 0) <= current_time < dtime(9, 30) and not self._snapshot_logged:
                    if self.engine.instruments is None:
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()
                    self.engine.startup_banner()
                    self._snapshot_logged = True

                if current_time >= Config.TOKEN_REFRESH_TIME and not self.token_refreshed_today:
                    attempts = 0
                    while attempts < Config.MAX_TOKEN_ATTEMPTS:
                        attempts += 1
                        self.logger.info(f"Daily token refresh attempt {attempts}")
                        access_token = generate_and_set_access_token_db(self.engine.kite, self.broker)
                        if access_token:
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                            self.token_refreshed_today = True
                            self.engine.startup_banner()
                            break
                        time.sleep(60)
                    else:
                        self.logger.critical("Token refresh failed - exiting")
                        break

                if current_time < Config.TOKEN_REFRESH_TIME:
                    self.token_refreshed_today = False
                    self._snapshot_logged = False

                if time.time() - self._last_token_health_check > 900:
                    try:
                        self.engine.kite.profile()
                        self._last_token_health_check = time.time()
                    except TokenException:
                        self.logger.critical("TOKEN EXPIRED MID-DAY — attempting re-auth")
                        success = self.engine._authenticate()
                        if success:
                            self.logger.info("Mid-day token refresh succeeded")
                        else:
                            self.logger.critical("Mid-day re-auth FAILED — stopping bot")
                            self.running = False
                    except Exception as e:
                        self.logger.warning("Token health check failed", {"error": str(e)})
                    self._last_token_health_check = time.time()

                if self.engine.instruments is not None and self.engine.weekly_df is not None:
                    if self.engine.state.data["trade_active"]:
                        reason = self.engine.check_exit()
                        if reason:
                            self.engine.exit(reason)
                        else:
                            adjusted = self.engine.check_and_adjust_defensive()
                            if adjusted:
                                self.engine.update_daily_profit_target(force=True)
                            if time.time() - self._last_hourly_log >= 3600:
                                current_pnl = self.engine.algo_pnl()
                                actual_used = self.engine.actual_used_capital()
                                self.logger.info("HOURLY STATUS", {
                                    "unrealized_pnl_₹": round(current_pnl, 2),
                                    "target_₹": round(self.engine.state.data.get("profit_target_rupee", 0)),
                                    "actual_capital_blocked_₹": round(actual_used)
                                })
                                self._last_hourly_log = time.time()
                            if time.time() - last_pnl_print >= 1.0:
                                pnl_now = self.engine.algo_pnl()
                                self.logger.info("Live unrealized P&L", {
                                    "pnl_₹": round(pnl_now, 2),
                                    "legs_active": len(self.engine.state.data.get("algo_legs", {}))
                                })
                                last_pnl_print = time.time()
                    else:
                        if Config.ENTRY_START <= current_time <= Config.ENTRY_END:
                            today_weekday = datetime.now(Config.TIMEZONE).weekday()
                            if today_weekday == 1:
                                self.logger.critical("TUESDAY SKIP ACTIVATED - NO ENTRY ATTEMPT TODAY")
                                time.sleep(300)
                                continue
                            self.logger.critical("ENTRY WINDOW IS OPEN RIGHT NOW", {
                                "current_time": current_time.strftime("%H:%M:%S"),
                                "start": Config.ENTRY_START.strftime("%H:%M:%S"),
                                "end": Config.ENTRY_END.strftime("%H:%M:%S")
                            })
                            now_ts = time.time()
                            if now_ts - self._last_entry_check_time < 60:
                                self.logger.info("Entry guard active — skipping this cycle (60s cooldown)")
                                time.sleep(5)
                                continue
                            self._last_entry_check_time = now_ts
                            self.logger.critical("=== DECIDED TO ATTEMPT ENTRY ===")
                            try:
                                bot_status = BotStatus.objects.get(user=self.user)
                                self.logger.critical(f"entry_attempted_date in DB = {bot_status.entry_attempted_date}")
                                self.logger.critical(f"trade_taken_today = {self.engine.state.data.get('trade_taken_today')}")
                            except:
                                self.logger.critical("Cannot read DB state")
                            try:
                                success = self.engine.enter()
                                if success:
                                    bot_status = BotStatus.objects.get(user=self.user)
                                    bot_status.last_successful_entry = timezone.now()
                                    bot_status.save(update_fields=['last_successful_entry'])
                                    self.logger.big_banner("ENTRY SUCCESS — day permanently locked")
                                    time.sleep(300)
                                else:
                                    self.logger.warning("Entry attempt failed — will retry next minute if still in window")
                                    time.sleep(10)
                            except Exception as e:
                                self.logger.error("Entry execution crashed", {"error": str(e), "trace": traceback.format_exc()})
                                time.sleep(30)

                if current_time >= Config.MARKET_CLOSE and not self._daily_summary_saved:
                    try:
                        unrealized = self.engine.algo_pnl()
                        today = datetime.now(Config.TIMEZONE).date()
                        with transaction.atomic():
                            daily, created = DailyPnL.objects.update_or_create(
                                user=self.user,
                                date=today,
                                defaults={
                                    'pnl': Decimal(str(unrealized)),
                                    'total_trades': len(self.engine.state.data.get("trade_symbols", [])) // 4 if self.engine.state.data.get("trade_symbols") else 0,
                                    'win_trades': 1 if unrealized > 0 else 0,
                                    'loss_trades': 1 if unrealized < 0 else 0,
                                }
                            )
                        self.logger.big_banner(f"DAILY PnL SAVED AT MARKET CLOSE | PnL: ₹{unrealized:,.2f} | Record {'created' if created else 'updated'}")
                        self._daily_summary_saved = True
                    except Exception as e:
                        self.logger.error("Market close PnL save failed", {"error": str(e)})

                if time.time() - last_heartbeat >= Config.HEARTBEAT_INTERVAL:
                    try:
                        bot_status = BotStatus.objects.get(user=self.user)
                        current_pnl = float(self.engine.algo_pnl() or 0)
                        current_margin = float(self.engine.actual_used_capital() or 0)
                        bot_status.last_heartbeat = timezone.now()
                        bot_status.current_unrealized_pnl = Decimal(str(current_pnl))
                        bot_status.current_margin = Decimal(str(current_margin))
                        bot_status.save(update_fields=[
                            'last_heartbeat',
                            'current_unrealized_pnl',
                            'current_margin'
                        ])
                        self.logger.info("Heartbeat saved", {
                            "unrealized_pnl": round(current_pnl, 2),
                            "margin": round(current_margin, 2)
                        })
                    except Exception as e:
                        self.logger.warning("Heartbeat save failed", {"error": str(e)})
                    last_heartbeat = time.time()

                if self.engine.state.data.get("trade_active"):
                    if time.time() - self._last_snapshot_time >= Config.PERIODIC_PNL_SNAPSHOT_INTERVAL:
                        self.save_periodic_pnl_snapshot()
                        self._last_snapshot_time = time.time()

                time.sleep(1.0)

        except KeyboardInterrupt:
            self.logger.info("Bot stopped by user")
            if self.engine.state.data.get("trade_active"):
                self.engine.exit("Manual stop")
        except Exception as e:
            self.logger.critical("Fatal error in main loop", {"error": str(e), "trace": traceback.format_exc()})
        finally:
            self.running = False
            self.logger.info("Bot loop exited")
            try:
                bot_status = BotStatus.objects.get(user=self.user)
                bot_status.last_heartbeat = timezone.now()
                bot_status.is_running = False
                bot_status.save(update_fields=['last_heartbeat', 'is_running'])
                self.logger.info("Final heartbeat & is_running=False saved on shutdown")
            except Exception as e:
                self.logger.warning("Failed to save final heartbeat on shutdown", {"error": str(e)})

    def save_periodic_pnl_snapshot(self):
        try:
            if not self.engine.state.data.get("trade_active"):
                return
            unrealized = self.engine.algo_pnl()
            today = datetime.now(Config.TIMEZONE).date()
            with transaction.atomic():
                daily, created = DailyPnL.objects.update_or_create(
                    user=self.user,
                    date=today,
                    defaults={
                        'pnl': Decimal(str(unrealized)),
                        'total_trades': len(self.engine.state.data.get("trade_symbols", [])) // 4,
                        'win_trades': 1 if unrealized > 0 else 0,
                        'loss_trades': 1 if unrealized < 0 else 0,
                    }
                )
            self.logger.info("PERIODIC PnL SNAPSHOT SAVED", {
                "pnl": round(unrealized, 2),
                "record": daily.id,
                "created": created
            })
        except Exception as e:
            self.logger.warning("Periodic PnL snapshot failed", {"error": str(e)})