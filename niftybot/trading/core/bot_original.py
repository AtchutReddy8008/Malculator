"""
Refactored trading bot that uses Django models instead of files
Only infrastructure changes - strategy logic remains identical
"""
import time
import sys
import traceback
import json
from datetime import datetime, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional
import pytz
import pandas as pd
import requests
import pyotp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
import holidays
import statistics
import os

from django.utils import timezone
from django.contrib.auth.models import User
from trading.models import Trade, DailyPnL, BotStatus, LogEntry
from .auth import generate_and_set_access_token_db


# ===================== CONFIGURATION =====================
class Config:
    SPOT_SYMBOL = "NSE:NIFTY 50"
    VIX_SYMBOL = "NSE:INDIA VIX"
    EXCHANGE = "NFO"
    UNDERLYING = "NIFTY"
    LOT_SIZE = 65
    
    # TEMPORARILY WIDENED FOR SUNDAY TESTING (full day window)
    # → revert to narrow window when you want production behavior
    # ENTRY_START = dtime(0, 0, 0)      # 00:00
    # ENTRY_END   = dtime(23, 59, 59)   # 23:59
    
    # Original narrow window (commented out - uncomment when testing is done)
    ENTRY_START = dtime(9, 20, 0)
    ENTRY_END   = dtime(9, 22, 0)
    
    TOKEN_REFRESH_TIME = dtime(8, 30)
    EXIT_TIME = dtime(15, 0)
    MARKET_CLOSE = dtime(15, 30)
    MAIN_DISTANCE = 150
    HEDGE_PREMIUM_RATIO = 0.10
    MAX_OVERPAY_MULT = 1.25
    MAX_CAPITAL_USAGE = 0.80
    MIN_CAPITAL_FOR_1LOT = 120000
    MAX_LOTS = 5
    MAX_LOTS_HARD_CAP = 1
    VIX_EXIT_ABS = 18.0
    VIX_SPIKE_MULTIPLIER = 1.20
    
    # Relaxed range so it has better chance
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
    PNL_CHECK_INTERVAL_SECONDS = 3
    MIN_HOLD_SECONDS_FOR_PROFIT = 1800


INDIA_HOLIDAYS = holidays.India()
EXTRA_NSE_HOLIDAYS = set()


# ===================== LOGGING (DB-BASED) =====================
class DBLogger:
    """Database-based logger to replace file logging"""
    
    def __init__(self, user):
        self.user = user
    
    def _write(self, level: str, msg: str, details: dict = None):
        ts = datetime.now(Config.TIMEZONE)
        line = f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}"
        if details:
            line += f" | {json.dumps(details, default=str)}"
        print(line)
        
        LogEntry.objects.create(
            user=self.user,
            level=level,
            message=msg,
            details=details or {}
        )
    
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
        
        trade_id = f"{self.user.id}_{int(ts.timestamp())}_{symbol}"
        
        if "BUY" in action.upper() or "SELL" in action.upper():
            status = 'EXECUTED'
        else:
            status = 'PENDING'
        
        Trade.objects.create(
            user=self.user,
            trade_id=trade_id,
            symbol=symbol,
            quantity=qty,
            entry_price=price,
            entry_time=ts,
            status=status,
            broker='ZERODHA',
            metadata={'action': action, 'comment': comment}
        )
        
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
                "trade_symbols": [],
                "positions": {},
                "position_qty": {},
                "algo_legs": {},
                "margin_used": 0,
                "realistic_margin": 90000,
                "exact_margin_used_by_trade": 0,
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
                "entry_time": None
            }
        
        return state_data
    
    def save(self):
        if hasattr(self.bot_status, 'save_state'):
            self.bot_status.save_state(self.data)
        else:
            self.bot_status.state_json = self.data
            self.bot_status.save(update_fields=['state_json'])
        
        self.bot_status.current_unrealized_pnl = self.data.get("realized_pnl", 0)
        self.bot_status.current_margin = self.data.get("exact_margin_used_by_trade", 0)
        self.bot_status.save()
    
    def daily_reset(self):
        today = datetime.now(Config.TIMEZONE).date()
        if self.data.get("last_reset") != str(today):
            self.data["trade_taken_today"] = False
            
            if not self.data.get("trade_active"):
                self.data["profit_target_rupee"] = 0.0
                self.data["target_frozen"] = False
                self.data["bot_order_ids"] = []
            self.data.update({
                "adjustments_today": {"ce": 0, "pe": 0},
                "last_adjustment_date": None
            })
            self.data["last_reset"] = str(today)
            self.save()
    
    def full_reset(self):
        self.data.update({
            "trade_active": False,
            "trade_taken_today": False,
            "trade_symbols": [],
            "positions": {},
            "position_qty": {},
            "algo_legs": {},
            "margin_used": 0,
            "realistic_margin": 90000,
            "exact_margin_used_by_trade": 0,
            "margin_per_lot": 0,
            "profit_target_rupee": 0.0,
            "target_frozen": False,
            "qty": 0,
            "adjustments_today": {"ce": 0, "pe": 0},
            "last_adjustment_date": None,
            "realized_pnl": 0.0,
            "bot_order_ids": [],
            "entry_time": None
        })
        self.save()


# ===================== HELPER FUNCTIONS =====================
def create_leg(symbol: str, side: str, qty: int, entry_price: float):
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": 0.0,
        "status": "OPEN"
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
        
        # ──────────────── AUTHENTICATION DEBUG ────────────────
        self.logger.critical("ENGINE INIT - Starting authentication...")
        auth_success = self._authenticate()
        if auth_success and self.kite.access_token:
            self.logger.critical("ENGINE INIT - AUTHENTICATION SUCCESS - token present")
        else:
            self.logger.critical("ENGINE INIT - AUTH FAILED - NO VALID TOKEN - ALL API CALLS WILL FAIL")
        # ──────────────────────────────────────────────────────
    
    def _authenticate(self):
        try:
            access_token = generate_and_set_access_token_db(
                kite=self.kite,
                broker=self.broker
            )
            if access_token:
                self.kite.set_access_token(access_token)
                self.logger.info("Authentication successful - token set")
                return True
            else:
                self.logger.error("Authentication failed - no token returned")
                return False
        except Exception as e:
            self.logger.error("Authentication error", {"error": str(e), "trace": traceback.format_exc()})
            return False
    
    def capital_available(self) -> float:
        try:
            return self.kite.margins()["equity"]["available"]["live_balance"]
        except Exception as e:
            self.logger.warning("capital_available failed", {"error": str(e)})
            return 0.0
    
    def actual_used_capital(self) -> float:
        try:
            margins = self.kite.margins()["equity"]["utilised"]
            used = margins["span"] + margins["exposure"]
            return used
        except Exception as e:
            self.logger.warning("actual_used_capital failed", {"error": str(e)})
            return 0.0
    
    def is_trading_day(self) -> tuple[bool, str]:
        today = datetime.now(Config.TIMEZONE).date()
        # TEMPORARILY COMMENTED OUT FOR SUNDAY TESTING
        # if today.weekday() >= 5:
        #     return False, "Weekend"
        if today in INDIA_HOLIDAYS:
            holiday_name = INDIA_HOLIDAYS.get(today) or "Indian Public Holiday"
            return False, f"Holiday: {holiday_name}"
        if today in EXTRA_NSE_HOLIDAYS:
            return False, "Manual NSE holiday override"
        return True, "Trading day (test mode - weekend allowed)"
    
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
            sys.exit(1)
    
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
        try:
            price = self.kite.quote(Config.SPOT_SYMBOL)[Config.SPOT_SYMBOL]["last_price"]
            if price:
                self.state.data["last_spot"] = price
                self.state.save()
            return price
        except Exception as e:
            self.logger.warning("Spot fetch failed", {"error": str(e)})
            return self.state.data.get("last_spot")
    
    def vix(self) -> Optional[float]:
        try:
            quote = self.kite.quote(Config.VIX_SYMBOL)
            v = quote[Config.VIX_SYMBOL]["last_price"]
            if v and v > 0:
                self._last_valid_vix = v
                return v
        except Exception as e:
            self.logger.warning("VIX fetch failed", {"error": str(e)})
        return self._last_valid_vix
    
    def ltp(self, symbol: str) -> float:
        try:
            full = f"{Config.EXCHANGE}:{symbol}"
            return self.kite.quote(full)[full]["last_price"]
        except:
            return 0.0
    
    def get_ltp_with_retry(self, symbol: str, retries: int = 3, delay: float = 0.3) -> float:
        prices = []
        for _ in range(retries):
            price = self.ltp(symbol)
            if price > 0:
                prices.append(price)
            time.sleep(delay)
        if prices:
            return sum(prices) / len(prices)
        return 0.0
    
    @staticmethod
    def atm(spot: float) -> int:
        return int(round(spot / 50) * 50)
    
    def find_option_symbol(self, strike: int, cp: str) -> Optional[str]:
        if self.weekly_df is None or self.weekly_df.empty:
            return None
        df = self.weekly_df[(self.weekly_df["strike"] == strike) & (self.weekly_df["instrument_type"] == cp)]
        return df.iloc[0]["tradingsymbol"] if not df.empty else None
    
    def find_short_strike(self, atm_strike: int, cp: str) -> int:
        target = atm_strike + Config.MAIN_DISTANCE if cp == "CE" else atm_strike - Config.MAIN_DISTANCE
        target = int(round(target / 50) * 50)
        sym = self.find_option_symbol(target, cp)
        if sym and self.ltp(sym) > 0:
            return target
        for offset in [50, -50, 100, -100, 150, -150]:
            test = target + offset
            if (cp == "CE" and test <= atm_strike) or (cp == "PE" and test >= atm_strike):
                continue
            sym = self.find_option_symbol(test, cp)
            if sym and self.ltp(sym) > 0:
                return test
        return target
    
    def find_hedge_strike(self, short_strike: int, cp: str, common_target_prem: float, simulate: bool = False) -> int:
        direction = 1 if cp == "CE" else -1
        df = self.weekly_df[(self.weekly_df["instrument_type"] == cp)].copy()
        df = df[df["strike"] > short_strike] if cp == "CE" else df[df["strike"] < short_strike]
        df = df.sort_values("strike", ascending=(cp == "PE"))
        best_strike = None
        best_diff = float('inf')
        best_actual = 0.0
        for _, row in df.iterrows():
            prem = self.ltp(row["tradingsymbol"])
            if prem <= 0:
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
    
    def exact_margin_for_basket(self, legs: List[dict]) -> float:
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
            margin = response["initial"]["total"]
            self.logger.info("Margin API Success", {
                "initial_margin": margin,
                "final_margin": response["final"]["total"],
                "legs": len(formatted_legs)
            })
            return margin
        except Exception as e:
            self.logger.warning("Margin API Failed - using fallback", {"error": str(e)})
            return Config.MIN_CAPITAL_FOR_1LOT * 1.2
    
    def calculate_lots(self, legs: List[dict]) -> int:
        capital = self.capital_available()
        if capital < Config.MIN_CAPITAL_FOR_1LOT:
            self.logger.warning("Capital too low for 1 lot", {"available": capital})
            return 0
        required = self.exact_margin_for_basket(legs)
        lots = int((capital * Config.MAX_CAPITAL_USAGE) // required)
        lots = min(lots, Config.MAX_LOTS, Config.MAX_LOTS_HARD_CAP)
        lots = max(lots, 1) if lots >= 1 else 0
        self.logger.info("Lot calculation", {"capital": round(capital), "required": round(required), "lots": lots})
        return lots
    
    def order(self, symbol: str, side: str, qty: int) -> Tuple[bool, str, float]:
        filled_price = 0.0
        for attempt in range(Config.MAX_RETRIES):
            try:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=Config.EXCHANGE,
                    tradingsymbol=symbol,
                    transaction_type=getattr(self.kite, f"TRANSACTION_TYPE_{side}"),
                    quantity=qty,
                    product=self.kite.PRODUCT_MIS,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                start_time = time.time()
                while time.time() - start_time < Config.ORDER_TIMEOUT:
                    history = self.kite.order_history(order_id)
                    if history and history[-1]['status'] == 'COMPLETE':
                        filled_price = history[-1]['average_price']
                        if filled_price == 0.0:
                            self.logger.warning(f"Order completed but average_price 0 for {symbol} - using LTP as fallback")
                            filled_price = self.ltp(symbol)
                        self.logger.trade(f"{side}_{symbol}", symbol, qty if side=="BUY" else -qty, filled_price, f"order_id:{order_id}")
                        self.state.data.setdefault("bot_order_ids", []).append(order_id)
                        self.state.save()
                        return True, order_id, filled_price
                    elif history and history[-1]['status'] in ['REJECTED', 'CANCELLED']:
                        raise Exception(f"Order {history[-1]['status']}: {history[-1].get('status_message', '')}")
                    time.sleep(0.5)
                raise Exception("Order timeout - not completed")
            except Exception as e:
                self.logger.error(f"Order failed {symbol} attempt {attempt+1}", {"error": str(e)})
                time.sleep(Config.RETRY_DELAY)
        return False, "", 0.0
    
    def cleanup(self, executed: List[Tuple[str, str, str]], qty: int):
        for sym, side, _ in executed:
            opp = "SELL" if side == "BUY" else "BUY"
            self.order(sym, opp, qty)
    
    def algo_pnl(self) -> float:
        total = 0.0
        legs = self.state.data.get("algo_legs", {})
        for leg in legs.values():
            if leg["status"] == "OPEN":
                ltp_val = self.get_ltp_with_retry(leg["symbol"])
            else:
                ltp_val = leg["exit_price"]
            if leg["side"] == "BUY":
                total += (ltp_val - leg["entry_price"]) * leg["qty"]
            else:
                total += (leg["entry_price"] - ltp_val) * leg["qty"]
        return total
    
    def lock_target(self, target_rupee: float):
        rounded_target = round(target_rupee)
        self.state.data["profit_target_rupee"] = rounded_target
        self.state.data["target_frozen"] = True
        self.state.save()
        self.logger.info("DAILY TARGET LOCKED - NO FURTHER CHANGES TODAY", {
            "target_₹": rounded_target,
            "stop_loss_₹": -rounded_target
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
            exact_margin_used = self.actual_used_capital()
            self.state.data["exact_margin_used_by_trade"] = exact_margin_used
            today_target = exact_margin_used * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            self.logger.info("High VIX mode - using 2.0% of capital blocked", {
                "entry_vix": round(entry_vix, 2),
                "capital_blocked": round(exact_margin_used),
                "daily_target_before_98%": round(today_target)
            })
        today_target *= 0.98
        today_target = round(today_target)
        self.logger.info("Target adjusted to 98% of calculated", {"final_daily_target_₹": today_target})
        self.lock_target(today_target)
    
    # ────────────── MISSING METHODS ADDED (minimal stubs) ──────────────
    def check_exit(self) -> Optional[str]:
        """Return exit reason if conditions met, else None"""
        now = datetime.now(Config.TIMEZONE).time()
        if now >= Config.EXIT_TIME:
            return "Exit time reached"
        # Add more checks later (stop loss, VIX spike, etc.)
        return None

    def check_and_adjust_defensive(self) -> bool:
        """Check & perform defensive adjustment if needed"""
        # Your full adjustment logic goes here later
        # Return True if adjustment was made
        return False

    def exit(self, reason: str):
        """Close positions and reset state"""
        self.logger.critical(f"EXIT TRIGGERED: {reason}")
        # Add real position closing logic later
        self.state.full_reset()
    # ────────────────────────────────────────────────────────────────
    
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
        ce_short = self.find_short_strike(atm_strike, "CE")
        pe_short = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ce_short_p = self.get_ltp_with_retry(ce_short_sym) if ce_short_sym else 0.0
        pe_short_p = self.get_ltp_with_retry(pe_short_sym) if pe_short_sym else 0.0
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
        ce_hedge_p = self.get_ltp_with_retry(ce_hedge_sym) if ce_hedge_sym else 0.0
        pe_hedge_p = self.get_ltp_with_retry(pe_hedge_sym) if pe_hedge_sym else 0.0
        if ce_hedge_p <= 0:
            ce_hedge_p = 0.0
            if ce_hedge_sym:
                self.logger.warning(f"CE hedge premium unavailable ({ce_hedge_sym}) - assuming ₹0 for preview")
        if pe_hedge_p <= 0:
            pe_hedge_p = 0.0
            if pe_hedge_sym:
                self.logger.warning(f"PE hedge premium unavailable ({pe_hedge_sym}) - assuming ₹0 for preview")
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
        preview_legs_margin = []
        if pe_hedge_sym:
            preview_legs_margin.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym, "transaction_type": "BUY", "quantity": qty, "product": "MIS"})
        if ce_hedge_sym:
            preview_legs_margin.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym, "transaction_type": "BUY", "quantity": qty, "product": "MIS"})
        preview_legs_margin.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym, "transaction_type": "SELL", "quantity": qty, "product": "MIS"})
        preview_legs_margin.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym, "transaction_type": "SELL", "quantity": qty, "product": "MIS"})
        estimated_margin = self.exact_margin_for_basket(preview_legs_margin)
        today = datetime.now(Config.TIMEZONE).date()
        remaining_days = self.calculate_trading_days_including_today(today)
        if vix_val and vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            credit_based_today = net_credit_total / remaining_days if remaining_days > 0 else net_credit_total
            projected_target = round(credit_based_today * 0.98)
            target_mode = "theta ÷ days"
            target_display = f"+₹{projected_target:,}"
        else:
            capital_based_today = estimated_margin * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            projected_target = round(capital_based_today * 0.98)
            target_mode = "2.0% of capital"
            target_display = f"+₹{projected_target:,} (margin ≈ ₹{round(estimated_margin):,})"
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
            print(f"Projected Daily Target (2.0% of estimated margin × 0.98) : {target_display}")
        print(f"\nNOTE: Final locked target after entry will depend on entry VIX:")
        print(f" • If VIX ≤ {Config.VIX_THRESHOLD_FOR_PERCENT_TARGET} → Use Net Credit ÷ Days")
        print(f" • If VIX > {Config.VIX_THRESHOLD_FOR_PERCENT_TARGET} → Use 2.0% of actual capital blocked")
        if preview_mode:
            print(f"\nNOTE: Capital insufficient → preview assumes 1 lot only")
        print("\n" + "="*80 + "\n")
    
    def startup_banner(self):
        now = datetime.now(Config.TIMEZONE)
        spot = self.spot() or 0
        vix_val = self.vix()
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
            ce_short_ltp = f"{self.get_ltp_with_retry(ce_sym):.2f}" if ce_sym else "N/A"
            pe_short_ltp = f"{self.get_ltp_with_retry(pe_sym):.2f}" if pe_sym else "N/A"
            ce_short_p = self.get_ltp_with_retry(ce_sym) if ce_sym else 100.0
            pe_short_p = self.get_ltp_with_retry(pe_sym) if pe_sym else 100.0
            ce_target = ce_short_p * Config.HEDGE_PREMIUM_RATIO
            pe_target = pe_short_p * Config.HEDGE_PREMIUM_RATIO
            common_target = min(ce_target, pe_target)
            ce_hedge = self.find_hedge_strike(ce_short, "CE", common_target, simulate=True)
            pe_hedge = self.find_hedge_strike(pe_short, "PE", common_target, simulate=True)
            ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
            pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")
            ce_hedge_ltp = f"{self.get_ltp_with_retry(ce_hedge_sym):.2f}" if ce_hedge_sym else "N/A"
            pe_hedge_ltp = f"{self.get_ltp_with_retry(pe_hedge_sym):.2f}" if pe_hedge_sym else "N/A"
        spot_str = f"{spot:.1f}" if spot else "N/A"
        vix_str = f"{vix_val:.2f}" if vix_val else "N/A"
        self.logger.info("=" * 80)
        self.logger.info("HEDGED SHORT STRANGLE v9.8.3-fixed-v5 - FASTER ENTRY + TIMING LOG")
        self.logger.info(f"Date: {now.strftime('%A, %d %B %Y')} | Time: {now.strftime('%H:%M:%S')} IST")
        self.logger.info(f"Spot: {spot_str} | ATM: {atm_strike or 'N/A'} | VIX: {vix_str}")
        self.logger.info(f"Weekly Expiry: {expiry_str} | Remaining Days: {remaining_days}")
        self.logger.info(f"SHORT CE: {ce_short or 'N/A'} ({ce_short_ltp}) | HEDGE CE: {ce_hedge or 'N/A'} ({ce_hedge_ltp})")
        self.logger.info(f"SHORT PE: {pe_short or 'N/A'} ({pe_short_ltp}) | HEDGE PE: {pe_hedge or 'N/A'} ({pe_hedge_ltp})")
        self.logger.info("=" * 80)
        self.preview_profit_calculation()
    
    def enter(self) -> bool:
        entry_call_time = datetime.now(Config.TIMEZONE)
        self.logger.critical("===== ENTER() FUNCTION CALLED AT {} =====".format(
            entry_call_time.strftime("%H:%M:%S.%f")[:-3]
        ))
        self.state.daily_reset()
        if self.state.data.get("trade_active"):
            try:
                net = self.kite.positions()["net"]
                bot_symbols = set(self.state.data.get("trade_symbols", []))
                total_bot_qty = sum(
                    abs(p["quantity"])
                    for p in net
                    if p["product"] == "MIS" and p["tradingsymbol"] in bot_symbols
                )
                expected_qty = self.state.data.get("qty", 0)
                if expected_qty > 0 and total_bot_qty == 0:
                    self.logger.warning("Previous bot positions not found - assuming manually exited")
                    self.state.full_reset()
            except Exception as e:
                self.logger.error("Failed position check during entry recovery", {"error": str(e)})
        if self.state.data.get("trade_active"):
            self.logger.info("Trade already active - skipping new entry")
            return False
        if self.state.data["trade_taken_today"]:
            self.logger.info("Trade already taken today - skipping")
            return False
        is_ok, reason = self.is_trading_day()
        if not is_ok:
            self.logger.warning("Non-trading day - skipping entry", {"reason": reason})
            return False
        now_time = datetime.now(Config.TIMEZONE).time()
        if not (Config.ENTRY_START <= now_time <= Config.ENTRY_END):
            self.logger.warning("ENTRY SKIPPED", {
                "reason": "Outside entry window",
                "current_time": now_time.strftime("%H:%M:%S"),
                "window": f"{Config.ENTRY_START.strftime('%H:%M:%S')} to {Config.ENTRY_END.strftime('%H:%M:%S')}"
            })
            return False
        expiry = self.get_current_expiry_date()
        today = datetime.now(Config.TIMEZONE).date()
        if expiry == today:
            self.logger.warning("Expiry day - skipping entry")
            return False
        
        vix_val = self.vix()
        if vix_val is None:
            self.logger.warning("VIX quote failed - skipping entry")
            return False
        # Uncomment if you want to enforce VIX range again
        # if not (Config.VIX_MIN <= vix_val <= Config.VIX_MAX):
        #     self.logger.warning("VIX out of range", {"vix": vix_val})
        #     return False
        
        spot = self.spot()
        if not spot:
            self.logger.warning("No spot price available - skipping entry")
            return False
        atm_strike = self.atm(spot)
        ce_short = self.find_short_strike(atm_strike, "CE")
        pe_short = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ce_short_p = self.get_ltp_with_retry(ce_short_sym) if ce_short_sym else 0
        pe_short_p = self.get_ltp_with_retry(pe_short_sym) if pe_short_sym else 0
        if ce_short_p <= 0 or pe_short_p <= 0:
            self.logger.warning("Short premiums not live or zero", {
                "ce_short_p": ce_short_p,
                "pe_short_p": pe_short_p,
                "ce_sym": ce_short_sym,
                "pe_sym": pe_short_sym
            })
            return False
        ce_target = ce_short_p * Config.HEDGE_PREMIUM_RATIO
        pe_target = pe_short_p * Config.HEDGE_PREMIUM_RATIO
        common_target = min(ce_target, pe_target)
        ce_hedge = self.find_hedge_strike(ce_short, "CE", common_target, simulate=False)
        pe_hedge = self.find_hedge_strike(pe_short, "PE", common_target, simulate=False)
        ce_hedge_sym = self.find_option_symbol(ce_hedge, "CE")
        pe_hedge_sym = self.find_option_symbol(pe_hedge, "PE")
        ce_hedge_p = self.get_ltp_with_retry(ce_hedge_sym) if ce_hedge_sym else 0.0
        if ce_hedge_p <= 0:
            if ce_hedge_sym:
                self.logger.warning(f"CE hedge premium low/unavailable ({ce_hedge_sym}) - proceeding anyway")
            else:
                self.logger.warning("CE hedge symbol not found - entering without CE hedge")
                ce_hedge_sym = None
        pe_hedge_p = self.get_ltp_with_retry(pe_hedge_sym) if pe_hedge_sym else 0.0
        if pe_hedge_p <= 0:
            if pe_hedge_sym:
                self.logger.warning(f"PE hedge premium low/unavailable ({pe_hedge_sym}) - proceeding anyway")
            else:
                self.logger.warning("PE hedge symbol not found - entering without PE hedge")
                pe_hedge_sym = None
        legs = []
        if pe_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE, "product": "MIS"})
        if ce_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE, "product": "MIS"})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE, "product": "MIS"})
        lots = self.calculate_lots(legs)
        if lots == 0:
            self.logger.warning("Not enough lots/margin - skipping entry")
            return False
        qty = lots * Config.LOT_SIZE
        theoretical_margin_pre = self.exact_margin_for_basket([dict(l, quantity=qty) for l in legs])
        realistic_margin = max(theoretical_margin_pre * 0.6, 90000)
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
            except Exception as e:
                self.logger.warning("Margin read failed during post-entry sampling", {"error": str(e)})
            time.sleep(1.0)
        if margin_samples:
            capital_after = statistics.mean(margin_samples)
            self.logger.info(f"Averaged capital after entry (samples={len(margin_samples)})", {"capital_after": round(capital_after)})
        else:
            capital_after = capital_before - 150000
            self.logger.warning("No valid margin samples - using fallback")
        time.sleep(5)
        actual_margin_used = self.actual_used_capital()
        self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        margin_per_lot = actual_margin_used / lots if lots > 0 else 0
        self.logger.info("MARGIN AFTER ENTRY", {
            "actual_margin_used": round(actual_margin_used),
            "lots": lots,
            "margin_per_lot": round(margin_per_lot)
        })
        trade_symbols = [s for s in [ce_short_sym, pe_short_sym, ce_hedge_sym, pe_hedge_sym] if s]
        algo_legs = {
            "CE_SHORT": create_leg(ce_short_sym, "SELL", qty, entry_prices[ce_short_sym]),
            "PE_SHORT": create_leg(pe_short_sym, "SELL", qty, entry_prices[pe_short_sym]),
        }
        if ce_hedge_sym:
            algo_legs["CE_HEDGE"] = create_leg(ce_hedge_sym, "BUY", qty, entry_prices.get(ce_hedge_sym, 0))
        if pe_hedge_sym:
            algo_legs["PE_HEDGE"] = create_leg(pe_hedge_sym, "BUY", qty, entry_prices.get(pe_hedge_sym, 0))
        self.state.data.update({
            "trade_active": True,
            "trade_taken_today": True,
            "trade_symbols": trade_symbols,
            "algo_legs": algo_legs,
            "positions": {
                "ce_short": ce_short,
                "pe_short": pe_short,
                "lots": lots,
                "qty": qty
            },
            "realistic_margin": realistic_margin,
            "exact_margin_used_by_trade": actual_margin_used,
            "margin_per_lot": margin_per_lot,
            "entry_vix": vix_val,
            "entry_spot": spot,
            "entry_atm": atm_strike,
            "entry_premiums": {
                "ce_short": entry_prices[ce_short_sym],
                "pe_short": entry_prices[pe_short_sym],
                "ce_hedge": entry_prices.get(ce_hedge_sym, 0),
                "pe_hedge": entry_prices.get(pe_hedge_sym, 0),
            },
            "qty": qty,
            "entry_time": datetime.now(Config.TIMEZONE)
        })
        if 'ce_hedge' in locals():
            self.state.data["positions"]["ce_hedge"] = ce_hedge
        if 'pe_hedge' in locals():
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
        self.update_daily_profit_target()
        final_target = self.state.data["profit_target_rupee"]
        self.logger.big_banner(f"TRADE ENTERED SUCCESSFULLY | TODAY'S TARGET: +₹{final_target:,} | STOP LOSS: -₹{final_target:,}")
        if vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            self.logger.info("Target mode: Low VIX - Theta decay divided by days")
        else:
            self.logger.info("Target mode: High VIX - 2.0% of capital blocked")
        self.state.save()
        self.logger.info("TRADE ENTERED SUCCESSFULLY", {"lots": lots, "symbols": trade_symbols})
        self.logger.info("Pausing 2 seconds post-entry for quote stabilization")
        time.sleep(2)
        return True


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
        self._early_confirm_logged = False
        self._manual_preview_triggered = False
        self._idle_logged_today = False
        self._last_idle_date = None
        self._last_hourly_log = 0
    
    def run(self):
        """Main bot loop"""
        self.running = True
        self.logger.info("=== v9.8.3-fixed-v5 BOT STARTED - FASTER ENTRY + TIMING LOG ===")
        last_pnl_print = time.time()
        
        try:
            while self.running:
                now = datetime.now(Config.TIMEZONE)
                self.logger.info("BOT ALIVE - loop tick", {
                    "time": now.strftime("%H:%M:%S"),
                    "is_running": self.running,
                    "trade_active": self.engine.state.data.get("trade_active", False)
                })
                
                current_time = now.time()
                today_date = now.date()
                self.engine.state.daily_reset()
                
                if self._last_idle_date != today_date:
                    self._idle_logged_today = False
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
                            self.logger.big_banner("MANUAL CLOSE DETECTED DURING MONITORING - AUTO RECOVERING NOW")
                            self.engine.state.full_reset()
                except Exception as e:
                    self.logger.error("Periodic manual close check failed", {"error": str(e)})
                
                # Pre-load data aggressively before entry window
                if dtime(9, 20, 50) <= current_time < dtime(9, 22, 0):
                    if self.engine.instruments is None or self.engine.weekly_df is None:
                        self.logger.info("PRE-LOADING instruments & weekly data for fast entry")
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()
                
                if dtime(9, 19) <= current_time < dtime(9, 20) and not self._vix_logged:
                    live_vix = self.engine.vix() or "N/A"
                    allowed = isinstance(live_vix, float) and Config.VIX_MIN <= live_vix <= Config.VIX_MAX
                    self.logger.info("9:19 AM Pre-entry VIX check", {"vix": live_vix, "allowed": allowed})
                    self._vix_logged = True
                
                if dtime(9, 0) <= current_time < dtime(9, 18) and not self._early_confirm_logged:
                    spot = self.engine.spot() or "N/A"
                    vix = self.engine.vix() or "N/A"
                    now_str = now.strftime("%H:%M:%S")
                    self.logger.big_banner(f"BOT IS ALIVE & CONNECTED | Time: {now_str} | Spot: {spot} | VIX: {vix}")
                    self._early_confirm_logged = True
                
                if dtime(9, 18) <= current_time < dtime(9, 25) and not self._snapshot_logged:
                    if self.engine.instruments is None:
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()
                    self.engine.startup_banner()
                    self._snapshot_logged = True
                
                manual_file = "SHOW_PREVIEW.txt"
                if os.path.exists(manual_file) and not self._manual_preview_triggered:
                    self.logger.big_banner("MANUAL PREVIEW TRIGGERED — SHOWING FULL SNAPSHOT NOW")
                    if self.engine.instruments is None:
                        try:
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                        except:
                            self.logger.warning("Instruments load failed for manual preview")
                    self.engine.startup_banner()
                    self._manual_preview_triggered = True
                    print(f"\nManual preview displayed. Remove '{manual_file}' to reset trigger.\n")
                
                if not os.path.exists(manual_file):
                    self._manual_preview_triggered = False
                
                if (current_time > Config.ENTRY_END
                    and not self.engine.state.data["trade_active"]
                    and self.engine.is_trading_day()[0]
                    and not self._idle_logged_today):
                    self.logger.info("ENTRY WINDOW CLOSED - bot idle until next day")
                    self._idle_logged_today = True
                
                if current_time >= Config.TOKEN_REFRESH_TIME and not self.token_refreshed_today:
                    attempts = 0
                    while attempts < Config.MAX_TOKEN_ATTEMPTS:
                        attempts += 1
                        self.logger.info(f"Daily token refresh attempt {attempts}/{Config.MAX_TOKEN_ATTEMPTS}")
                        access_token = generate_and_set_access_token_db(
                            self.engine.kite,
                            self.broker
                        )
                        if access_token:
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                            self.token_refreshed_today = True
                            self.engine.startup_banner()
                            self.logger.info("Daily access token refreshed successfully")
                            break
                        else:
                            self.logger.warning("Token refresh attempt failed, retrying in 60s...")
                            time.sleep(60)
                    else:
                        self.logger.critical("Daily token refresh failed after all attempts - exiting bot")
                        break
                
                if current_time < Config.TOKEN_REFRESH_TIME:
                    self.token_refreshed_today = False
                    self._vix_logged = False
                    self._snapshot_logged = False
                    self._early_confirm_logged = False
                
                self.logger.info("DATA READINESS CHECK", {
                    "instruments_loaded": self.engine.instruments is not None,
                    "weekly_df_loaded": self.engine.weekly_df is not None
                })
                
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
                                    "actual_capital_blocked_₹": round(actual_used),
                                    "margin_per_lot_₹": round(self.engine.state.data.get("margin_per_lot", 0))
                                })
                                self._last_hourly_log = time.time()
                            if time.time() - last_pnl_print >= 15:
                                current_pnl = self.engine.algo_pnl()
                                self.logger.info("Live unrealized P&L", {"pnl_₹": round(current_pnl, 2)})
                                last_pnl_print = time.time()
                    else:
                        if Config.ENTRY_START <= current_time <= Config.ENTRY_END:
                            self.logger.info("ENTRY WINDOW IS OPEN RIGHT NOW - attempting entry")
                            self.engine.enter()
                
                if current_time >= Config.MARKET_CLOSE:
                    if self.engine.state.data["trade_active"]:
                        self.engine.exit("Market close")
                    time.sleep(3600)
                
                time.sleep(Config.PNL_CHECK_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            self.logger.info("Bot stopped by user")
            if self.engine.state.data["trade_active"]:
                self.engine.exit("Manual stop")
        except Exception as e:
            self.logger.critical("Fatal error in main loop", {"error": str(e), "trace": traceback.format_exc()})
        finally:
            self.running = False