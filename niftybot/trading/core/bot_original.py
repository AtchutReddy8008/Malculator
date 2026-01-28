#!/usr/bin/env python3
"""
ULTIMATE PRODUCTION HEDGED SHORT STRANGLE - NIFTY WEEKLY
Version: 9.8.3-fixed-v5 - FASTER ENTRY + EXACT TIMING LOG + FINAL_MARGIN_TARGET
Only changed: high VIX target uses 2% of final_margin from basket_order_margins
"""
import time
import sys
import traceback
import json
import csv
import os
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

# ===================== CONFIGURATION =====================
class Config:
    SPOT_SYMBOL = "NSE:NIFTY 50"
    VIX_SYMBOL = "NSE:INDIA VIX"
    EXCHANGE = "NFO"
    UNDERLYING = "NIFTY"
    LOT_SIZE = 65
    ENTRY_START = dtime(9, 21, 0)
    ENTRY_END = dtime(9, 21, 30)
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
    VIX_MIN = 9.0
    VIX_MAX = 22.0
    VIX_THRESHOLD_FOR_PERCENT_TARGET = 12
    PERCENT_TARGET_WHEN_VIX_HIGH = 0.020  # 2.0%
    ADJUSTMENT_TRIGGER_POINTS = 50
    ADJUSTMENT_CUTOFF_TIME = dtime(13, 30)
    MAX_ADJUSTMENTS_PER_SIDE_PER_DAY = 1
    MIN_HEDGE_GAP = 300
    TIMEZONE = pytz.timezone("Asia/Kolkata")
    LOG_FILE = "hedged_strangle_v9.8.3_final_margin.log"
    TRADE_LOG = "trades_v9.8.3_final_margin.csv"
    STATE_FILE = "state_v9.8.3_final_margin.json"
    EMERGENCY_STOP_FILE = "EMERGENCY_STOP.txt"
    MAX_TOKEN_ATTEMPTS = 10
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    ORDER_TIMEOUT = 10
    PNL_CHECK_INTERVAL_SECONDS = 3
    MIN_HOLD_SECONDS_FOR_PROFIT = 1800  # 30 minutes

INDIA_HOLIDAYS = holidays.India()
EXTRA_NSE_HOLIDAYS = set()

# ===================== CREDENTIALS =====================
def get_credentials():
    print("\n" + "="*80)
    print("HEDGED SHORT STRANGLE v9.8.3 - FINAL_MARGIN TARGET UPDATE")
    print("AUTO LOGIN ENABLED")
    print("="*80)
    api_key = "zeth17dj3c6sazmn"
    api_secret = "c1lu3d0ihqrkulvb392fc4l6vb7jtuts"
    user_id = "IES848"
    password = "Balaji@123"
    totp_secret = "NNBFBLRKJ5XHPCVN3OJG4OHQFK4ODPLT"
    if not all([api_key, api_secret, user_id, password, totp_secret]):
        print("ERROR: Missing credentials!")
        sys.exit(1)
    print(f"User: {user_id}")
    print("="*80 + "\n")
    return api_key, api_secret, user_id, password, totp_secret

API_KEY, API_SECRET, USER_ID, PASSWORD, TOTP_SECRET = get_credentials()

# ===================== LOGGING =====================
class Logger:
    @staticmethod
    def _write(level: str, msg: str, details: dict = None):
        ts = datetime.now(Config.TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"
        if details:
            line += f" | {json.dumps(details, default=str)}"
        print(line)
        try:
            with open(Config.LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except:
            pass

    @staticmethod
    def info(msg: str, details: dict = None): Logger._write("INFO", msg, details)
    @staticmethod
    def warning(msg: str, details: dict = None): Logger._write("WARNING", msg, details)
    @staticmethod
    def error(msg: str, details: dict = None): Logger._write("ERROR", msg, details)
    @staticmethod
    def critical(msg: str, details: dict = None): Logger._write("CRITICAL", msg, details)

    @staticmethod
    def trade(action: str, symbol="", qty=0, price=0.0, comment=""):
        ts = datetime.now(Config.TIMEZONE)
        file_exists = os.path.isfile(Config.TRADE_LOG)
        try:
            with open(Config.TRADE_LOG, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["Date", "Time", "Action", "Symbol", "Qty", "Price", "Comment"])
                w.writerow([ts.strftime("%Y-%m-%d"), ts.strftime("%H:%M:%S"), action, symbol, qty, f"{price:.2f}", comment])
        except Exception as e:
            Logger.error("Trade log failed", {"error": str(e)})

    @staticmethod
    def big_banner(msg: str):
        print("\n" + "="*80)
        print(f"*** {msg} ***".center(80))
        print("="*80 + "\n")
        with open(Config.LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "="*80 + "\n")
            f.write(f"*** {msg} ***\n")
            f.write("="*80 + "\n\n")

# ===================== AUTO TOKEN =====================
def generate_and_set_access_token(kite: KiteConnect) -> bool:
    try:
        Logger.info("Generating new access token...")
        session = requests.Session()
        login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
        resp = session.get(login_url, timeout=15)
        if resp.status_code != 200:
            raise Exception(f"Login page failed: {resp.status_code}")
        current_url = resp.url
        login_resp = session.post("https://kite.zerodha.com/api/login",
                                  data={"user_id": USER_ID, "password": PASSWORD}, timeout=15)
        login_data = login_resp.json()
        if login_data.get("status") != "success":
            raise Exception(f"Login failed: {login_data}")
        request_id = login_data["data"]["request_id"]
        Logger.info("Login successful")
        totp_code = pyotp.TOTP(TOTP_SECRET).now()
        Logger.info(f"TOTP generated: {totp_code}")
        twofa_resp = session.post("https://kite.zerodha.com/api/twofa",
                                  data={"user_id": USER_ID, "request_id": request_id,
                                        "twofa_value": totp_code, "twofa_type": "totp"}, timeout=15)
        if twofa_resp.json().get("status") != "success":
            raise Exception("2FA failed")
        Logger.info("2FA successful")
        redirect_resp = session.get(current_url + "&skip_session=true", allow_redirects=True, timeout=15)
        request_token = parse_qs(urlparse(redirect_resp.url).query)["request_token"][0]
        Logger.info("Request token obtained")
        data = kite.generate_session(request_token=request_token, api_secret=API_SECRET)
        kite.set_access_token(data["access_token"])
        Logger.info("Access token set successfully")
        return True
    except Exception as e:
        Logger.critical("Token generation failed", {"error": str(e), "trace": traceback.format_exc()})
        return False

# ===================== HELPER =====================
def create_leg(symbol: str, side: str, qty: int, entry_price: float):
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": 0.0,
        "status": "OPEN"
    }

# ===================== STATE =====================
class State:
    def __init__(self):
        self.data = {
            "trade_active": False,
            "trade_taken_today": False,
            "trade_symbols": [],
            "positions": {},
            "position_qty": {},
            "algo_legs": {},
            "margin_used": 0,
            "realistic_margin": 90000,
            "exact_margin_used_by_trade": 0,
            "final_margin_used": 0.0,  # NEW: stored final margin for target
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
        self.load()

    def load(self):
        if os.path.exists(Config.STATE_FILE):
            try:
                with open(Config.STATE_FILE) as f:
                    saved = json.load(f)
                    for k in self.data:
                        if k in saved:
                            self.data[k] = saved[k]
                Logger.info("State loaded")
            except:
                Logger.warning("State load failed")

    def save(self):
        try:
            with open(Config.STATE_FILE, "w") as f:
                json.dump(self.data, f, indent=2, default=str)
        except:
            pass

    def daily_reset(self):
        today = datetime.now(Config.TIMEZONE).date()
        if self.data.get("last_reset") != str(today):
            if not self.data.get("trade_active"):
                self.data["profit_target_rupee"] = 0.0
                self.data["target_frozen"] = False
                self.data["bot_order_ids"] = []
            self.data.update({
                "trade_taken_today": False,
                "adjustments_today": {"ce": 0, "pe": 0},
                "last_adjustment_date": None
            })
            self.data["last_reset"] = str(today)
            self.save()
            Logger.info("Daily reset complete")

    def full_reset(self):
        Logger.big_banner("AUTO RECOVERY: RESETTING BOT STATE AFTER MANUAL/MISMATCH CLOSE")
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
            "final_margin_used": 0.0,
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
        Logger.info("Full state reset complete - bot ready for new trade")

# ===================== ENGINE =====================
class Engine:
    def __init__(self):
        self.kite = KiteConnect(api_key=API_KEY)
        self.state = State()
        self.instruments = None
        self.weekly_df = None
        self._last_valid_vix: Optional[float] = None

    def capital_available(self) -> float:
        try:
            return self.kite.margins()["equity"]["available"]["live_balance"]
        except:
            return 0.0

    def actual_used_capital(self) -> float:
        try:
            margins = self.kite.margins()["equity"]["utilised"]
            used = margins["span"] + margins["exposure"]
            return used
        except:
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
            Logger.info(f"Loaded {len(df)} options")
        except Exception as e:
            Logger.critical("Instrument load failed", {"error": str(e), "trace": traceback.format_exc()})
            sys.exit(1)

    def load_weekly_df(self):
        if self.instruments is None or self.instruments.empty:
            self.weekly_df = pd.DataFrame()
            return
        try:
            today = datetime.now(Config.TIMEZONE).date()
            next_tue = today + timedelta(days=(1 - today.weekday()) % 7 or 7)
            self.weekly_df = self.instruments[self.instruments["expiry"] == next_tue].copy()
            Logger.info("Weekly cache updated", {"expiry": next_tue.strftime("%Y-%m-%d"), "count": len(self.weekly_df)})
        except Exception as e:
            Logger.error("Weekly cache failed", {"error": str(e)})
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
        except:
            return self.state.data.get("last_spot")

    def vix(self) -> Optional[float]:
        try:
            quote = self.kite.quote(Config.VIX_SYMBOL)
            v = quote[Config.VIX_SYMBOL]["last_price"]
            if v and v > 0:
                self._last_valid_vix = v
                return v
        except:
            pass
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
                Logger.info("Symmetric hedge selected", {
                    "side": cp,
                    "common_target": round(common_target_prem, 2),
                    "strike": best_strike,
                    "actual": round(best_actual, 2),
                    "diff": round(best_actual - common_target_prem, 2)
                })
            return best_strike
        fallback = short_strike + direction * 1000
        if not simulate:
            Logger.warning("Symmetric hedge fallback used", {"side": cp, "strike": fallback})
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
            Logger.info("Margin API Success", {
                "initial_margin": round(initial),
                "final_margin": round(final),
                "legs": len(formatted_legs)
            })
            return initial, final
        except Exception as e:
            Logger.warning("Margin API Failed - using fallback", {"error": str(e)})
            fallback = Config.MIN_CAPITAL_FOR_1LOT * 1.2
            return fallback, fallback * 0.85  # rough estimate

    def calculate_lots(self, legs: List[dict]) -> int:
        capital = self.capital_available()
        if capital < Config.MIN_CAPITAL_FOR_1LOT:
            return 0
        initial_margin, _ = self.exact_margin_for_basket(legs)
        lots = int((capital * Config.MAX_CAPITAL_USAGE) // initial_margin)
        lots = min(lots, Config.MAX_LOTS, Config.MAX_LOTS_HARD_CAP)
        lots = max(lots, 1) if lots >= 1 else 0
        Logger.info("Lot calculation", {"capital": round(capital), "initial_margin": round(initial_margin), "lots": lots})
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
                            Logger.warning(f"Order completed but average_price 0 for {symbol} - using LTP as fallback")
                            filled_price = self.ltp(symbol)
                        Logger.trade(f"{side}_{symbol}", symbol, qty if side=="BUY" else -qty, filled_price, f"order_id:{order_id}")
                        self.state.data.setdefault("bot_order_ids", []).append(order_id)
                        self.state.save()
                        return True, order_id, filled_price
                    elif history and history[-1]['status'] in ['REJECTED', 'CANCELLED']:
                        raise Exception(f"Order {history[-1]['status']}: {history[-1].get('status_message', '')}")
                    time.sleep(0.5)
                raise Exception("Order timeout - not completed")
            except Exception as e:
                Logger.error(f"Order failed {symbol} attempt {attempt+1}", {"error": str(e)})
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
        Logger.info("DAILY TARGET LOCKED - NO FURTHER CHANGES TODAY", {
            "target_₹": rounded_target,
            "stop_loss_₹": -rounded_target
        })

    def update_daily_profit_target(self, force: bool = False):
        if self.state.data["target_frozen"] and not force:
            Logger.info("Target already frozen today - skipping recalculation")
            return
        if not self.state.data["trade_active"]:
            return
        entry_vix = self.state.data.get("entry_vix")
        if entry_vix is None:
            Logger.warning("Missing entry_vix - cannot calculate target")
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
            Logger.info("Low VIX mode - using Net Credit ÷ Days", {
                "entry_vix": round(entry_vix, 2),
                "total_credit": round(total_credit),
                "remaining_days": remaining_days,
                "daily_target_before_98%": round(today_target)
            })
        else:
            # === ONLY CHANGE HERE: prefer final_margin when VIX > 12 ===
            margin_for_target = self.state.data.get("final_margin_used", 0.0)
            if margin_for_target <= 0:
                margin_for_target = self.actual_used_capital()
                Logger.warning("final_margin_used not available → fallback to actual used capital")
            else:
                Logger.info("Using FINAL_MARGIN for high VIX target calculation")
            today_target = margin_for_target * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            Logger.info("High VIX mode - using 2.0% of final_margin", {
                "entry_vix": round(entry_vix, 2),
                "final_margin": round(margin_for_target),
                "daily_target_before_98%": round(today_target)
            })
        today_target *= 0.98
        today_target = round(today_target)
        Logger.info("Target adjusted to 98% of calculated", {"final_daily_target_₹": today_target})
        self.lock_target(today_target)

    def preview_profit_calculation(self):
        live_spot = self.spot()
        if live_spot:
            spot = live_spot
        else:
            Logger.warning("Spot price temporarily unavailable - using last known values for preview")
            spot = self.state.data.get("last_spot") or self.state.data.get("entry_spot") or 24500
        atm_strike = self.atm(spot)
        Logger.info("PREVIEW SNAPSHOT (NOT LOCKED)", {"Spot": spot, "ATM": atm_strike})
        vix_val = self.vix()
        if not vix_val:
            Logger.warning("VIX not available - continuing preview")
        ce_short = self.find_short_strike(atm_strike, "CE")
        pe_short = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ce_short_p = self.get_ltp_with_retry(ce_short_sym) if ce_short_sym else 0.0
        pe_short_p = self.get_ltp_with_retry(pe_short_sym) if pe_short_sym else 0.0
        if ce_short_p <= 0 or pe_short_p <= 0:
            Logger.warning("Short leg premiums not reliably available - skipping preview")
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
                Logger.warning(f"CE hedge premium unavailable ({ce_hedge_sym}) - assuming ₹0 for preview")
        if pe_hedge_p <= 0:
            pe_hedge_p = 0.0
            if pe_hedge_sym:
                Logger.warning(f"PE hedge premium unavailable ({pe_hedge_sym}) - assuming ₹0 for preview")
        legs = []
        if pe_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
        if ce_hedge_sym:
            legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_hedge_sym, "transaction_type": "BUY", "quantity": Config.LOT_SIZE})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": pe_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE})
        legs.append({"exchange": Config.EXCHANGE, "tradingsymbol": ce_short_sym, "transaction_type": "SELL", "quantity": Config.LOT_SIZE})
        initial_margin, final_margin = self.exact_margin_for_basket(legs)
        actual_lots = int((self.capital_available() * Config.MAX_CAPITAL_USAGE) // initial_margin)
        actual_lots = min(max(actual_lots, 1 if actual_lots >= 1 else 0), Config.MAX_LOTS, Config.MAX_LOTS_HARD_CAP)
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
            capital_based_today = final_margin * Config.PERCENT_TARGET_WHEN_VIX_HIGH
            projected_target = round(capital_based_today * 0.98)
            target_mode = "2.0% of final margin"
            target_display = f"+₹{projected_target:,} (final margin ≈ ₹{round(final_margin):,})"
        mode = "PREVIEW (ASSUMING 1 LOT)" if preview_mode else "LIVE PREVIEW"
        hedge_note = " (some hedges unavailable → assuming ₹0 cost)" if (ce_hedge_p == 0 or pe_hedge_p == 0) else ""
        spot_note = " (Spot temporarily unavailable)" if live_spot is None else ""
        vix_note = f" | Current VIX: {vix_val:.2f} → using {target_mode} logic" if vix_val else ""
        Logger.big_banner(f"{mode}{hedge_note}{spot_note}{vix_note} | PROJECTED DAILY TARGET: {target_display} | STOP LOSS: -{target_display.replace('+','')}")
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
            print(f"Projected Daily Target (2.0% of final margin × 0.98) : {target_display}")
        print(f"\nNOTE: Final locked target after entry will depend on entry VIX:")
        print(f" • If VIX ≤ {Config.VIX_THRESHOLD_FOR_PERCENT_TARGET} → Use Net Credit ÷ Days")
        print(f" • If VIX > {Config.VIX_THRESHOLD_FOR_PERCENT_TARGET} → Use 2.0% of final margin from basket API")
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
        Logger.info("=" * 80)
        Logger.info("HEDGED SHORT STRANGLE v9.8.3 - FINAL_MARGIN TARGET UPDATE")
        Logger.info(f"Date: {now.strftime('%A, %d %B %Y')} | Time: {now.strftime('%H:%M:%S')} IST")
        Logger.info(f"Spot: {spot_str} | ATM: {atm_strike or 'N/A'} | VIX: {vix_str}")
        Logger.info(f"Weekly Expiry: {expiry_str} | Remaining Days: {remaining_days}")
        Logger.info(f"SHORT CE: {ce_short or 'N/A'} ({ce_short_ltp}) | HEDGE CE: {ce_hedge or 'N/A'} ({ce_hedge_ltp})")
        Logger.info(f"SHORT PE: {pe_short or 'N/A'} ({pe_short_ltp}) | HEDGE PE: {pe_hedge or 'N/A'} ({pe_hedge_ltp})")
        Logger.info("=" * 80)
        self.preview_profit_calculation()

    def enter(self) -> bool:
        entry_call_time = datetime.now(Config.TIMEZONE)
        Logger.info("=== ENTER() FUNCTION CALLED ===", {
            "exact_time": entry_call_time.strftime("%H:%M:%S.%f")[:-3],
            "iso_time": entry_call_time.isoformat()
        })
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
                    Logger.warning("Previous bot positions not found - assuming manually exited")
                    self.state.full_reset()
            except Exception as e:
                Logger.error("Failed position check during entry recovery", {"error": str(e)})
        if self.state.data.get("trade_active"):
            Logger.info("Trade already active - skipping new entry")
            return False
        if self.state.data["trade_taken_today"]:
            return False
        is_ok, reason = self.is_trading_day()
        if not is_ok:
            Logger.info("Non-trading day - skipping entry", {"reason": reason})
            return False
        now_time = datetime.now(Config.TIMEZONE).time()
        if not (Config.ENTRY_START <= now_time <= Config.ENTRY_END):
            Logger.info("ENTRY SKIPPED", {"reason": "Outside entry window", "time": now_time.strftime("%H:%M:%S")})
            return False
        expiry = self.get_current_expiry_date()
        today = datetime.now(Config.TIMEZONE).date()
        if expiry == today:
            Logger.info("Expiry day - skipping entry")
            return False
        today_weekday = datetime.now(Config.TIMEZONE).weekday()
        if today_weekday == 1:
            Logger.info("ENTRY SKIPPED", {"reason": "User-defined rule: Skip all trades on Tuesdays"})
            return False
        vix_val = self.vix()
        if not vix_val or not (Config.VIX_MIN <= vix_val <= Config.VIX_MAX):
            Logger.info("VIX out of range", {"vix": vix_val})
            return False
        spot = self.spot()
        if not spot:
            return False
        atm_strike = self.atm(spot)
        ce_short = self.find_short_strike(atm_strike, "CE")
        pe_short = self.find_short_strike(atm_strike, "PE")
        ce_short_sym = self.find_option_symbol(ce_short, "CE")
        pe_short_sym = self.find_option_symbol(pe_short, "PE")
        ce_short_p = self.get_ltp_with_retry(ce_short_sym) if ce_short_sym else 0
        pe_short_p = self.get_ltp_with_retry(pe_short_sym) if pe_short_sym else 0
        if ce_short_p <= 0 or pe_short_p <= 0:
            Logger.warning("Short premiums not live")
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
                Logger.warning(f"CE hedge premium low/unavailable ({ce_hedge_sym}) - proceeding anyway")
            else:
                Logger.warning("CE hedge symbol not found - entering without CE hedge")
                ce_hedge_sym = None
        pe_hedge_p = self.get_ltp_with_retry(pe_hedge_sym) if pe_hedge_sym else 0.0
        if pe_hedge_p <= 0:
            if pe_hedge_sym:
                Logger.warning(f"PE hedge premium low/unavailable ({pe_hedge_sym}) - proceeding anyway")
            else:
                Logger.warning("PE hedge symbol not found - entering without PE hedge")
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
            return False
        qty = lots * Config.LOT_SIZE
        # Get margin estimates before placing orders
        _, final_margin_pre = self.exact_margin_for_basket([dict(l, quantity=qty) for l in legs])
        self.state.data["final_margin_used"] = final_margin_pre  # <--- STORED HERE
        capital_before = self.capital_available()
        Logger.info("CAPITAL BEFORE ENTRY", {"available_₹": round(capital_before)})
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
                Logger.warning("Margin read failed during post-entry sampling", {"error": str(e)})
            time.sleep(1.0)
        if margin_samples:
            capital_after = statistics.mean(margin_samples)
            Logger.info(f"Averaged capital after entry (samples={len(margin_samples)})", {"capital_after": round(capital_after)})
        else:
            capital_after = capital_before - 150000
            Logger.warning("No valid margin samples - using fallback")
        time.sleep(5)
        actual_margin_used = self.actual_used_capital()
        self.state.data["exact_margin_used_by_trade"] = actual_margin_used
        margin_per_lot = actual_margin_used / lots if lots > 0 else 0
        Logger.info("MARGIN AFTER ENTRY", {
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
            "realistic_margin": max(final_margin_pre * 0.6, 90000),
            "exact_margin_used_by_trade": actual_margin_used,
            "final_margin_used": final_margin_pre,
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
        self.update_daily_profit_target()
        final_target = self.state.data["profit_target_rupee"]
        Logger.big_banner(f"TRADE ENTERED SUCCESSFULLY | TODAY'S TARGET: +₹{final_target:,} | STOP LOSS: -₹{final_target:,}")
        if vix_val <= Config.VIX_THRESHOLD_FOR_PERCENT_TARGET:
            Logger.info("Target mode: Low VIX - Theta decay divided by days")
        else:
            Logger.info("Target mode: High VIX - 2.0% of final_margin from basket API")
        self.state.save()
        Logger.info("TRADE ENTERED SUCCESSFULLY", {"lots": lots, "symbols": trade_symbols})
        Logger.info("Pausing 2 seconds post-entry for quote stabilization")
        time.sleep(2)
        return True

    def check_existing_positions(self) -> bool:
        try:
            net = self.kite.positions()["net"]
            current_trade_symbols = set(self.state.data.get("trade_symbols", []))
            if self.state.data.get("trade_active"):
                Logger.info("Bot trade already active - skipping entry")
                return True
            if current_trade_symbols:
                existing_symbols = {
                    p["tradingsymbol"]
                    for p in net
                    if p["product"] == "MIS" and p["quantity"] != 0
                }
                if current_trade_symbols.issubset(existing_symbols):
                    Logger.info("Recovering bot trade after restart/crash")
                    self.state.data["trade_active"] = True
                    self.state.save()
                    return True
            Logger.info("Manual MIS trades detected — allowed. Bot will trade independently.")
            return False
        except Exception as e:
            Logger.error("Failed to check existing positions", {"error": str(e)})
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
            Logger.info("DEFENSIVE ADJUSTMENT: CE STRUCK", {"spot": spot, "old_strike": ce_short})
            old_sym = self.find_option_symbol(ce_short, "CE")
            if not old_sym:
                Logger.error("Old CE symbol not found")
                return False
            current_pnl = self.algo_pnl()
            success, _, filled_p = self.order(old_sym, "BUY", qty)
            if not success:
                Logger.error("Failed to close struck CE - aborting adjustment")
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                    leg["exit_price"] = filled_p
                    leg["status"] = "CLOSED"
                    break
            pnl_after_close = self.algo_pnl()
            realized = current_pnl - pnl_after_close
            self.state.data["realized_pnl"] += realized
            Logger.info("REALIZED P&L FROM CE ROLL", {"realized_₹": round(realized, 2)})
            new_ce_short = self.find_short_strike(current_atm, "CE")
            hedge_strike = pos.get("ce_hedge")
            if hedge_strike and abs(new_ce_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                Logger.warning("ADJUSTMENT SKIPPED — NEW SHORT TOO CLOSE TO HEDGE", {
                    "new_short": new_ce_short, "hedge": hedge_strike
                })
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            new_sym = self.find_option_symbol(new_ce_short, "CE")
            if not new_sym or self.ltp(new_sym) <= 0:
                Logger.error("New CE symbol unavailable")
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
                Logger.critical("ROLL FAILED — RESTORING OLD CE STRIKE", {"old_strike": ce_short})
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
            Logger.info("CE LEG ROLLED SUCCESSFULLY", {"new_strike": new_ce_short, "premium": sell_filled_p})
            time.sleep(5)
            actual_margin_used = self.actual_used_capital()
            self.state.data["exact_margin_used_by_trade"] = actual_margin_used
            lots = qty // Config.LOT_SIZE
            if lots > 0:
                self.state.data["margin_per_lot"] = actual_margin_used / lots
        if (spot <= pe_short - Config.ADJUSTMENT_TRIGGER_POINTS and
                self.state.data["adjustments_today"]["pe"] < Config.MAX_ADJUSTMENTS_PER_SIDE_PER_DAY):
            Logger.info("DEFENSIVE ADJUSTMENT: PE STRUCK", {"spot": spot, "old_strike": pe_short})
            old_sym = self.find_option_symbol(pe_short, "PE")
            if not old_sym:
                return False
            current_pnl = self.algo_pnl()
            success, _, filled_p = self.order(old_sym, "BUY", qty)
            if not success:
                Logger.error("Failed to close struck PE")
                return False
            for leg in algo_legs.values():
                if leg["symbol"] == old_sym and leg["status"] == "OPEN":
                    leg["exit_price"] = filled_p
                    leg["status"] = "CLOSED"
                    break
            pnl_after_close = self.algo_pnl()
            realized = current_pnl - pnl_after_close
            self.state.data["realized_pnl"] += realized
            Logger.info("REALIZED P&L FROM PE ROLL", {"realized_₹": round(realized, 2)})
            new_pe_short = self.find_short_strike(current_atm, "PE")
            hedge_strike = pos.get("pe_hedge")
            if hedge_strike and abs(new_pe_short - hedge_strike) < Config.MIN_HEDGE_GAP:
                Logger.warning("ADJUSTMENT SKIPPED — NEW SHORT TOO CLOSE TO HEDGE", {
                    "new_short": new_pe_short, "hedge": hedge_strike
                })
                self.order(old_sym, "SELL", qty)
                self.state.data["realized_pnl"] -= realized
                for leg in algo_legs.values():
                    if leg["symbol"] == old_sym and leg["status"] == "CLOSED":
                        leg["status"] = "OPEN"
                        leg["exit_price"] = 0.0
                        break
                return False
            new_sym = self.find_option_symbol(new_pe_short, "PE")
            if not new_sym or self.ltp(new_sym) <= 0:
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
                Logger.critical("ROLL FAILED — RESTORING OLD PE STRIKE", {"old_strike": pe_short})
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
            Logger.info("PE LEG ROLLED SUCCESSFULLY", {"new_strike": new_pe_short, "premium": sell_filled_p})
            time.sleep(5)
            actual_margin_used = self.actual_used_capital()
            self.state.data["exact_margin_used_by_trade"] = actual_margin_used
            lots = qty // Config.LOT_SIZE
            if lots > 0:
                self.state.data["margin_per_lot"] = actual_margin_used / lots
        if adjusted:
            self.state.data["algo_legs"] = algo_legs
            self.update_daily_profit_target(force=True)
            self.state.save()
        return adjusted

    def check_exit(self) -> Optional[str]:
        now_t = datetime.now(Config.TIMEZONE).time()
        if now_t >= Config.EXIT_TIME:
            return "Scheduled 3:00 PM exit"
        now = datetime.now(Config.TIMEZONE)
        entry_time = self.state.data.get("entry_time")
        time_since_entry = (now - entry_time).total_seconds() if entry_time else float('inf')
        pnl_val = self.algo_pnl()
        target_rupee = self.state.data.get("profit_target_rupee", 0.0)
        if time_since_entry < 300 and abs(pnl_val) > 2500:
            Logger.critical("EXTREME P&L GLITCH DETECTED - IGNORED", {
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
            Logger.warning("State empty — refusing exit to protect account")
            return
        if not self.state.data.get("trade_active", False):
            Logger.info("No active bot trade detected - skipping exit")
            return
        Logger.info(f"EXITING BOT TRADE ONLY: {reason}", {"symbols": trade_syms})
        try:
            net_positions = self.kite.positions()["net"]
            shorts = []
            hedges = []
            pos_qty_map = self.state.data.get("position_qty", {})
            algo_legs = self.state.data.get("algo_legs", {})
            total_bot_qty = sum(abs(q) for q in pos_qty_map.values())
            bot_symbols = set(self.state.data["trade_symbols"])
            account_bot_qty = sum(
                abs(p["quantity"])
                for p in net_positions
                if p["product"] == "MIS" and p["tradingsymbol"] in bot_symbols
            )
            if account_bot_qty != total_bot_qty:
                Logger.critical("Bot quantity mismatch", {"expected": total_bot_qty, "found": account_bot_qty})
                if account_bot_qty == 0 and total_bot_qty > 0:
                    Logger.big_banner("AUTO RECOVERY: MANUAL/MISMATCH CLOSE DETECTED")
                    self.state.full_reset()
                return
            matched_count = 0
            for pos in net_positions:
                sym = pos["tradingsymbol"]
                if (pos["product"] == "MIS" and pos["quantity"] != 0 and sym in trade_syms):
                    expected_qty = pos_qty_map.get(sym)
                    if expected_qty is None:
                        continue
                    if pos["quantity"] != expected_qty:
                        Logger.warning("Quantity mismatch", {"symbol": sym, "expected": expected_qty, "actual": pos["quantity"]})
                        continue
                    matched_count += 1
                    exit_price = self.ltp(sym)
                    for leg in algo_legs.values():
                        if leg["symbol"] == sym and leg["status"] == "OPEN":
                            leg["exit_price"] = exit_price
                            leg["status"] = "CLOSED"
                            break
                    if pos["quantity"] < 0:
                        shorts.append((sym, "BUY", abs(pos["quantity"])))
                    else:
                        hedges.append((sym, "SELL", pos["quantity"]))
            if matched_count == 0:
                Logger.warning("No matching bot positions found")
                return
            Logger.info(f"Closing {len(shorts)} shorts and {len(hedges)} hedges")
            Logger.info("STEP 1: Closing SHORT legs first")
            for sym, side, qty in shorts:
                self.order(sym, side, qty)
            Logger.info("STEP 2: Closing HEDGE legs")
            for sym, side, qty in hedges:
                self.order(sym, side, qty)
            realized = self.state.data.get("realized_pnl", 0.0)
            unrealized = self.algo_pnl()
            total_pnl = realized + unrealized
            Logger.info("FINAL PNL BREAKDOWN", {
                "realized_₹": round(realized, 2),
                "unrealized_₹": round(unrealized, 2),
                "total_pnl_₹": round(total_pnl, 2),
                "reason": reason
            })
            self.state.full_reset()
        except Exception as e:
            Logger.critical("Exit failed", {"error": str(e), "trace": traceback.format_exc()})

# ===================== MAIN APPLICATION =====================
class TradingApplication:
    def __init__(self):
        self.engine = Engine()
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
        self.running = True
        Logger.info("=== v9.8.3 - FINAL_MARGIN TARGET UPDATE BOT STARTED ===")
        last_pnl_print = time.time()
        try:
            while self.running:
                now = datetime.now(Config.TIMEZONE)
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
                            Logger.big_banner("MANUAL CLOSE DETECTED DURING MONITORING - AUTO RECOVERING NOW")
                            self.engine.state.full_reset()
                except Exception as e:
                    Logger.error("Periodic manual close check failed", {"error": str(e)})
                if dtime(9, 20, 50) <= current_time < dtime(9, 22, 0):
                    if self.engine.instruments is None or self.engine.weekly_df is None:
                        Logger.info("PRE-LOADING instruments & weekly data for fast entry")
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()
                if dtime(9, 19) <= current_time < dtime(9, 20) and not self._vix_logged:
                    live_vix = self.engine.vix() or "N/A"
                    allowed = isinstance(live_vix, float) and Config.VIX_MIN <= live_vix <= Config.VIX_MAX
                    Logger.info("9:19 AM Pre-entry VIX check", {"vix": live_vix, "allowed": allowed})
                    self._vix_logged = True
                if dtime(9, 0) <= current_time < dtime(9, 18) and not self._early_confirm_logged:
                    spot = self.engine.spot() or "N/A"
                    vix = self.engine.vix() or "N/A"
                    now_str = now.strftime("%H:%M:%S")
                    Logger.big_banner(f"BOT IS ALIVE & CONNECTED | Time: {now_str} | Spot: {spot} | VIX: {vix}")
                    self._early_confirm_logged = True
                if dtime(9, 18) <= current_time < dtime(9, 25) and not self._snapshot_logged:
                    if self.engine.instruments is None:
                        self.engine.load_instruments()
                        self.engine.load_weekly_df()
                    self.engine.startup_banner()
                    self._snapshot_logged = True
                manual_file = "SHOW_PREVIEW.txt"
                if os.path.exists(manual_file) and not self._manual_preview_triggered:
                    Logger.big_banner("MANUAL PREVIEW TRIGGERED — SHOWING FULL SNAPSHOT NOW")
                    if self.engine.instruments is None:
                        try:
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                        except:
                            Logger.warning("Instruments load failed for manual preview")
                    self.engine.startup_banner()
                    self._manual_preview_triggered = True
                    print(f"\nManual preview displayed. Remove '{manual_file}' to reset trigger.\n")
                if not os.path.exists(manual_file):
                    self._manual_preview_triggered = False
                if (current_time > Config.ENTRY_END
                    and not self.engine.state.data["trade_active"]
                    and self.engine.is_trading_day()[0]
                    and not self._idle_logged_today):
                    Logger.info("ENTRY WINDOW CLOSED - bot idle until next day")
                    self._idle_logged_today = True
                if current_time >= Config.TOKEN_REFRESH_TIME and not self.token_refreshed_today:
                    attempts = 0
                    while attempts < Config.MAX_TOKEN_ATTEMPTS:
                        attempts += 1
                        Logger.info(f"Daily token refresh attempt {attempts}/{Config.MAX_TOKEN_ATTEMPTS}")
                        if generate_and_set_access_token(self.engine.kite):
                            self.engine.load_instruments()
                            self.engine.load_weekly_df()
                            self.token_refreshed_today = True
                            self.engine.startup_banner()
                            Logger.info("Daily access token refreshed successfully")
                            break
                        else:
                            Logger.warning("Token refresh attempt failed, retrying in 60s...")
                            time.sleep(60)
                    else:
                        Logger.critical("Daily token refresh failed after all attempts - exiting bot")
                        sys.exit(1)
                if current_time < Config.TOKEN_REFRESH_TIME:
                    self.token_refreshed_today = False
                    self._vix_logged = False
                    self._snapshot_logged = False
                    self._early_confirm_logged = False
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
                                Logger.info("HOURLY STATUS", {
                                    "unrealized_pnl_₹": round(current_pnl, 2),
                                    "target_₹": round(self.engine.state.data.get("profit_target_rupee", 0)),
                                    "actual_capital_blocked_₹": round(actual_used),
                                    "margin_per_lot_₹": round(self.engine.state.data.get("margin_per_lot", 0))
                                })
                                self._last_hourly_log = time.time()
                            if time.time() - last_pnl_print >= 15:
                                current_pnl = self.engine.algo_pnl()
                                Logger.info("Live unrealized P&L", {"pnl_₹": round(current_pnl, 2)})
                                last_pnl_print = time.time()
                    else:
                        if Config.ENTRY_START <= current_time <= Config.ENTRY_END:
                            self.engine.enter()
                if current_time >= Config.MARKET_CLOSE:
                    if self.engine.state.data["trade_active"]:
                        self.engine.exit("Market close")
                    time.sleep(3600)
                time.sleep(Config.PNL_CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            Logger.info("Bot stopped by user")
            if self.engine.state.data["trade_active"]:
                self.engine.exit("Manual stop")
        except Exception as e:
            Logger.critical("Fatal error in main loop", {"error": str(e), "trace": traceback.format_exc()})

if __name__ == "__main__":
    os.makedirs(os.path.dirname(Config.LOG_FILE) or ".", exist_ok=True)
    if os.path.exists(Config.EMERGENCY_STOP_FILE):
        print(f"EMERGENCY STOP ACTIVE: Remove '{Config.EMERGENCY_STOP_FILE}' to run")
        sys.exit(1)
    TradingApplication().run()