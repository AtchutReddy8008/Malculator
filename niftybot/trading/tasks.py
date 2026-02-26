import os
import sys
import signal
import time
import traceback
import logging
from datetime import timedelta, date

from celery import shared_task, current_app as celery_app
from celery.exceptions import SoftTimeLimitExceeded, Ignore
from django.contrib.auth.models import User
from django.utils import timezone
from django.db import transaction, close_old_connections
from decimal import Decimal

from .models import Broker, BotStatus, LogEntry, Trade, DailyPnL
from .core.bot_original import TradingApplication
from .core.auth import generate_and_set_access_token_db
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)

# ==============================================================================
# SIGNAL HANDLING (REGISTERED ONCE PER WORKER)
# ==============================================================================

_signals_registered = False


def register_shutdown_signals():
    global _signals_registered
    if _signals_registered:
        return

    def graceful_shutdown(sig, frame):
        logger.warning(f"[SIGNAL] Received signal {sig} — exiting worker gracefully")
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    _signals_registered = True


register_shutdown_signals()


# ==============================================================================
# MAIN LONG-RUNNING BOT TASK — ONE TASK PER USER
# ==============================================================================

@shared_task(bind=True, acks_late=True, time_limit=None, soft_time_limit=None)
def run_user_bot(self, user_id):
    """
    Long-running Celery task that runs a trading bot for ONE specific user.

    FIX: The old code called app.run() inside a while loop. But app.run()
    contains its OWN infinite while loop — so the outer loop in tasks.py never
    iterated, meaning the tasks.py heartbeat never fired. Then check_bot_health()
    saw no heartbeat for 5+ minutes, set is_running=False, and bot_original.py's
    _check_is_running() stopped the bot.

    The correct architecture is:
      - bot_original.py owns the main loop (app.run())
      - tasks.py just bootstraps, calls app.run() ONCE, and cleans up after
      - heartbeat is handled INSIDE bot_original.py's run() loop (already there)
      - check_bot_health threshold raised to 15 min to survive restarts safely
    """
    task_id = self.request.id

    try:
        user = User.objects.get(id=user_id)
        user_prefix = f"[user:{user.username}] "
    except User.DoesNotExist:
        logger.error(f"[TASK START] User id={user_id} not found — task aborted")
        return

    logger.info(f"{user_prefix}[TASK START] run_user_bot task_id={task_id}")
    print(f"{user_prefix}[TASK START] launched at {timezone.now()} task_id={task_id}", flush=True)

    bot_status = None
    app = None

    try:
        # ── 1. Mark bot as running in DB ──────────────────────────────────────
        bot_status, created = BotStatus.objects.get_or_create(user=user)
        bot_status.celery_task_id = task_id
        bot_status.is_running = True
        bot_status.last_started = timezone.now()
        bot_status.last_heartbeat = timezone.now()   # FIX: set immediately so health check
        bot_status.last_error = None                 #      doesn't kill us before app.run() starts
        bot_status.current_unrealized_pnl = Decimal('0.00')
        bot_status.current_margin = Decimal('0.00')
        bot_status.save(update_fields=[
            'celery_task_id',
            'is_running',
            'last_started',
            'last_heartbeat',
            'last_error',
            'current_unrealized_pnl',
            'current_margin',
        ])
        logger.info(f"{user_prefix}[{task_id}] BotStatus {'created' if created else 'updated'} — is_running=True")

        # ── 2. Load broker ────────────────────────────────────────────────────
        broker = Broker.objects.filter(
            user=user,
            broker_name="ZERODHA",
            is_active=True
        ).first()

        if not broker:
            raise ValueError("No active Zerodha broker found")
        if not broker.access_token:
            raise ValueError("Missing broker access token")

        logger.info(f"{user_prefix}[{task_id}] Broker loaded successfully")

        # ── 3. Initialize trading engine ──────────────────────────────────────
        app = TradingApplication(user=user, broker=broker)
        logger.info(f"{user_prefix}[{task_id}] TradingApplication initialized — handing control to app.run()")

        # ── 4. Run the bot ────────────────────────────────────────────────────
        #
        # FIX: Call app.run() exactly ONCE. It contains its own while loop that:
        #   - checks is_running from DB every iteration
        #   - writes heartbeat + unrealized PnL every 5 seconds
        #   - handles entry, exit, adjustments, VIX checks, etc.
        #
        # There must be NO outer while loop here. Adding one caused the old bug
        # where tasks.py's heartbeat was unreachable and check_bot_health() killed
        # the bot after 5 minutes of perceived silence.
        #
        app.run()

        logger.info(f"{user_prefix}[{task_id}] app.run() returned cleanly")

    except ValueError as cfg_err:
        # Config errors (missing broker, token) — log clearly, don't retry
        logger.error(f"{user_prefix}[{task_id}] Config error: {cfg_err}")
        if bot_status:
            bot_status.last_error = str(cfg_err)[:500]
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save(update_fields=['last_error', 'is_running', 'last_stopped'])

    except SoftTimeLimitExceeded:
        # Should never fire (soft_time_limit=None), but handled for safety
        logger.warning(f"{user_prefix}[{task_id}] SoftTimeLimitExceeded — stopping")
        if app and app.engine.state.data.get("trade_active"):
            try:
                app.engine.exit("Celery soft time limit")
            except Exception:
                pass

    except Exception as fatal_err:
        error_msg = f"Fatal error: {str(fatal_err)}\n{traceback.format_exc()}"
        logger.critical(f"{user_prefix}[{task_id}] {error_msg}")
        if bot_status:
            bot_status.last_error = str(fatal_err)[:500]
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save(update_fields=['last_error', 'is_running', 'last_stopped'])

    finally:
        # Always mark bot as stopped when this task ends for any reason
        if bot_status:
            try:
                bot_status.refresh_from_db()
                bot_status.is_running = False
                bot_status.last_stopped = timezone.now()
                bot_status.save(update_fields=['is_running', 'last_stopped'])
                logger.info(f"{user_prefix}[{task_id}] Final cleanup: is_running=False saved")
            except Exception as cleanup_err:
                logger.error(f"{user_prefix}[{task_id}] Final cleanup DB save failed: {cleanup_err}")

        # Safety net: exit any lingering trade position
        if app and app.engine.state.data.get("trade_active", False):
            try:
                logger.info(f"{user_prefix}[{task_id}] Final cleanup: forcing engine exit")
                app.engine.exit("Task shutdown / Celery final cleanup")
            except Exception as exit_err:
                logger.critical(f"{user_prefix}[{task_id}] Final engine exit failed: {exit_err}")

        logger.info(f"{user_prefix}[{task_id}] Task complete — bot marked stopped")


# ==============================================================================
# PERIODIC HEALTH CHECK TASK (CELERY BEAT)
# ==============================================================================

@shared_task
def check_bot_health():
    """
    Detects and cleans up stale bot records.

    FIX: Threshold raised from 5 min → 15 min.
    bot_original.py writes a heartbeat every 5 seconds, but during startup
    (instrument loading, token auth, banner) it can take 2-4 minutes before
    the first heartbeat lands. 5 minutes was too tight and caused false kills.
    """
    now = timezone.now()
    STALE_THRESHOLD_MINUTES = 15   # FIX: was 5, raised to 15
    logger.info(f"[HEALTH CHECK] Running — stale threshold: {STALE_THRESHOLD_MINUTES} min")

    running_bots = BotStatus.objects.filter(is_running=True)

    if not running_bots.exists():
        logger.info("[HEALTH CHECK] No bots currently marked as running")
        return

    logger.info(f"[HEALTH CHECK] Checking {running_bots.count()} running bots")

    for status in running_bots:
        issues = []

        if status.last_heartbeat:
            age = now - status.last_heartbeat
            if age > timedelta(minutes=STALE_THRESHOLD_MINUTES):
                issues.append(f"No heartbeat for {age.total_seconds()/60:.1f} min")
        elif status.last_started and (now - status.last_started) > timedelta(minutes=30):
            issues.append("Long-running without any heartbeat ever")

        if issues:
            msg = "; ".join(issues)
            logger.warning(f"[HEALTH CHECK] Stale bot: {status.user.username} — {msg}")
            status.is_running = False
            status.last_error = f"Auto-stopped: {msg}"
            status.save(update_fields=['is_running', 'last_error'])
        else:
            logger.debug(f"[HEALTH CHECK] Bot {status.user.username} is healthy")

    logger.info("[HEALTH CHECK] Completed")


# ==============================================================================
# BACKGROUND ZERODHA TOKEN GENERATION TASK
# ==============================================================================

@shared_task
def generate_zerodha_token_task(broker_id):
    try:
        broker = Broker.objects.select_related('user').get(id=broker_id)
        kite = KiteConnect(api_key=broker.api_key)
        success = generate_and_set_access_token_db(kite=kite, broker=broker)

        level = 'INFO' if success else 'ERROR'
        msg = "Background Zerodha token generation " + ("succeeded" if success else "failed")
        LogEntry.objects.create(
            user=broker.user,
            level=level,
            message=msg,
            details={'broker_id': broker_id, 'user_id': broker.user.id}
        )
        logger.info(f"Token generation task {'succeeded' if success else 'failed'} for broker {broker_id}")

    except Broker.DoesNotExist:
        logger.error(f"Broker {broker_id} not found for token generation task")
    except Exception as e:
        logger.exception(f"Token generation task failed: {str(e)}")


# ==============================================================================
# AUTO-START BOTS AT MARKET OPEN (09:00 IST)
# ==============================================================================

@shared_task
def auto_start_user_bots():
    logger.info("[AUTO-START] Checking users for morning bot auto-start")
    users = User.objects.filter(is_active=True)
    started_count = 0

    for user in users:
        try:
            broker = Broker.objects.filter(
                user=user,
                broker_name='ZERODHA',
                is_active=True,
                access_token__isnull=False
            ).first()

            if not broker:
                continue

            bot_status, _ = BotStatus.objects.get_or_create(user=user)
            if bot_status.is_running:
                continue

            result = run_user_bot.delay(user.id)

            with transaction.atomic():
                bot_status.refresh_from_db()
                bot_status.is_running = True
                bot_status.celery_task_id = result.id
                bot_status.last_started = timezone.now()
                bot_status.last_error = None
                bot_status.save(update_fields=[
                    'is_running', 'celery_task_id', 'last_started', 'last_error'
                ])

            LogEntry.objects.create(
                user=user,
                level='INFO',
                message='Bot auto-started at market open (09:00 IST)',
                details={'task_id': result.id, 'auto': True}
            )
            logger.info(f"[AUTO-START] Bot launched for {user.username} - task_id={result.id}")
            started_count += 1

        except Exception as e:
            logger.error(f"[AUTO-START] Failed for {user.username}: {str(e)}")

    logger.info(f"[AUTO-START] Completed — started {started_count} bots")


# ==============================================================================
# DAILY PnL SAVE FOR ALL USERS (15:45 IST)
# ==============================================================================

@shared_task
def save_daily_pnl_all_users():
    today = date.today()
    logger.info(f"[DAILY PnL SAVE] Running for date: {today}")
    users = User.objects.filter(is_active=True)
    saved_count = skipped_count = 0

    for user in users:
        try:
            broker = Broker.objects.filter(
                user=user, broker_name='ZERODHA', is_active=True
            ).first()

            if not broker:
                skipped_count += 1
                continue

            bot_status = BotStatus.objects.filter(user=user).first()
            pnl = Decimal('0.00')

            if bot_status and bot_status.is_running:
                pnl = bot_status.current_unrealized_pnl or Decimal('0.00')

            trades_today = Trade.objects.filter(
                user=user, entry_time__date=today, status='EXECUTED'
            )
            total_trades = trades_today.count()
            win_trades = trades_today.filter(pnl__gt=0).count()
            loss_trades = trades_today.filter(pnl__lt=0).count()

            DailyPnL.objects.update_or_create(
                user=user,
                date=today,
                defaults={
                    'pnl': pnl,
                    'total_trades': total_trades,
                    'win_trades': win_trades,
                    'loss_trades': loss_trades,
                }
            )
            saved_count += 1

        except Exception as e:
            logger.error(f"[DAILY PnL] Failed for {user.username}: {str(e)}")
            skipped_count += 1

    logger.info(f"[DAILY PnL SAVE] Completed — saved: {saved_count}, skipped: {skipped_count}")


# ==============================================================================
# WEEKLY LOG CLEANUP (EVERY SUNDAY 2:30 AM IST)
# ==============================================================================

@shared_task(name='trading.cleanup_old_logs')
def cleanup_old_logs(days=30):
    cutoff = timezone.now() - timedelta(days=days)
    deleted_count = LogEntry.objects.filter(
        level__in=['INFO', 'WARNING'],
        timestamp__lt=cutoff
    ).delete()[0]
    logger.info(f"[WEEKLY CLEANUP] Deleted {deleted_count} old logs older than {days} days")
    return f"Deleted {deleted_count} old INFO/WARNING logs."