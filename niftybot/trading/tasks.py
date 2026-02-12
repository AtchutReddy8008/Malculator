# trading/tasks.py

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
from django.db import transaction

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
    """
    Register shutdown handlers once per Celery worker process.
    Safe even with gevent/prefork + --noreload.
    """
    global _signals_registered
    if _signals_registered:
        return

    def graceful_shutdown(sig, frame):
        logger.warning(f"[SIGNAL] Received signal {sig} — exiting worker gracefully")
        sys.exit(0)

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    _signals_registered = True


# Register signals at module import (once per worker)
register_shutdown_signals()


# ==============================================================================
# HELPER CONTROL CLASS
# ==============================================================================

class BotRunner:
    """Simple per-task control object for graceful shutdown"""

    def __init__(self):
        self.running = True

    def stop(self):
        self.running = False


# ==============================================================================
# MAIN LONG-RUNNING BOT TASK — ONE TASK PER USER
# ==============================================================================

@shared_task(bind=True, acks_late=True, time_limit=None, soft_time_limit=None)
def run_user_bot(self, user_id):
    """
    Long-running Celery task that runs a trading bot for ONE specific user.
    Supports multiple users simultaneously — each user gets their own independent task.
    """
    task_id = self.request.id

    try:
        user = User.objects.get(id=user_id)
        user_prefix = f"[user:{user.username}] "
    except User.DoesNotExist:
        logger.error(f"[TASK START] User id={user_id} not found — task aborted")
        return

    logger.info(f"{user_prefix}[TASK START] run_user_bot task_id={task_id}")
    print(f"{user_prefix}[TASK START] launched at {timezone.now()} task_id={task_id}")

    runner = BotRunner()
    bot_status = None

    try:
        # Initialize / update BotStatus
        bot_status, created = BotStatus.objects.get_or_create(user=user)
        bot_status.celery_task_id = task_id
        bot_status.is_running = True
        bot_status.last_started = timezone.now()
        bot_status.last_error = None
        bot_status.save(update_fields=[
            'celery_task_id',
            'is_running',
            'last_started',
            'last_error'
        ])
        logger.info(f"{user_prefix}[{task_id}] BotStatus {'created' if created else 'updated'}")

        # Load broker
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

        # Initialize trading engine (user-specific)
        app = TradingApplication(user=user, broker=broker)
        logger.info(f"{user_prefix}[{task_id}] TradingApplication initialized")

        # Initial heartbeat
        bot_status.last_heartbeat = timezone.now()
        bot_status.current_unrealized_pnl = 0
        bot_status.current_margin = 0
        bot_status.save(update_fields=[
            'last_heartbeat',
            'current_unrealized_pnl',
            'current_margin'
        ])
        logger.info(f"{user_prefix}[{task_id}] Initial heartbeat saved")

        # Main loop
        HEARTBEAT_INTERVAL = 2   # seconds — more responsive for dashboard
        last_heartbeat = time.time()

        while runner.running:
            # Celery revocation check
            if getattr(self.request, 'revoked', False):
                logger.warning(f"{user_prefix}[{task_id}] Task revoked externally — shutting down")
                runner.stop()
                raise Ignore()

            # DB stop flag check
            bot_status.refresh_from_db()
            if not bot_status.is_running:
                logger.info(f"{user_prefix}[{task_id}] BotStatus.is_running=False — stopping gracefully")
                runner.stop()
                break

            try:
                # Run ONE full bot cycle
                app.run()

                # Heartbeat update
                if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                    try:
                        bot_status.last_heartbeat = timezone.now()
                        bot_status.current_unrealized_pnl = float(app.engine.algo_pnl() or 0)
                        bot_status.current_margin = float(app.engine.actual_used_capital() or 0)
                        bot_status.save(update_fields=[
                            'last_heartbeat',
                            'current_unrealized_pnl',
                            'current_margin'
                        ])
                        last_heartbeat = time.time()
                        logger.debug(f"{user_prefix}[{task_id}] Heartbeat updated")
                    except Exception as hb_err:
                        logger.warning(f"{user_prefix}[{task_id}] Heartbeat save failed: {hb_err}")

            except KeyboardInterrupt:
                logger.info(f"{user_prefix}[{task_id}] KeyboardInterrupt — stopping")
                runner.stop()

            except SoftTimeLimitExceeded:
                logger.warning(f"{user_prefix}[{task_id}] Soft time limit exceeded — stopping")
                break

            except Exception as loop_err:
                error_msg = f"Error in bot loop: {str(loop_err)}\n{traceback.format_exc()}"
                logger.error(f"{user_prefix}[{task_id}] {error_msg}")
                if bot_status:
                    bot_status.last_error = str(loop_err)[:500]
                    bot_status.save(update_fields=['last_error'])
                time.sleep(10)  # backoff

            time.sleep(1)  # Yield control to Celery

        logger.info(f"{user_prefix}[{task_id}] Main loop exited cleanly")

    except User.DoesNotExist:
        logger.error(f"[TASK START] User id={user_id} not found — task aborted")
    except Exception as fatal_err:
        error_msg = f"Fatal error: {str(fatal_err)}\n{traceback.format_exc()}"
        logger.critical(f"{user_prefix}[{task_id}] {error_msg}")

        if bot_status:
            bot_status.last_error = str(fatal_err)[:500]
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save()

    finally:
        # FINAL CLEANUP (always runs)
        if bot_status:
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save(update_fields=['is_running', 'last_stopped'])

        logger.info(f"{user_prefix}[{task_id}] Task cleanup complete — bot marked stopped")


# ==============================================================================
# PERIODIC HEALTH CHECK TASK (CELERY BEAT)
# ==============================================================================

@shared_task
def check_bot_health():
    """
    Periodic Celery Beat task to detect and clean up stale bot records.
    Runs every ~5 minutes (configure in celery beat schedule).
    """
    now = timezone.now()
    logger.info("[HEALTH CHECK] Starting bot health check")

    running_bots = BotStatus.objects.filter(is_running=True)

    if not running_bots.exists():
        logger.info("[HEALTH CHECK] No bots currently marked as running")
        return

    logger.info(f"[HEALTH CHECK] Checking {running_bots.count()} running bots")

    for status in running_bots:
        issues = []

        # No heartbeat for >5 min → stale
        if status.last_heartbeat:
            age = now - status.last_heartbeat
            if age > timedelta(minutes=5):
                issues.append(f"No heartbeat for {age.total_seconds()/60:.1f} min")
        # Started long ago but no heartbeat
        elif status.last_started and (now - status.last_started) > timedelta(minutes=30):
            issues.append("Long-running without heartbeat")

        if issues:
            msg = "; ".join(issues)
            logger.warning(f"[HEALTH CHECK] Stale bot detected: {status.user.username} - {msg}")
            status.is_running = False
            status.last_error = f"Stale bot - {msg}"
            status.save(update_fields=['is_running', 'last_error'])
        else:
            logger.debug(f"[HEALTH CHECK] Bot {status.user.username} looks healthy")

    logger.info("[HEALTH CHECK] Completed")


# ==============================================================================
# BACKGROUND ZERODHA TOKEN GENERATION TASK
# ==============================================================================

@shared_task
def generate_zerodha_token_task(broker_id):
    """
    Background task to generate and save Zerodha access token
    after user submits credentials.
    """
    try:
        broker = Broker.objects.select_related('user').get(id=broker_id)
        kite = KiteConnect(api_key=broker.api_key)

        success = generate_and_set_access_token_db(kite=kite, broker=broker)

        if success:
            LogEntry.objects.create(
                user=broker.user,
                level='INFO',
                message="Background Zerodha token generation succeeded",
                details={'broker_id': broker_id, 'user_id': broker.user.id}
            )
            logger.info(f"Token generation task succeeded for broker {broker_id}")
        else:
            LogEntry.objects.create(
                user=broker.user,
                level='ERROR',
                message="Background Zerodha token generation failed",
                details={'broker_id': broker_id, 'user_id': broker.user.id}
            )
            logger.error(f"Token generation task failed for broker {broker_id}")

    except Broker.DoesNotExist:
        logger.error(f"Broker {broker_id} not found for token generation task")
    except Exception as e:
        logger.exception(f"Token generation task failed: {str(e)}")


# ==============================================================================
# AUTO-START BOTS AT MARKET OPEN (09:00 IST)
# ==============================================================================

@shared_task
def auto_start_user_bots():
    """
    Celery Beat task — runs daily at 09:00 IST
    Automatically starts bots for users who have active Zerodha connection
    but bot is not currently running.
    """
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
                continue  # already running → skip

            # Launch the bot task
            result = run_user_bot.delay(user.id)

            with transaction.atomic():
                bot_status.refresh_from_db()
                bot_status.is_running = True
                bot_status.celery_task_id = result.id
                bot_status.last_started = timezone.now()
                bot_status.last_error = None
                bot_status.save(update_fields=[
                    'is_running',
                    'celery_task_id',
                    'last_started',
                    'last_error'
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
# Ensures calendar always has data — even no trade / bot not running
# ==============================================================================

@shared_task
def save_daily_pnl_all_users():
    """
    Celery Beat task — runs daily at ~15:45 IST
    Creates/updates DailyPnL record for every active user with broker,
    ensuring calendar view always has data (even PnL=0 days).
    """
    today = date.today()
    logger.info(f"[DAILY PnL SAVE] Running for date: {today}")

    users = User.objects.filter(is_active=True)
    saved_count = 0
    skipped_count = 0

    for user in users:
        try:
            broker = Broker.objects.filter(
                user=user,
                broker_name='ZERODHA',
                is_active=True
            ).first()

            if not broker:
                skipped_count += 1
                continue  # no active broker → skip

            bot_status = BotStatus.objects.filter(user=user).first()

            pnl = 0.0
            total_trades = 0
            win_trades = 0
            loss_trades = 0

            # Prefer live bot data if running
            if bot_status and bot_status.is_running:
                pnl = float(bot_status.current_unrealized_pnl or 0)

            # Count executed trades today (more reliable than bot state)
            trades_today = Trade.objects.filter(
                user=user,
                entry_time__date=today,
                status='EXECUTED'
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

            logger.debug(f"[DAILY PnL] Saved for {user.username}: ₹{pnl:.2f}, trades={total_trades}")
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
    """
    Weekly cleanup task (runs every Sunday at 2:30 AM IST)
    Deletes INFO and WARNING logs older than `days` days.
    Keeps ERROR and CRITICAL logs forever.
    """
    cutoff = timezone.now() - timedelta(days=days)

    deleted_count = LogEntry.objects.filter(
        level__in=['INFO', 'WARNING'],
        timestamp__lt=cutoff
    ).delete()[0]   # returns (count, dict of deleted objects)

    logger.info(f"[WEEKLY CLEANUP] Deleted {deleted_count} old INFO/WARNING logs older than {days} days")
    return f"Deleted {deleted_count} old INFO/WARNING logs older than {days} days."