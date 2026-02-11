# trading/tasks.py

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded, Ignore
from django.contrib.auth.models import User
from django.utils import timezone
import time
import traceback
import logging
import signal
import sys
from datetime import timedelta

from .models import Broker, BotStatus, LogEntry
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
# MAIN LONG-RUNNING BOT TASK
# ==============================================================================

@shared_task(bind=True, acks_late=True, time_limit=None, soft_time_limit=None)
def run_user_bot(self, user_id):
    """
    Long-running Celery task that runs a trading bot for one user.

    Features:
    - Proper Celery revocation detection
    - Heartbeat monitoring (saved to DB)
    - Graceful shutdown on SIGTERM/SIGINT/revoke
    - DB-controlled stop (BotStatus.is_running)
    - Crash-safe cleanup
    """
    task_id = self.request.id
    logger.info(f"[TASK START] run_user_bot user_id={user_id} task_id={task_id}")
    print(f"[TASK START] run_user_bot launched for user_id={user_id} at {timezone.now()} task_id={task_id}")

    runner = BotRunner()
    bot_status = None

    try:
        # ------------------------------------------------------------------
        # Load user
        # ------------------------------------------------------------------
        user = User.objects.get(id=user_id)
        logger.info(f"[{task_id}] User loaded: {user.username} ({user.id})")

        # ------------------------------------------------------------------
        # Initialize / update BotStatus
        # ------------------------------------------------------------------
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
        logger.info(f"[{task_id}] BotStatus {'created' if created else 'updated'}")

        # ------------------------------------------------------------------
        # Load broker
        # ------------------------------------------------------------------
        broker = Broker.objects.filter(
            user=user,
            broker_name="ZERODHA",
            is_active=True
        ).first()

        if not broker:
            raise ValueError("No active Zerodha broker found")

        if not broker.access_token:
            raise ValueError("Missing broker access token")

        logger.info(f"[{task_id}] Broker loaded successfully")

        # ------------------------------------------------------------------
        # Initialize trading engine
        # ------------------------------------------------------------------
        app = TradingApplication(user=user, broker=broker)
        logger.info(f"[{task_id}] TradingApplication initialized")

        # ------------------------------------------------------------------
        # Initial heartbeat
        # ------------------------------------------------------------------
        bot_status.last_heartbeat = timezone.now()
        bot_status.current_unrealized_pnl = 0
        bot_status.current_margin = 0
        bot_status.save(update_fields=[
            'last_heartbeat',
            'current_unrealized_pnl',
            'current_margin'
        ])
        logger.info(f"[{task_id}] Initial heartbeat saved")

        # ------------------------------------------------------------------
        # Main loop - FIXED: one cycle per iteration + sleep
        # ------------------------------------------------------------------
        HEARTBEAT_INTERVAL = 10   # more responsive than 30s
        last_heartbeat = time.time()

        while runner.running:
            # --------------------------------------------------------------
            # Check for Celery revocation (Celery 5.x safe way)
            # --------------------------------------------------------------
            if getattr(self.request, 'revoked', False):
                logger.warning(f"[{task_id}] Task revoked externally — shutting down")
                runner.stop()
                raise Ignore()

            # --------------------------------------------------------------
            # Check DB stop flag (admin or manual stop)
            # --------------------------------------------------------------
            bot_status.refresh_from_db()
            if not bot_status.is_running:
                logger.info(f"[{task_id}] BotStatus.is_running=False — stopping gracefully")
                runner.stop()
                break

            try:
                # ----------------------------------------------------------
                # Run ONE full bot cycle
                # ----------------------------------------------------------
                app.run()

                # ----------------------------------------------------------
                # Heartbeat update
                # ----------------------------------------------------------
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
                        logger.debug(f"[{task_id}] Heartbeat updated")
                    except Exception as hb_err:
                        logger.warning(f"[{task_id}] Heartbeat save failed: {hb_err}")

            except KeyboardInterrupt:
                logger.info(f"[{task_id}] KeyboardInterrupt — stopping")
                runner.stop()

            except SoftTimeLimitExceeded:
                logger.warning(f"[{task_id}] Soft time limit exceeded — stopping")
                break

            except Exception as loop_err:
                error_msg = f"Error in bot loop: {str(loop_err)}\n{traceback.format_exc()}"
                logger.error(f"[{task_id}] {error_msg}")
                if bot_status:
                    bot_status.last_error = str(loop_err)[:500]
                    bot_status.save(update_fields=['last_error'])
                time.sleep(10)  # backoff

            time.sleep(1)  # ← CRITICAL: yield control to Celery

        logger.info(f"[{task_id}] Main loop exited cleanly")

    except Exception as fatal_err:
        error_msg = f"Fatal error: {str(fatal_err)}\n{traceback.format_exc()}"
        logger.critical(f"[{task_id}] {error_msg}")
        print(f"[FATAL] {error_msg}")

        if bot_status:
            bot_status.last_error = str(fatal_err)[:500]
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save()

    finally:
        # ------------------------------------------------------------------
        # FINAL CLEANUP (always runs)
        # ------------------------------------------------------------------
        if bot_status:
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save(update_fields=['is_running', 'last_stopped'])

        logger.info(f"[{task_id}] Task cleanup complete — bot marked stopped")


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