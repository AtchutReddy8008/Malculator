from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from django.contrib.auth.models import User
from django.utils import timezone
import time
import traceback
import logging
import signal
import sys
from datetime import datetime  # ← FIXED: added this import (was missing, caused name error)

from .models import Broker, BotStatus
from .core.bot_original import TradingApplication  # ← make sure this import path is correct
from .core.auth import generate_and_set_access_token_db

logger = logging.getLogger(__name__)


# Per-task running flag (not global — each task has its own)
class BotRunner:
    def __init__(self):
        self.running = True

    def stop(self):
        self.running = False


def signal_handler(sig, frame):
    """Graceful shutdown handler for the current task"""
    logger.info("Shutdown signal received - stopping bot gracefully")
    # We can't access task instance here directly, but we can rely on finally block
    sys.exit(0)


# Register signals (will apply to worker process)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


@shared_task(bind=True, time_limit=None)  # No time limit - bot runs until stopped
def run_user_bot(self, user_id):
    """
    Main Celery task to run trading bot for a specific user.
    Uses graceful shutdown via signals + per-task flag.
    """
    print(f"[FORCE DEBUG] run_user_bot STARTED for user_id={user_id} at {timezone.now()} (task_id={self.request.id})")
    logger.info(f"Starting bot task for user_id: {user_id} (task_id: {self.request.id})")

    runner = BotRunner()  # Per-task control

    bot_status = None

    try:
        print("[FORCE DEBUG] Step 1: Loading user object")
        user = User.objects.get(id=user_id)
        print(f"[FORCE DEBUG] User loaded: {user.username}")

        print("[FORCE DEBUG] Step 2: Getting/Creating BotStatus")
        bot_status, _ = BotStatus.objects.get_or_create(user=user)
        bot_status.celery_task_id = self.request.id
        bot_status.is_running = True
        bot_status.last_started = timezone.now()
        bot_status.last_error = None
        bot_status.save(update_fields=['celery_task_id', 'is_running', 'last_started', 'last_error'])
        print("[FORCE DEBUG] BotStatus saved successfully")

        print("[FORCE DEBUG] Step 3: Loading Broker profile")
        broker = Broker.objects.filter(user=user, broker_name='ZERODHA', is_active=True).first()
        if not broker:
            error_msg = "No active broker connection found"
            print(f"[FORCE DEBUG] {error_msg}")
            logger.error(f"{error_msg} for {user.username}")
            bot_status.last_error = error_msg
            bot_status.is_running = False
            bot_status.save(update_fields=['last_error', 'is_running'])
            return f"User {user.username}: No broker connection"

        print("[FORCE DEBUG] Broker found - proceeding to initialize TradingApplication")
        logger.info(f"Found broker for {user.username}")

        # Initialize trading application
        print("[FORCE DEBUG] Step 4: Creating TradingApplication instance")
        app = TradingApplication(user=user, broker=broker)  # ← this line often crashes
        print("[FORCE DEBUG] TradingApplication created successfully")

        # Start the bot loop
        app.running = True

        # Heartbeat update frequency (seconds)
        HEARTBEAT_INTERVAL = 60
        last_heartbeat = time.time()

        print("[FORCE DEBUG] Step 5: Entering main bot loop")
        logger.info(f"Starting bot loop for {user.username}")
        while runner.running and app.running:
            try:
                print("[FORCE DEBUG] Loop tick - calling app.run() single step")
                # Run one iteration of bot logic
                now = datetime.now(Config.TIMEZONE)
                current_time = now.time()
                today_date = now.date()
                app.engine.state.daily_reset()

                # Pre-load data if needed
                if dtime(9, 20, 50) <= current_time < dtime(9, 22, 0):
                    if app.engine.instruments is None or app.engine.weekly_df is None:
                        print("[FORCE DEBUG] Pre-loading instruments & weekly data")
                        app.engine.load_instruments()
                        app.engine.load_weekly_df()

                # Main logic (single cycle)
                if Config.ENTRY_START <= current_time <= Config.ENTRY_END:
                    print("[FORCE DEBUG] ENTRY WINDOW OPEN - attempting entry")
                    app.engine.enter()

                # Heartbeat
                if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                    bot_status.last_heartbeat = timezone.now()
                    bot_status.current_unrealized_pnl = app.engine.algo_pnl() or 0
                    bot_status.current_margin = app.engine.actual_used_capital() or 0
                    bot_status.save(update_fields=['last_heartbeat', 'current_unrealized_pnl', 'current_margin'])
                    last_heartbeat = time.time()
                    print("[FORCE DEBUG] Heartbeat saved")

                time.sleep(3)  # Match your PNL_CHECK_INTERVAL_SECONDS

            except Exception as inner_e:
                print(f"[FORCE DEBUG] Exception inside loop: {str(inner_e)}")
                logger.error(f"Error in bot loop for {user.username}: {str(inner_e)}")
                bot_status.last_error = str(inner_e)[:500]
                bot_status.save(update_fields=['last_error'])

        # Graceful exit
        print("[FORCE DEBUG] Bot loop exited")
        logger.info(f"Bot loop stopped for {user.username}")
        # app.exit("Task shutdown")  # call if you have exit method

    except SoftTimeLimitExceeded:
        print("[FORCE DEBUG] SoftTimeLimitExceeded caught")
        logger.warning(f"Task time limit exceeded for user_id: {user_id}")
        if bot_status:
            bot_status.last_error = "Task time limit exceeded"
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save()
        return f"User {user.username}: Time limit exceeded"

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[FORCE CRASH] FATAL EXCEPTION in run_user_bot: {error_msg}")
        logger.error(f"Fatal error in run_user_bot for user_id {user_id}: {error_msg}")
        if bot_status:
            bot_status.last_error = error_msg[:500]
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save()
        return f"User {user.username}: Error - {str(e)}"

    finally:
        # Ensure cleanup
        print("[FORCE DEBUG] Finally block - cleaning up BotStatus")
        if bot_status:
            bot_status.is_running = False
            bot_status.last_stopped = timezone.now()
            bot_status.save()
        logger.info(f"Bot task ended for user_id {user_id}")


@shared_task
def check_bot_health():
    """Periodic task (Celery Beat) to detect and clean up stale/dead bots"""
    print("[FORCE DEBUG] check_bot_health task running")
    running_bots = BotStatus.objects.filter(is_running=True)
    logger.info(f"Health check: {running_bots.count()} bots marked as running")

    now = timezone.now()
    for status in running_bots:
        if status.last_heartbeat:
            age = now - status.last_heartbeat
            if age > timedelta(minutes=10):
                logger.warning(f"Detected stale bot for {status.user.username} - last heartbeat {age}")
                status.is_running = False
                status.last_error = f"Stale - no heartbeat for {age}"
                status.save(update_fields=['is_running', 'last_error'])
        elif status.last_started and (now - status.last_started) > timedelta(hours=1):
            logger.warning(f"Old bot without heartbeat for {status.user.username} - marking stopped")
            status.is_running = False
            status.last_error = "No heartbeat support - assumed stale"
            status.save(update_fields=['is_running', 'last_error'])

# from celery import shared_task
# from celery.exceptions import SoftTimeLimitExceeded
# from django.contrib.auth.models import User
# from django.utils import timezone
# import time
# import traceback
# import logging
# import signal
# import sys

# from .models import Broker, BotStatus
# from .core.bot_original import TradingApplication  # ← make sure this import path is correct
# from .core.auth import generate_and_set_access_token_db

# logger = logging.getLogger(__name__)


# # Per-task running flag (not global — each task has its own)
# class BotRunner:
#     def __init__(self):
#         self.running = True

#     def stop(self):
#         self.running = False


# def signal_handler(sig, frame):
#     """Graceful shutdown handler for the current task"""
#     logger.info("Shutdown signal received - stopping bot gracefully")
#     sys.exit(0)


# # Register signals (will apply to worker process)
# signal.signal(signal.SIGTERM, signal_handler)
# signal.signal(signal.SIGINT, signal_handler)


# @shared_task(bind=True, time_limit=None)  # No time limit - bot runs until stopped
# def run_user_bot(self, user_id):
#     """
#     Main Celery task to run trading bot for a specific user.
#     Uses graceful shutdown via signals + per-task flag.
#     """
#     print(f"[FORCE DEBUG] run_user_bot STARTED for user_id={user_id} at {timezone.now()} (task_id={self.request.id})")
#     logger.info(f"Starting bot task for user_id: {user_id} (task_id: {self.request.id})")

#     runner = BotRunner()  # Per-task control

#     bot_status = None

#     try:
#         print("[FORCE DEBUG] Step 1: Loading user object")
#         user = User.objects.get(id=user_id)
#         print(f"[FORCE DEBUG] User loaded: {user.username}")

#         print("[FORCE DEBUG] Step 2: Getting/Creating BotStatus")
#         bot_status, _ = BotStatus.objects.get_or_create(user=user)
#         bot_status.celery_task_id = self.request.id
#         bot_status.is_running = True
#         bot_status.last_started = timezone.now()
#         bot_status.last_error = None
#         bot_status.save(update_fields=['celery_task_id', 'is_running', 'last_started', 'last_error'])
#         print("[FORCE DEBUG] BotStatus saved successfully")

#         print("[FORCE DEBUG] Step 3: Loading Broker profile")
#         broker = Broker.objects.filter(user=user, broker_name='ZERODHA', is_active=True).first()
#         if not broker:
#             error_msg = "No active broker connection found"
#             print(f"[FORCE DEBUG] {error_msg}")
#             logger.error(f"{error_msg} for {user.username}")
#             bot_status.last_error = error_msg
#             bot_status.is_running = False
#             bot_status.save(update_fields=['last_error', 'is_running'])
#             return f"User {user.username}: No broker connection"

#         print("[FORCE DEBUG] Broker found - proceeding to initialize TradingApplication")
#         logger.info(f"Found broker for {user.username}")

#         # Initialize trading application
#         print("[FORCE DEBUG] Step 4: Creating TradingApplication instance")
#         app = TradingApplication(user=user, broker=broker)  # ← pass user & broker
#         print("[FORCE DEBUG] TradingApplication created successfully")

#         # Start the bot loop
#         app.running = True

#         # Heartbeat update frequency (seconds)
#         HEARTBEAT_INTERVAL = 60
#         last_heartbeat = time.time()

#         print("[FORCE DEBUG] Step 5: Entering main bot loop")
#         logger.info(f"Starting bot loop for {user.username}")
#         while runner.running and app.running:
#             try:
#                 print("[FORCE DEBUG] Loop tick - calling app.run() single step")
#                 # FIXED: Instead of non-existent run_one_cycle(), call the full run() method
#                 # But since run() is infinite, we need to run it in a controlled way
#                 # → run a single iteration manually (extracted logic)
#                 now = datetime.now(Config.TIMEZONE)
#                 current_time = now.time()
#                 today_date = now.date()
#                 app.engine.state.daily_reset()

#                 # Pre-load data if needed
#                 if dtime(9, 20, 50) <= current_time < dtime(9, 22, 0):
#                     if app.engine.instruments is None or app.engine.weekly_df is None:
#                         print("[FORCE DEBUG] Pre-loading instruments & weekly data")
#                         app.engine.load_instruments()
#                         app.engine.load_weekly_df()

#                 # Main logic (copy from your run() method - single cycle)
#                 if Config.ENTRY_START <= current_time <= Config.ENTRY_END:
#                     print("[FORCE DEBUG] ENTRY WINDOW OPEN - attempting entry")
#                     app.engine.enter()

#                 # Heartbeat
#                 if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
#                     bot_status.last_heartbeat = timezone.now()
#                     bot_status.current_unrealized_pnl = app.engine.algo_pnl() or 0
#                     bot_status.current_margin = app.engine.actual_used_capital() or 0
#                     bot_status.save(update_fields=['last_heartbeat', 'current_unrealized_pnl', 'current_margin'])
#                     last_heartbeat = time.time()
#                     print("[FORCE DEBUG] Heartbeat saved")

#                 time.sleep(3)  # Match PNL_CHECK_INTERVAL_SECONDS

#             except Exception as inner_e:
#                 print(f"[FORCE DEBUG] Exception inside loop: {str(inner_e)}")
#                 logger.error(f"Error in bot loop for {user.username}: {str(inner_e)}")
#                 bot_status.last_error = str(inner_e)[:500]
#                 bot_status.save(update_fields=['last_error'])

#         # Graceful exit
#         print("[FORCE DEBUG] Bot loop exited")
#         logger.info(f"Bot loop stopped for {user.username}")
#         # app.exit("Task shutdown")  # call if you have exit method

#     except SoftTimeLimitExceeded:
#         print("[FORCE DEBUG] SoftTimeLimitExceeded caught")
#         logger.warning(f"Task time limit exceeded for user_id: {user_id}")
#         if bot_status:
#             bot_status.last_error = "Task time limit exceeded"
#             bot_status.is_running = False
#             bot_status.last_stopped = timezone.now()
#             bot_status.save()
#         return f"User {user.username}: Time limit exceeded"

#     except Exception as e:
#         error_msg = f"{str(e)}\n{traceback.format_exc()}"
#         print(f"[FORCE CRASH] FATAL EXCEPTION in run_user_bot: {error_msg}")
#         logger.error(f"Fatal error in run_user_bot for user_id {user_id}: {error_msg}")
#         if bot_status:
#             bot_status.last_error = error_msg[:500]
#             bot_status.is_running = False
#             bot_status.last_stopped = timezone.now()
#             bot_status.save()
#         return f"User {user.username}: Error - {str(e)}"

#     finally:
#         print("[FORCE DEBUG] Finally block - cleaning up BotStatus")
#         if bot_status:
#             bot_status.is_running = False
#             bot_status.last_stopped = timezone.now()
#             bot_status.save()
#         logger.info(f"Bot task ended for user_id {user_id}")


# @shared_task
# def check_bot_health():
#     """Periodic task (Celery Beat) to detect and clean up stale/dead bots"""
#     print("[FORCE DEBUG] check_bot_health task running")
#     running_bots = BotStatus.objects.filter(is_running=True)
#     logger.info(f"Health check: {running_bots.count()} bots marked as running")

#     now = timezone.now()
#     for status in running_bots:
#         if status.last_heartbeat:
#             age = now - status.last_heartbeat
#             if age > timedelta(minutes=10):
#                 logger.warning(f"Detected stale bot for {status.user.username} - last heartbeat {age}")
#                 status.is_running = False
#                 status.last_error = f"Stale - no heartbeat for {age}"
#                 status.save(update_fields=['is_running', 'last_error'])
#         elif status.last_started and (now - status.last_started) > timedelta(hours=1):
#             logger.warning(f"Old bot without heartbeat for {status.user.username} - marking stopped")
#             status.is_running = False
#             status.last_error = "No heartbeat support - assumed stale"
#             status.save(update_fields=['is_running', 'last_error'])
