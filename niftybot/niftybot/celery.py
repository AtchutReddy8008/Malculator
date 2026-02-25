# # niftybot/niftybot/celery.py

# import os
# from celery import Celery
# from celery.schedules import crontab

# # ------------------------------------------------------------------------------
# # SET DJANGO SETTINGS MODULE
# # ------------------------------------------------------------------------------
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "niftybot.settings")

# # ------------------------------------------------------------------------------
# # CREATE CELERY APPLICATION INSTANCE
# # ------------------------------------------------------------------------------
# app = Celery("niftybot")

# # Makes "from niftybot.celery import app" cleaner
# __all__ = ("app",)

# # ------------------------------------------------------------------------------
# # LOAD CONFIGURATION FROM DJANGO SETTINGS (CELERY_ namespace)
# # ------------------------------------------------------------------------------
# app.config_from_object("django.conf:settings", namespace="CELERY")

# # ------------------------------------------------------------------------------
# # AUTO-DISCOVER TASKS IN ALL INSTALLED APPS
# # ------------------------------------------------------------------------------
# app.autodiscover_tasks()

# # ------------------------------------------------------------------------------
# # CRITICAL CONFIGURATION — OPTIMIZED FOR LONG-RUNNING TRADING BOTS
# # ------------------------------------------------------------------------------
# app.conf.update(
#     # ── Never use time limits — trading tasks must run until stopped ──
#     task_time_limit=None,
#     task_soft_time_limit=None,

#     # ── Worker settings safe for long-running / stateful tasks ──
#     worker_prefetch_multiplier=1,       # No prefetching → prevents task stealing
#     task_acks_late=True,                # Acknowledge only after task completes
#     worker_concurrency=1,               # One bot per worker process (recommended)

#     # ── Connection stability ──
#     broker_connection_retry_on_startup=True,
#     broker_connection_max_retries=100,  # ← Changed: safer than None (infinite)
#     broker_connection_timeout=10,

#     # ── Result backend cleanup (not using results heavily) ──
#     result_expires=3600,                # 1 hour

#     # ── Timezone & UTC handling (critical for Indian market hours) ──
#     timezone="Asia/Kolkata",
#     enable_utc=False,

#     # ── Better visibility in Flower / logs ──
#     task_track_started=True,

#     # Optional: global rate limit example (uncomment if needed)
#     # task_default_rate_limit='30/m',   # e.g. max 30 tasks per minute globally

#     # Optional: example queue routing (uncomment and customize when scaling)
#     # task_routes={
#     #     'trading.tasks.run_user_bot': {'queue': 'bot_tasks'},
#     #     'trading.tasks.check_bot_health': {'queue': 'beat_tasks'},
#     # },
# )

# # ------------------------------------------------------------------------------
# # CELERY BEAT SCHEDULE — PERIODIC TASKS
# # ------------------------------------------------------------------------------
# app.conf.beat_schedule = {
#     # Health check — detects stale/zombie bots
#     "check-bot-health-every-5-minutes": {
#         "task": "trading.tasks.check_bot_health",
#         "schedule": crontab(minute="*/5"),
#         "options": {
#             "expires": 300,             # 5 minutes
#         },
#     },

#     # Auto-start all eligible user bots at market open
#     "auto-start-user-bots-0900": {
#         "task": "trading.tasks.auto_start_user_bots",
#         "schedule": crontab(hour=9, minute=0),
#     },

#     # Save daily PnL snapshot for every user at market close
#     "save-daily-pnl-all-users-1545": {
#         "task": "trading.tasks.save_daily_pnl_all_users",
#         "schedule": crontab(hour=15, minute=45),
#     },

#     # Weekly cleanup of non-critical logs (INFO & WARNING only)
#     # Keeps ERROR and CRITICAL logs forever
#     "weekly-log-cleanup-sunday-0230": {
#         "task": "trading.cleanup_old_logs",
#         "schedule": crontab(day_of_week=6, hour=2, minute=30),  # Sunday = 6
#         "options": {
#             "expires": 3600,            # 1 hour
#         },
#     },
# }

# # ------------------------------------------------------------------------------
# # DEBUG / TEST TASK (VERY USEFUL DURING DEVELOPMENT)
# # ------------------------------------------------------------------------------
# @app.task(bind=True, ignore_result=True)
# def debug_task(self):
#     """Simple debug task to test Celery connectivity and logging."""
#     print(f"Celery Debug Request: {self.request!r}")


# # ------------------------------------------------------------------------------
# # ENTRY POINT — ALLOWS RUNNING CELERY DIRECTLY (python celery.py worker ...)
# # ------------------------------------------------------------------------------
# if __name__ == "__main__":
#     app.start()
import os
from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "niftybot.settings")

# Create Celery app instance
app = Celery("niftybot")

# Makes "from niftybot.celery import app" cleaner in other modules
__all__ = ("app",)

# Load configuration from Django settings (using CELERY_ namespace)
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()


# ──────────────────────────────────────────────────────────────────────────────
# CRITICAL CONFIGURATION — OPTIMIZED FOR LONG-RUNNING TRADING BOTS
# ──────────────────────────────────────────────────────────────────────────────
app.conf.update(
    # Never kill long-running tasks automatically (trading must continue)
    task_time_limit=None,
    task_soft_time_limit=None,

    # Worker behavior — important for stateful / long-lived bots
    worker_prefetch_multiplier=1,           # Prevent task stealing
    task_acks_late=True,                    # Ack only after task finishes
    worker_concurrency=1,                   # 1 bot per process (recommended for isolation)

    # Connection resilience (very important in India with variable internet)
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=100,
    broker_connection_timeout=15,           # Slightly higher timeout
    broker_heartbeat=30,                    # Enable heartbeat detection

    # Result backend (we don't use results much, so short expiry)
    result_expires=3600,                    # 1 hour

    # Timezone — must match Indian market
    timezone="Asia/Kolkata",
    enable_utc=False,

    # Better observability
    task_track_started=True,
    task_default_rate_limit="100/m",        # Global safety valve

    # Soft shutdown support (Celery ≥ 5.3) — gives tasks time to clean up
    worker_soft_shutdown_on_idle=True,
    worker_soft_shutdown_timeout=300,       # 5 minutes grace period

    # Optional: when you scale to many users, consider separate queues
    # task_routes = {
    #     'trading.tasks.run_user_bot':          {'queue': 'long_running_bots'},
    #     'trading.tasks.check_bot_health':      {'queue': 'beat_monitoring'},
    #     'trading.tasks.refresh_expired_tokens':{'queue': 'beat_monitoring'},
    # },
)


# ──────────────────────────────────────────────────────────────────────────────
# CELERY BEAT SCHEDULE — PERIODIC TASKS (IST times)
# ──────────────────────────────────────────────────────────────────────────────
app.conf.beat_schedule = {
    # ── Health monitoring ─────────────────────────────────────────────────────
    "check-bot-health-every-5-min": {
        "task": "trading.tasks.check_bot_health",
        "schedule": crontab(minute="*/5"),
        "options": {"expires": 300},
    },

    # ── Proactive Zerodha token refresh (before daily expiry ~6 AM) ───────────
    "refresh-zerodha-tokens-0545": {
        "task": "trading.tasks.refresh_expired_tokens",
        "schedule": crontab(hour=5, minute=45),
        "options": {"expires": 1800},           # 30 min
    },

    # ── Auto-start bots shortly after market open (gives Zerodha time) ────────
    "auto-start-user-bots-0905": {
        "task": "trading.tasks.auto_start_user_bots",
        "schedule": crontab(hour=9, minute=5),
    },

    # ── Daily PnL snapshot — after most positions are closed ──────────────────
    "save-daily-pnl-all-users-1550": {
        "task": "trading.tasks.save_daily_pnl_all_users",
        "schedule": crontab(hour=15, minute=50),
    },

    # ── Weekly cleanup of non-critical logs (Sunday early morning) ────────────
    "weekly-log-cleanup-sunday-0230": {
        "task": "trading.tasks.cleanup_old_logs",
        "schedule": crontab(day_of_week=6, hour=2, minute=30),  # Sunday=6
        "options": {"expires": 3600},
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# DEBUG / CONNECTIVITY TEST TASK (very useful during dev & deploy)
# ──────────────────────────────────────────────────────────────────────────────
@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Simple task to verify Celery is alive and logging correctly."""
    request = self.request
    logger = logging.getLogger("celery.debug")
    logger.info(
        f"Debug task executed | "
        f"hostname={request.hostname} | "
        f"delivery_info={request.delivery_info} | "
        f"time={timezone.now().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    return "Celery is working"


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT — allows running celery directly (python celery.py worker ...)
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.start()