# niftybot/niftybot/celery.py

import os
from celery import Celery
from celery.schedules import crontab

# ------------------------------------------------------------------------------
# SET DJANGO SETTINGS MODULE
# ------------------------------------------------------------------------------
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "niftybot.settings")

# ------------------------------------------------------------------------------
# CREATE CELERY APPLICATION INSTANCE
# ------------------------------------------------------------------------------
app = Celery("niftybot")

# ------------------------------------------------------------------------------
# LOAD CONFIGURATION FROM DJANGO SETTINGS (CELERY_ namespace)
# ------------------------------------------------------------------------------
app.config_from_object("django.conf:settings", namespace="CELERY")

# ------------------------------------------------------------------------------
# AUTO-DISCOVER TASKS IN ALL INSTALLED APPS
# ------------------------------------------------------------------------------
app.autodiscover_tasks()

# ------------------------------------------------------------------------------
# CRITICAL CONFIGURATION — OPTIMIZED FOR LONG-RUNNING TRADING BOTS
# ------------------------------------------------------------------------------
app.conf.update(
    # ── Never use time limits — trading tasks must run until stopped ──
    task_time_limit=None,
    task_soft_time_limit=None,

    # ── Worker settings safe for long-running / stateful tasks ──
    worker_prefetch_multiplier=1,       # No prefetching → prevents task stealing
    task_acks_late=True,                # Acknowledge only after task completes
    worker_concurrency=1,               # One bot per worker process (recommended)

    # ── Connection stability ──
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,

    # ── Result backend cleanup (not using results heavily) ──
    result_expires=3600,                # 1 hour

    # ── Timezone & UTC handling (critical for Indian market hours) ──
    timezone="Asia/Kolkata",
    enable_utc=False,

    # ── Better visibility in Flower / logs ──
    task_track_started=True,
)

# ------------------------------------------------------------------------------
# CELERY BEAT SCHEDULE — PERIODIC TASKS
# ------------------------------------------------------------------------------
app.conf.beat_schedule = {
    # Health check — detects stale/zombie bots
    "check-bot-health-every-5-minutes": {
        "task": "trading.tasks.check_bot_health",
        "schedule": crontab(minute="*/5"),
        "options": {
            "expires": 300,             # 5 minutes
        },
    },

    # Auto-start all eligible user bots at market open
    "auto-start-user-bots-0900": {
        "task": "trading.tasks.auto_start_user_bots",
        "schedule": crontab(hour=9, minute=0),
    },

    # Save daily PnL snapshot for every user at market close
    "save-daily-pnl-all-users-1545": {
        "task": "trading.tasks.save_daily_pnl_all_users",
        "schedule": crontab(hour=15, minute=45),
    },

    # Weekly cleanup of non-critical logs (INFO & WARNING only)
    # Keeps ERROR and CRITICAL logs forever
    "weekly-log-cleanup-sunday-0230": {
        "task": "trading.cleanup_old_logs",
        "schedule": crontab(day_of_week=6, hour=2, minute=30),  # Sunday = 6
        "options": {
            "expires": 3600,            # 1 hour
        },
    },
}

# ------------------------------------------------------------------------------
# DEBUG / TEST TASK (VERY USEFUL DURING DEVELOPMENT)
# ------------------------------------------------------------------------------
@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Simple debug task to test Celery connectivity and logging."""
    print(f"Celery Debug Request: {self.request!r}")


# ------------------------------------------------------------------------------
# ENTRY POINT — ALLOWS RUNNING CELERY DIRECTLY (python celery.py worker ...)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    app.start()