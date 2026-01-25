from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from django.contrib.auth.models import User
from django.utils import timezone
import time
import traceback
import logging

logger = logging.getLogger(__name__)

from .models import Broker, BotStatus
from .core.bot_original import TradingApplication
from .core.auth import generate_and_set_access_token_db

@shared_task(bind=True, time_limit=1800)  # 30 minute time limit
def run_user_bot(self, user_id):
    """
    Main Celery task to run trading bot for a specific user
    Each user runs in their own isolated task
    """
    logger.info(f"Starting bot task for user_id: {user_id}")
    
    try:
        user = User.objects.get(id=user_id)
        logger.info(f"Found user: {user.username}")
        
        bot_status, created = BotStatus.objects.get_or_create(user=user)
        logger.info(f"Bot status: {'created' if created else 'existing'}")
        
        # Update task ID in bot status
        bot_status.celery_task_id = self.request.id
        bot_status.is_running = True
        bot_status.last_started = timezone.now()
        bot_status.save()
        logger.info(f"Updated bot status for {user.username}")
        
        # Get broker credentials
        broker = Broker.objects.filter(user=user, broker_name='ZERODHA', is_active=True).first()
        if not broker:
            error_msg = "No active broker connection found"
            logger.error(f"{error_msg} for {user.username}")
            bot_status.last_error = error_msg
            bot_status.is_running = False
            bot_status.save()
            return f"User {user.username}: No broker connection"
        
        logger.info(f"Found broker for {user.username}")
        
        try:
            # Initialize and run the trading application
            logger.info(f"Initializing TradingApplication for {user.username}")
            app = TradingApplication(user=user, broker=broker)
            logger.info(f"Starting bot loop for {user.username}")
            app.run()
            logger.info(f"Bot completed for {user.username}")
        except Exception as e:
            logger.error(f"Error in TradingApplication for {user.username}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        
        # When bot stops naturally
        bot_status.is_running = False
        bot_status.last_stopped = timezone.now()
        bot_status.save()
        logger.info(f"Bot stopped normally for {user.username}")
        
        return f"User {user.username}: Bot completed"
        
    except SoftTimeLimitExceeded:
        logger.warning(f"Task time limit exceeded for user_id: {user_id}")
        bot_status.last_error = "Task time limit exceeded"
        bot_status.is_running = False
        bot_status.last_stopped = timezone.now()
        bot_status.save()
        return f"User {user.username}: Time limit exceeded"
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        logger.error(f"Error in run_user_bot for user_id {user_id}: {error_msg}")
        bot_status.last_error = error_msg[:500]  # Limit error message length
        bot_status.is_running = False
        bot_status.last_stopped = timezone.now()
        bot_status.save()
        return f"User {user.username}: Error - {str(e)}"

@shared_task
def check_bot_health():
    """Periodic task to check bot health and clean up stale tasks"""
    running_statuses = BotStatus.objects.filter(is_running=True)
    logger.info(f"Health check: {running_statuses.count()} bots running")
    
    for status in running_statuses:
        # Check if task is still running (simplified check)
        # In production, you'd use Celery's inspect() to check actual task status
        if status.last_started and (timezone.now() - status.last_started).seconds > 3600:
            # If bot has been "running" for more than an hour without updates,
            # mark it as stopped
            logger.warning(f"Marking bot as stopped for {status.user.username} - health check timeout")
            status.is_running = False
            status.last_error = "Bot health check timeout"
            status.save()