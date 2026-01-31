from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models.signals import post_save
from django.dispatch import receiver
import json


class Broker(models.Model):
    """Store Zerodha credentials for each user (plain text as per requirements)"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='brokers')
    broker_name = models.CharField(max_length=50, default='ZERODHA')
    api_key = models.CharField(max_length=255)
    secret_key = models.CharField(max_length=255)
    totp = models.CharField(max_length=100, null=True, blank=True)
    zerodha_user_id = models.CharField(max_length=100, null=True, blank=True, verbose_name='Zerodha User ID')
    password = models.CharField(max_length=100, null=True, blank=True)
    is_active = models.BooleanField(default=True)

    # ────────────── NEW FIELDS ADDED FOR AUTHENTICATION FLOW ──────────────
    request_token = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Paste the request_token from manual Zerodha login redirect URL"
    )
    access_token = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="Auto-generated access token (saved after successful login)"
    )
    token_generated_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp when access_token was last generated"
    )
    # ──────────────────────────────────────────────────────────────────────

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('user', 'broker_name')
        verbose_name = 'Broker Connection'
        verbose_name_plural = 'Broker Connections'

    def __str__(self):
        return f"{self.user.username} - {self.broker_name}"


class Trade(models.Model):
    """Trade records for each user"""
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('EXECUTED', 'Executed'),
        ('CANCELLED', 'Cancelled'),
        ('FAILED', 'Failed'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='trades')
    algorithm_name = models.CharField(max_length=100, default='Hedged Short Strangle')
    trade_id = models.CharField(max_length=100, unique=True)
    symbol = models.CharField(max_length=50)
    quantity = models.IntegerField()
    entry_price = models.DecimalField(max_digits=15, decimal_places=6)
    exit_price = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    entry_time = models.DateTimeField()
    exit_time = models.DateTimeField(null=True, blank=True)
    pnl = models.DecimalField(max_digits=15, decimal_places=6, default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    broker = models.CharField(max_length=50)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-entry_time']
        indexes = [
            models.Index(fields=['user', 'entry_time']),
            models.Index(fields=['trade_id']),
        ]
    
    def __str__(self):
        return f"{self.trade_id} - {self.symbol}"
    
    def close_trade(self, exit_price, exit_time=None):
        """Auto-calculate PnL when closing trade"""
        if self.status != 'EXECUTED' or self.exit_price is not None:
            return
        self.exit_price = exit_price
        self.exit_time = exit_time or timezone.now()
        qty_abs = abs(self.quantity)
        if self.quantity > 0:  # long
            self.pnl = (exit_price - self.entry_price) * qty_abs
        else:  # short
            self.pnl = (self.entry_price - exit_price) * qty_abs
        self.save()


class DailyPnL(models.Model):
    """Daily PnL summary for each user"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='daily_pnl')
    algorithm_name = models.CharField(max_length=100, default='Hedged Short Strangle')
    date = models.DateField()
    pnl = models.DecimalField(max_digits=15, decimal_places=6)
    total_trades = models.IntegerField(default=0)
    win_trades = models.IntegerField(default=0)
    loss_trades = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ('user', 'date')  # removed algorithm_name from unique if single-algo
        ordering = ['-date']
        verbose_name = 'Daily P&L'
        verbose_name_plural = 'Daily P&L Records'
    
    def __str__(self):
        return f"{self.user.username} - {self.date} - ₹{self.pnl}"


class BotStatus(models.Model):
    """Track bot status for each user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='bot_status')
    is_running = models.BooleanField(default=False)
    pid = models.PositiveIntegerField(null=True, blank=True)                  # Added for process control
    last_started = models.DateTimeField(null=True, blank=True)
    last_stopped = models.DateTimeField(null=True, blank=True)
    last_heartbeat = models.DateTimeField(null=True, blank=True)             # Added for alive detection
    last_error = models.TextField(blank=True, null=True)
    current_unrealized_pnl = models.DecimalField(max_digits=15, decimal_places=6, default=0)
    current_margin = models.DecimalField(max_digits=15, decimal_places=6, default=0)
    state_json = models.JSONField(default=dict, blank=True)                  # Renamed from current_state_json
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # These two fields are now correctly included
    daily_profit_target = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        default=0, 
        null=True, 
        blank=True,
        verbose_name="Today's Profit Target (₹)"
    )
    
    daily_stop_loss = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        default=0, 
        null=True, 
        blank=True,
        verbose_name="Today's Stop Loss (₹)"
    )
    
    class Meta:
        verbose_name = 'Bot Status'
        verbose_name_plural = 'Bot Statuses'
    
    def __str__(self):
        return f"{self.user.username} - {'Running' if self.is_running else 'Stopped'}"
    
    def save_state(self, state_data):
        self.state_json = state_data
        self.save(update_fields=['state_json'])
    
    def load_state(self):
        return self.state_json or {}


class LogEntry(models.Model):
    """Database logging for bot activities"""
    LEVEL_CHOICES = [
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='logs')
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES)
    message = models.TextField()
    details = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['user', 'timestamp']),
        ]
        verbose_name = 'Log Entry'
        verbose_name_plural = 'Log Entries'
    
    def __str__(self):
        return f"[{self.level}] {self.user.username} - {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"


@receiver(post_save, sender=User)
def create_user_bot_status(sender, instance, created, **kwargs):
    """Create BotStatus when a new user is created"""
    if created:
        BotStatus.objects.create(user=instance)
