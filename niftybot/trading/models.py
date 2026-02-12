# trading/models.py

from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from decimal import Decimal
import json

# Optional: for production encryption of sensitive fields
# pip install django-fernet-fields
# from fernet_fields import EncryptedCharField


class Broker(models.Model):
    """
    Store Zerodha credentials for each user.
    WARNING: api_key, secret_key, totp, password are stored in PLAIN TEXT in the current version.
    This is INSECURE for production. Strongly consider using EncryptedCharField or external vault.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='brokers')
    broker_name = models.CharField(max_length=50, default='ZERODHA')
    
    api_key = models.CharField(max_length=255)          # EncryptedCharField(max_length=255) in production
    secret_key = models.CharField(max_length=255)       # EncryptedCharField(max_length=255) in production
    totp = models.CharField(max_length=100, null=True, blank=True,
                            help_text="TOTP secret for 2FA")
    zerodha_user_id = models.CharField(max_length=100, null=True, blank=True,
                                       verbose_name='Zerodha User ID')
    password = models.CharField(max_length=100, null=True, blank=True,
                                help_text="Only needed for initial login flow - remove in production")
    
    is_active = models.BooleanField(default=True)
    
    # Authentication flow fields
    request_token = models.CharField(
        max_length=255, blank=True, null=True,
        help_text="Paste the request_token from manual Zerodha login redirect URL"
    )
    access_token = models.CharField(
        max_length=100, blank=True, null=True,
        help_text="Auto-generated access token (saved after successful login)"
    )
    token_generated_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when access_token was last generated/renewed"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('user', 'broker_name')
        verbose_name = 'Broker Connection'
        verbose_name_plural = 'Broker Connections'
        ordering = ['-updated_at']

    def __str__(self):
        return f"{self.user.username} - {self.broker_name} ({'Active' if self.is_active else 'Inactive'})"

    def is_authenticated(self):
        """Check if we have a valid access token"""
        return bool(self.access_token and self.token_generated_at)

    def needs_reauth(self):
        """Check if token is close to expiry (Zerodha tokens expire ~daily)"""
        if not self.token_generated_at:
            return True
        age = timezone.now() - self.token_generated_at
        return age.total_seconds() > 86400 * 0.9  # re-auth ~before 24h expiry


class Trade(models.Model):
    """Trade records for each user"""
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('EXECUTED', 'Executed'),
        ('CANCELLED', 'Cancelled'),
        ('FAILED', 'Failed'),
    ]

    DIRECTION_CHOICES = [
        ('LONG', 'Long'),
        ('SHORT', 'Short'),
    ]

    OPTION_TYPE_CHOICES = [
        ('CE', 'Call'),
        ('PE', 'Put'),
        ('FUT', 'Future'),
        ('EQ', 'Equity'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='trades')
    algorithm_name = models.CharField(max_length=100, default='Hedged Short Strangle')
    trade_id = models.CharField(max_length=100, unique=True)
    symbol = models.CharField(max_length=50)
    
    # Strongly recommended additions for options trading
    direction = models.CharField(max_length=10, choices=DIRECTION_CHOICES, default='SHORT')
    option_type = models.CharField(max_length=10, choices=OPTION_TYPE_CHOICES, blank=True)
    strike = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    expiry = models.DateField(null=True, blank=True)
    
    quantity = models.IntegerField()
    entry_price = models.DecimalField(max_digits=12, decimal_places=2)
    exit_price = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    entry_time = models.DateTimeField()
    exit_time = models.DateTimeField(null=True, blank=True)
    pnl = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    broker = models.CharField(max_length=50, default='ZERODHA')
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-entry_time']
        indexes = [
            models.Index(fields=['user', 'entry_time']),
            models.Index(fields=['trade_id']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.trade_id} - {self.symbol} ({self.status})"

    def close_trade(self, exit_price: Decimal, exit_time=None, brokerage_fee: Decimal = Decimal('20.0')):
        """Auto-calculate PnL when closing trade - improved version"""
        if self.exit_price is not None:
            return

        qty_abs = abs(self.quantity)
        
        if self.direction == 'LONG':
            gross_pnl = (exit_price - self.entry_price) * qty_abs
        else:  # SHORT
            gross_pnl = (self.entry_price - exit_price) * qty_abs

        # Subtract approximate brokerage/slippage (customize as needed)
        self.pnl = gross_pnl - brokerage_fee
        self.exit_price = exit_price
        self.exit_time = exit_time or timezone.now()
        self.status = 'EXECUTED'
        self.save()


class DailyPnL(models.Model):
    """Daily PnL summary for each user"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='daily_pnl')
    algorithm_name = models.CharField(max_length=100, default='Hedged Short Strangle')
    date = models.DateField()
    pnl = models.DecimalField(max_digits=12, decimal_places=2)
    total_trades = models.PositiveIntegerField(default=0)
    win_trades = models.PositiveIntegerField(default=0)
    loss_trades = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'date')
        ordering = ['-date']
        verbose_name = 'Daily P&L'
        verbose_name_plural = 'Daily P&L Records'

    def __str__(self):
        return f"{self.user.username} - {self.date} - ₹{self.pnl:,.2f}"


class BotStatus(models.Model):
    """Track bot status and configuration for each user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='bot_status')
    
    is_running = models.BooleanField(default=False)
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    last_started = models.DateTimeField(null=True, blank=True)
    last_stopped = models.DateTimeField(null=True, blank=True)
    last_heartbeat = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True, null=True)
    
    current_unrealized_pnl = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    current_margin = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    
    daily_profit_target = models.DecimalField(
        max_digits=12, decimal_places=2, default=0,
        verbose_name="Today's Profit Target (₹)"
    )
    daily_stop_loss = models.DecimalField(
        max_digits=12, decimal_places=2, default=0,
        verbose_name="Today's Stop Loss (₹)"
    )
    
    # User-configurable max lots cap (used in bot logic)
    max_lots_hard_cap = models.PositiveIntegerField(
        default=1,
        validators=[MinValueValidator(1), MaxValueValidator(10)],
        help_text="Maximum lots the bot is allowed to trade (1 = safest)"
    )
    
    # Prevent multiple entries same day
    entry_attempted_date = models.DateField(
        null=True, blank=True,
        help_text="Date on which an entry was last attempted"
    )
    
    last_successful_entry = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp of the last successful trade entry"
    )
    
    state_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

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
            models.Index(fields=['level']),
        ]
        verbose_name = 'Log Entry'
        verbose_name_plural = 'Log Entries'

    def __str__(self):
        return f"[{self.level}] {self.user.username} - {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"

# ───────────────────────────────────────────────
# Signals
# ───────────────────────────────────────────────

@receiver(post_save, sender=User)
def create_user_bot_status(sender, instance, created, **kwargs):
    """Automatically create BotStatus when a new user is created"""
    if created:
        BotStatus.objects.create(user=instance)