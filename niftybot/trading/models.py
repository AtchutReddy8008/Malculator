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
    # RENAME this field to avoid clash with Django's automatic user_id field
    zerodha_user_id = models.CharField(max_length=100, null=True, blank=True, verbose_name='Zerodha User ID')
    password = models.CharField(max_length=100, null=True, blank=True)
    is_active = models.BooleanField(default=True)
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
        unique_together = ('user', 'algorithm_name', 'date')
        ordering = ['-date']
        verbose_name = 'Daily P&L'
        verbose_name_plural = 'Daily P&L Records'
    
    def __str__(self):
        return f"{self.user.username} - {self.date} - ₹{self.pnl}"

class BotStatus(models.Model):
    """Track bot status for each user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='bot_status')
    is_running = models.BooleanField(default=False)
    last_started = models.DateTimeField(null=True, blank=True)
    last_stopped = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True, null=True)
    current_unrealized_pnl = models.DecimalField(max_digits=15, decimal_places=6, default=0)
    current_margin = models.DecimalField(max_digits=15, decimal_places=6, default=0)
    current_state_json = models.JSONField(default=dict, blank=True)
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = 'Bot Status'
        verbose_name_plural = 'Bot Statuses'
    
    def __str__(self):
        return f"{self.user.username} - {'Running' if self.is_running else 'Stopped'}"
    
    def save_state(self, state_data):
        """Save bot state to JSON field"""
        self.current_state_json = state_data
        self.save()
    
    def load_state(self):
        """Load bot state from JSON field"""
        return self.current_state_json or {}

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