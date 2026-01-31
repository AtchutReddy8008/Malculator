from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User
from .models import Broker, Trade, DailyPnL, BotStatus, LogEntry


class BrokerAdmin(admin.ModelAdmin):
    list_display = (
        'user', 
        'broker_name', 
        'is_active', 
        'created_at',
        'token_generated_at',  # NEW: show when token was last generated
    )
    list_filter = ('broker_name', 'is_active')
    search_fields = ('user__username', 'zerodha_user_id')
    readonly_fields = ('created_at', 'updated_at', 'token_generated_at')  # NEW: make token timestamp readonly
    fieldsets = (
        ('User Info', {'fields': ('user', 'broker_name')}),
        ('Zerodha Credentials', {
            'fields': (
                'api_key', 
                'secret_key', 
                'zerodha_user_id', 
                'password', 
                'totp'
            )
        }),
        ('Authentication Tokens', {  # NEW section for easy visibility
            'fields': (
                'request_token', 
                'access_token', 
                'token_generated_at'
            )
        }),
        ('Status', {'fields': ('is_active',)}),
        ('Timestamps', {'fields': ('created_at', 'updated_at')}),
    )


class TradeAdmin(admin.ModelAdmin):
    list_display = ('trade_id', 'user', 'symbol', 'quantity', 'entry_price', 'pnl', 'status', 'entry_time')
    list_filter = ('status', 'broker', 'entry_time')
    search_fields = ('trade_id', 'symbol', 'user__username')
    readonly_fields = ('created_at',)
    date_hierarchy = 'entry_time'


class DailyPnLAdmin(admin.ModelAdmin):
    list_display = ('user', 'date', 'pnl', 'total_trades', 'win_trades', 'loss_trades')
    list_filter = ('date', 'algorithm_name')
    search_fields = ('user__username',)
    readonly_fields = ('created_at',)


class BotStatusAdmin(admin.ModelAdmin):
    list_display = (
        'user', 
        'is_running', 
        'last_started', 
        'last_stopped', 
        'current_unrealized_pnl', 
        'daily_profit_target', 
        'daily_stop_loss'
    )
    list_filter = ('is_running',)
    search_fields = ('user__username',)
    readonly_fields = (
        'created_at', 
        'updated_at', 
        'celery_task_id', 
        'daily_profit_target', 
        'daily_stop_loss'
    )
    
    def has_add_permission(self, request):
        return False
    
    def has_delete_permission(self, request, obj=None):
        return False


class LogEntryAdmin(admin.ModelAdmin):
    list_display = ('user', 'level', 'message', 'timestamp')
    list_filter = ('level', 'timestamp')
    search_fields = ('user__username', 'message')
    readonly_fields = ('timestamp',)
    date_hierarchy = 'timestamp'


# Register all models (no duplicates)
admin.site.register(Broker, BrokerAdmin)
admin.site.register(Trade, TradeAdmin)
admin.site.register(DailyPnL, DailyPnLAdmin)
admin.site.register(BotStatus, BotStatusAdmin)
admin.site.register(LogEntry, LogEntryAdmin)
