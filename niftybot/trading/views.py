from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Sum, Count, Q
from django.utils import timezone
from datetime import date, timedelta, datetime
import json
import calendar

from .models import Broker, Trade, DailyPnL, BotStatus, LogEntry
from .forms import SignUpForm, ZerodhaConnectionForm

def home(request):
    """Home page with introduction"""
    if request.user.is_authenticated:
        return redirect('dashboard')
    
    # Get some statistics for display
    total_users = 0
    total_trades = 0
    total_pnl = 0
    
    if request.user.is_authenticated:
        total_trades = Trade.objects.filter(user=request.user).count()
        total_pnl_result = DailyPnL.objects.filter(user=request.user).aggregate(Sum('pnl'))
        total_pnl = total_pnl_result['pnl__sum'] or 0
    
    context = {
        'total_trades': total_trades,
        'total_pnl': total_pnl,
    }
    return render(request, 'trading/home.html', context)

@login_required
def dashboard(request):
    """User dashboard showing bot status and recent activity"""
    bot_status, _ = BotStatus.objects.get_or_create(user=request.user)
    
    # Get recent trades
    recent_trades = Trade.objects.filter(user=request.user).order_by('-entry_time')[:10]
    
    # Get today's PnL
    today = date.today()
    daily_pnl = DailyPnL.objects.filter(user=request.user, date=today).first()
    
    # Get weekly PnL summary
    week_ago = today - timedelta(days=7)
    weekly_summary = DailyPnL.objects.filter(
        user=request.user,
        date__gte=week_ago
    ).aggregate(
        total_pnl=Sum('pnl'),
        total_trades=Sum('total_trades')
    )
    
    # Get broker connection status
    has_broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA', is_active=True).exists()
    
    # Get monthly summary
    month_start = date(today.year, today.month, 1)
    monthly_summary = DailyPnL.objects.filter(
        user=request.user,
        date__gte=month_start
    ).aggregate(
        total_pnl=Sum('pnl'),
        total_trades=Sum('total_trades')
    )
    
    # Get win rate
    all_trades = Trade.objects.filter(user=request.user, status='EXECUTED')
    win_trades = all_trades.filter(pnl__gt=0).count()
    total_executed_trades = all_trades.count()
    win_rate = (win_trades / total_executed_trades * 100) if total_executed_trades > 0 else 0
    
    context = {
        'bot_status': bot_status,
        'recent_trades': recent_trades,
        'daily_pnl': daily_pnl,
        'weekly_pnl': weekly_summary['total_pnl'] or 0,
        'weekly_trades': weekly_summary['total_trades'] or 0,
        'monthly_pnl': monthly_summary['total_pnl'] or 0,
        'monthly_trades': monthly_summary['total_trades'] or 0,
        'win_rate': round(win_rate, 2),
        'has_broker': has_broker,
        'today': today,
    }
    return render(request, 'trading/dashboard.html', context)

@login_required
def broker_page(request):
    """Broker credentials management page"""
    broker_instance = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()
    
    if request.method == 'POST':
        form = ZerodhaConnectionForm(request.POST, instance=broker_instance)
        if form.is_valid():
            broker = form.save(commit=False)
            broker.user = request.user
            broker.broker_name = 'ZERODHA'
            broker.save()
            messages.success(request, 'Zerodha credentials saved successfully!')
            return redirect('broker')
    else:
        form = ZerodhaConnectionForm(instance=broker_instance)
    
    # Get broker connection history
    brokers = Broker.objects.filter(user=request.user).order_by('-updated_at')
    
    context = {
        'form': form,
        'brokers': brokers,
        'has_credentials': broker_instance is not None,
    }
    return render(request, 'trading/broker.html', context)

@login_required
def pnl_calendar(request):
    """Daily PnL calendar view"""
    # Get year and month from request or use current
    year = request.GET.get('year', date.today().year)
    month = request.GET.get('month', date.today().month)
    
    try:
        year = int(year)
        month = int(month)
    except (ValueError, TypeError):
        year = date.today().year
        month = date.today().month
    
    # Get daily PnL for the month
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)
    
    daily_pnl_records = DailyPnL.objects.filter(
        user=request.user,
        date__range=[month_start, month_end]
    ).order_by('date')
    
    # Create a calendar for the month
    cal = calendar.monthcalendar(year, month)
    
    # Create a dict of date -> pnl for easy lookup
    pnl_dict = {record.date: record for record in daily_pnl_records}
    
    # Get statistics
    monthly_stats = {
        'total_pnl': sum(record.pnl for record in daily_pnl_records),
        'average_pnl': sum(record.pnl for record in daily_pnl_records) / len(daily_pnl_records) if daily_pnl_records else 0,
        'positive_days': sum(1 for record in daily_pnl_records if record.pnl > 0),
        'negative_days': sum(1 for record in daily_pnl_records if record.pnl < 0),
        'max_pnl': max((record.pnl for record in daily_pnl_records), default=0),
        'min_pnl': min((record.pnl for record in daily_pnl_records), default=0),
    }
    
    # Previous and next month
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    
    context = {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'calendar': cal,
        'pnl_dict': pnl_dict,
        'daily_pnl_records': daily_pnl_records,
        'stats': monthly_stats,
        'prev_month': prev_month,
        'prev_year': prev_year,
        'next_month': next_month,
        'next_year': next_year,
    }
    return render(request, 'trading/pnl_calendar.html', context)

@login_required
def algorithms_page(request):
    """Algorithms information and control page"""
    bot_status, _ = BotStatus.objects.get_or_create(user=request.user)
    
    # Algorithm details
    algorithm_details = {
        'name': 'Hedged Short Strangle',
        'version': 'v9.8.3-fixed-v5',
        'description': 'NIFTY Weekly Options Trading Strategy',
        'underlying': 'NIFTY 50',
        'lot_size': 65,
        'entry_window': '09:21:00 - 09:21:30 IST',
        'exit_time': '15:00:00 IST',
        'max_lots': 5,
        'strategy_type': 'Options Selling with Hedges',
        'risk_level': 'Medium',
        'expected_returns': '1-2% per week',
        'max_drawdown': '10-15%',
    }
    
    # Strategy rules
    strategy_rules = [
        'No trading on Tuesdays',
        'No trading on expiry day',
        'Entry only in 9:21-9:21:30 window',
        'VIX must be between 9-22 for entry',
        'Daily target: 2% of margin or net credit ÷ days',
        'Stop loss: Equal to daily target',
        'Defensive adjustments at 50 points from short strike',
        'Max 1 adjustment per side per day',
        'Auto-exit at 3:00 PM',
    ]
    
    # Get recent algorithm logs
    recent_logs = LogEntry.objects.filter(
        user=request.user,
        level__in=['INFO', 'WARNING', 'ERROR', 'CRITICAL']
    ).order_by('-timestamp')[:20]
    
    # Get performance statistics
    today = date.today()
    month_start = date(today.year, today.month, 1)
    
    performance_stats = {
        'total_trades': Trade.objects.filter(user=request.user).count(),
        'win_rate': calculate_win_rate(request.user),
        'avg_daily_pnl': calculate_average_pnl(request.user),
        'best_day': get_best_day(request.user),
        'worst_day': get_worst_day(request.user),
        'current_streak': get_current_streak(request.user),
    }
    
    context = {
        'bot_status': bot_status,
        'algorithm': algorithm_details,
        'strategy_rules': strategy_rules,
        'recent_logs': recent_logs,
        'performance': performance_stats,
    }
    return render(request, 'trading/algorithms.html', context)

# Helper functions for algorithms page
def calculate_win_rate(user):
    trades = Trade.objects.filter(user=user, status='EXECUTED')
    if trades.count() == 0:
        return 0
    win_trades = trades.filter(pnl__gt=0).count()
    return round((win_trades / trades.count()) * 100, 2)

def calculate_average_pnl(user):
    daily_pnl = DailyPnL.objects.filter(user=user)
    if daily_pnl.count() == 0:
        return 0
    total = sum(record.pnl for record in daily_pnl)
    return round(total / daily_pnl.count(), 2)

def get_best_day(user):
    best = DailyPnL.objects.filter(user=user).order_by('-pnl').first()
    return best if best else None

def get_worst_day(user):
    worst = DailyPnL.objects.filter(user=user).order_by('pnl').first()
    return worst if worst else None

def get_current_streak(user):
    # Get consecutive profitable days
    today = date.today()
    streak = 0
    current_date = today
    
    while True:
        daily = DailyPnL.objects.filter(user=user, date=current_date).first()
        if daily and daily.pnl > 0:
            streak += 1
            current_date -= timedelta(days=1)
        else:
            break
    
    return streak

@login_required
def connect_zerodha(request):
    """Connect Zerodha credentials (legacy redirect)"""
    return redirect('broker')

@login_required
@csrf_exempt
def start_bot(request):
    """Start the trading bot for the current user"""
    # Check if user has broker credentials
    if not Broker.objects.filter(user=request.user, broker_name='ZERODHA', is_active=True).exists():
        messages.error(request, 'Please connect your Zerodha account first.')
        return redirect('broker')
    
    # Check if bot is already running
    bot_status = BotStatus.objects.get(user=request.user)
    if bot_status.is_running:
        messages.warning(request, 'Bot is already running.')
        return redirect('algorithms')
    
    # Update bot status
    bot_status.is_running = True
    bot_status.last_started = timezone.now()
    bot_status.last_error = None
    bot_status.save()
    
    messages.success(request, 'Bot started successfully! The bot will run in the background during market hours.')
    
    # Log the action
    LogEntry.objects.create(
        user=request.user,
        level='INFO',
        message='Bot started manually from web interface',
        details={'action': 'start_bot', 'time': str(timezone.now())}
    )
    
    return redirect('algorithms')

@login_required
@csrf_exempt
def stop_bot(request):
    """Stop the trading bot for the current user"""
    bot_status = BotStatus.objects.get(user=request.user)
    
    if not bot_status.is_running:
        messages.warning(request, 'Bot is not running.')
        return redirect('algorithms')
    
    # Update status
    bot_status.is_running = False
    bot_status.last_stopped = timezone.now()
    bot_status.save()
    
    messages.success(request, 'Bot stopped successfully!')
    
    # Log the action
    LogEntry.objects.create(
        user=request.user,
        level='INFO',
        message='Bot stopped manually from web interface',
        details={'action': 'stop_bot', 'time': str(timezone.now())}
    )
    
    return redirect('algorithms')

@login_required
def bot_status(request):
    """AJAX endpoint for bot status updates"""
    bot_status = BotStatus.objects.get(user=request.user)
    
    return JsonResponse({
        'is_running': bot_status.is_running,
        'last_started': bot_status.last_started.isoformat() if bot_status.last_started else None,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl),
        'current_margin': float(bot_status.current_margin),
    })

def signup(request):
    """User registration view"""
    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, 'Account created successfully! Welcome to NIFTY Trading Bot.')
            return redirect('dashboard')
    else:
        form = SignUpForm()
    return render(request, 'registration/signup.html', {'form': form})