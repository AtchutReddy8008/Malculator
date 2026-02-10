from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login, logout
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django.db.models import Sum
from django.utils import timezone
from datetime import date, timedelta
import json
import traceback

from django.contrib.auth.models import User

# Celery
from celery import current_app as celery_app

# Models & Forms
from .models import Broker, Trade, DailyPnL, BotStatus, LogEntry
from .forms import SignUpForm, ZerodhaConnectionForm

# Tasks
from .tasks import run_user_bot

# Auth helpers
from .core.auth import generate_and_set_access_token_db
from kiteconnect import KiteConnect


def home(request):
    if request.user.is_authenticated:
        return redirect('dashboard')

    context = {
        'total_users': User.objects.count(),
        'total_trades': Trade.objects.count(),
        'total_pnl': Trade.objects.aggregate(total=Sum('pnl'))['total'] or 0,
    }
    return render(request, 'trading/home.html', context)


@login_required
def dashboard(request):
    bot_status, _ = BotStatus.objects.get_or_create(user=request.user)

    recent_trades = Trade.objects.filter(user=request.user).order_by('-entry_time')[:10]

    today = date.today()
    daily_pnl = DailyPnL.objects.filter(user=request.user, date=today).first()

    week_ago = today - timedelta(days=7)
    weekly_agg = DailyPnL.objects.filter(
        user=request.user,
        date__gte=week_ago
    ).aggregate(
        total_pnl=Sum('pnl'),
        total_trades=Sum('total_trades')
    )

    month_start = date(today.year, today.month, 1)
    monthly_agg = DailyPnL.objects.filter(
        user=request.user,
        date__gte=month_start
    ).aggregate(
        total_pnl=Sum('pnl'),
        total_trades=Sum('total_trades')
    )

    trades_qs = Trade.objects.filter(user=request.user, status='EXECUTED')
    total_executed = trades_qs.count()
    win_count = trades_qs.filter(pnl__gt=0).count()
    win_rate = (win_count / total_executed * 100) if total_executed > 0 else 0

    broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()
    broker_ready = broker and bool(broker.access_token)

    context = {
        'bot_status': bot_status,
        'recent_trades': recent_trades,
        'daily_pnl': daily_pnl,
        'weekly_pnl': weekly_agg['total_pnl'] or 0,
        'weekly_trades': weekly_agg['total_trades'] or 0,
        'monthly_pnl': monthly_agg['total_pnl'] or 0,
        'monthly_trades': monthly_agg['total_trades'] or 0,
        'win_rate': round(win_rate, 2),
        'broker_ready': broker_ready,
        'broker': broker,
        'today': today,
        'daily_target': float(bot_status.daily_profit_target or 0),
        'daily_stop_loss': float(bot_status.daily_stop_loss or 0),
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
        'max_lots_hard_cap': bot_status.max_lots_hard_cap,
    }
    return render(request, 'trading/dashboard.html', context)


@login_required
def broker_page(request):
    broker = Broker.objects.filter(
        user=request.user, broker_name='ZERODHA'
    ).first()

    if request.method == 'POST':
        form = ZerodhaConnectionForm(request.POST, instance=broker)
        if form.is_valid():
            broker_obj = form.save(commit=False)
            broker_obj.user = request.user
            broker_obj.broker_name = 'ZERODHA'
            broker_obj.save()

            success = generate_and_set_access_token_db(
                kite=KiteConnect(api_key=broker_obj.api_key),
                broker=broker_obj
            )

            if success:
                messages.success(request, 'Zerodha credentials saved and access token generated successfully!')
            else:
                messages.warning(
                    request,
                    'Credentials saved, but automatic token generation failed. '
                    'Please check TOTP/clock sync or generate manually.'
                )

            return redirect('broker')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = ZerodhaConnectionForm(instance=broker)

    brokers = Broker.objects.filter(user=request.user).order_by('-updated_at')

    context = {
        'form': form,
        'brokers': brokers,
        'has_credentials': broker is not None,
        'broker': broker,
    }
    return render(request, 'trading/broker.html', context)


@login_required
def pnl_calendar(request):
    year_str = request.GET.get('year', str(date.today().year))
    month_str = request.GET.get('month', str(date.today().month))

    try:
        year = int(year_str)
        month = int(month_str)
    except ValueError:
        year = date.today().year
        month = date.today().month

    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)

    daily_records = DailyPnL.objects.filter(
        user=request.user,
        date__range=[month_start, month_end]
    ).order_by('date')

    monthly_stats = {
        'total_pnl': sum(r.pnl for r in daily_records) if daily_records else 0,
        'average_pnl': sum(r.pnl for r in daily_records) / len(daily_records) if daily_records else 0,
        'positive_days': sum(1 for r in daily_records if r.pnl > 0),
        'negative_days': sum(1 for r in daily_records if r.pnl < 0),
        'max_pnl': max((r.pnl for r in daily_records), default=0),
        'min_pnl': min((r.pnl for r in daily_records), default=0),
    }

    context = {
        'year': year,
        'month': month,
        'month_name': date(year, month, 1).strftime('%B'),
        'daily_records': daily_records,
        'stats': monthly_stats,
        'prev_month': month - 1 if month > 1 else 12,
        'prev_year': year if month > 1 else year - 1,
        'next_month': month + 1 if month < 12 else 1,
        'next_year': year if month < 12 else year + 1,
    }
    return render(request, 'trading/pnl_calendar.html', context)


@login_required
def algorithms_page(request):
    bot_status, _ = BotStatus.objects.get_or_create(user=request.user)

    algorithm_details = {
        'name': 'Hedged Short Strangle',
        'version': 'v9.8.3-fixed-v5',
        'description': 'NIFTY Weekly Options Trading Strategy',
        'underlying': 'NIFTY 50',
        'lot_size': 65,
        'entry_window': '09:20:00 - 09:23:30 IST',
        'exit_time': '15:00:00 IST',
        'max_lots': 50,
        'strategy_type': 'Options Selling with Hedges',
        'risk_level': 'Medium',
        'expected_returns': '1-2% per week',
        'max_drawdown': '10-15%',
    }

    strategy_rules = [
        'No trading on Tuesdays (optional)',
        'No trading on expiry day',
        'Entry only in widened window 9:20–9:23:30',
        'VIX must be between 9-22 for entry',
        'Daily target: 2% of margin',
        'Stop loss: Equal to daily target',
        'Defensive adjustments at 50 points from short strike',
        'Max 1 adjustment per side per day',
        'Auto-exit at 3:00 PM',
    ]

    recent_logs = LogEntry.objects.filter(
        user=request.user,
        level__in=['INFO', 'WARNING', 'ERROR', 'CRITICAL']
    ).order_by('-timestamp')[:20]

    performance_stats = {
        'total_trades': Trade.objects.filter(user=request.user).count(),
        'win_rate': calculate_win_rate(request.user),
        'avg_daily_pnl': calculate_average_pnl(request.user),
        'best_day': get_best_day(request.user),
        'worst_day': get_worst_day(request.user),
        'current_streak': get_current_streak(request.user),
    }

    broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()
    broker_ready = broker and bool(broker.access_token)

    context = {
        'bot_status': bot_status,
        'algorithm': algorithm_details,
        'strategy_rules': strategy_rules,
        'recent_logs': recent_logs,
        'performance': performance_stats,
        'daily_target': float(bot_status.daily_profit_target or 0),
        'daily_stop_loss': float(bot_status.daily_stop_loss or 0),
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
        'max_lots_hard_cap': bot_status.max_lots_hard_cap,
        'broker_ready': broker_ready,
        'broker': broker,
        'can_control_bot': request.user.is_staff,
    }

    return render(request, 'trading/algorithms.html', context)


# Helper functions
def calculate_win_rate(user):
    qs = Trade.objects.filter(user=user, status='EXECUTED')
    total = qs.count()
    if total == 0:
        return 0.0
    wins = qs.filter(pnl__gt=0).count()
    return round((wins / total) * 100, 2)


def calculate_average_pnl(user):
    qs = DailyPnL.objects.filter(user=user)
    count = qs.count()
    if count == 0:
        return 0.0
    total = qs.aggregate(total=Sum('pnl'))['total'] or 0
    return round(total / count, 2)


def get_best_day(user):
    return DailyPnL.objects.filter(user=user).order_by('-pnl').first()


def get_worst_day(user):
    return DailyPnL.objects.filter(user=user).order_by('pnl').first()


def get_current_streak(user):
    streak = 0
    current = date.today()
    while True:
        record = DailyPnL.objects.filter(user=user, date=current).first()
        if record and record.pnl > 0:
            streak += 1
            current -= timedelta(days=1)
        else:
            break
    return streak


@login_required
@require_POST
@csrf_protect
def start_bot(request):
    if not request.user.is_staff:
        messages.error(request, "Only staff users are allowed to run algorithms.")
        return redirect('dashboard')

    user = request.user
    bot_status = BotStatus.objects.filter(user=user).first()

    if bot_status and bot_status.is_running:
        messages.warning(request, 'Bot is already running. No new task launched.')
        return redirect('dashboard')

    broker = Broker.objects.filter(user=user, broker_name='ZERODHA', is_active=True).first()
    if not broker or not broker.access_token:
        messages.error(request, 'Zerodha connection not ready. Set up broker first.')
        return redirect('broker')

    try:
        result = run_user_bot.delay(user.id)

        bot_status, _ = BotStatus.objects.get_or_create(user=user)
        bot_status.is_running = True
        bot_status.celery_task_id = result.id
        bot_status.last_started = timezone.now()
        bot_status.last_error = None
        bot_status.save(update_fields=['is_running', 'celery_task_id', 'last_started', 'last_error'])

        LogEntry.objects.create(
            user=user,
            level='INFO',
            message='Bot started – Celery task launched',
            details={'task_id': result.id, 'time': str(timezone.now())}
        )

        messages.success(
            request,
            f'Bot started successfully! Task ID: {result.id[:8]}... '
            '(refresh in 10–30 seconds for live status)'
        )
    except Exception as e:
        error_msg = f'Failed to start bot: {str(e)}'
        if bot_status:
            bot_status.last_error = error_msg[:500]
            bot_status.save(update_fields=['last_error'])
        LogEntry.objects.create(
            user=user,
            level='ERROR',
            message=error_msg,
            details={'action': 'start_bot_failed'}
        )
        messages.error(request, error_msg)

    return redirect('dashboard')


@login_required
@require_POST
@csrf_protect
def stop_bot(request):
    if not request.user.is_staff:
        messages.error(request, "Only staff users are allowed to control the bot.")
        return redirect('dashboard')

    user = request.user
    bot_status = get_object_or_404(BotStatus, user=user)

    if not bot_status.is_running:
        messages.warning(request, 'Bot is not running.')
        return redirect('dashboard')

    revoked = False
    if bot_status.celery_task_id:
        try:
            celery_app.control.revoke(
                bot_status.celery_task_id,
                terminate=True,
                signal='SIGTERM'
            )
            revoked = True
            messages.info(request, 'Stop signal sent. Task should exit within 5–30 seconds.')
        except Exception as e:
            LogEntry.objects.create(
                user=user,
                level='WARNING',
                message=f'Failed to revoke task {bot_status.celery_task_id}: {str(e)}'
            )
            messages.warning(request, 'Could not revoke task cleanly (still stopping via flag).')

    bot_status.is_running = False
    bot_status.celery_task_id = None
    bot_status.last_stopped = timezone.now()
    bot_status.save(update_fields=['is_running', 'celery_task_id', 'last_stopped'])

    LogEntry.objects.create(
        user=user,
        level='INFO',
        message='Bot stopped manually',
        details={'task_revoked': revoked}
    )

    messages.success(request, 'Bot stop requested. Refresh page to confirm.')
    return redirect('dashboard')


@login_required
def bot_status(request):
    bot_status = get_object_or_404(BotStatus, user=request.user)
    now = timezone.now()

    heartbeat_ago = "Never"
    is_actually_alive = bot_status.is_running

    if hasattr(bot_status, 'last_heartbeat') and bot_status.last_heartbeat:
        delta = now - bot_status.last_heartbeat
        heartbeat_ago = f"{delta.total_seconds() // 60:.0f} min ago"
        is_actually_alive = bot_status.is_running and delta < timedelta(minutes=5)

    return JsonResponse({
        'is_running': is_actually_alive,
        'flag_set': bot_status.is_running,
        'last_heartbeat_ago': heartbeat_ago,
        'last_started': bot_status.last_started.isoformat() if bot_status.last_started else None,
        'last_stopped': bot_status.last_stopped.isoformat() if bot_status.last_stopped else None,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
        'daily_target': float(bot_status.daily_profit_target or 0),
        'daily_stop_loss': float(bot_status.daily_stop_loss or 0),
        'max_lots_hard_cap': bot_status.max_lots_hard_cap,
        'task_id': bot_status.celery_task_id or 'None',
        'last_error': bot_status.last_error or 'None'
    })


@login_required
@require_POST
@csrf_protect
def update_max_lots(request):
    if not request.user.is_staff:
        return JsonResponse({'success': False, 'error': 'Only staff users can update max lots.'}, status=403)

    try:
        data = json.loads(request.body)
        new_cap = int(data.get('max_lots_hard_cap', 0))

        if not 1 <= new_cap <= 10:
            return JsonResponse({'success': False, 'error': 'Value must be between 1 and 10'})

        bot_status = BotStatus.objects.get(user=request.user)
        old_cap = bot_status.max_lots_hard_cap
        bot_status.max_lots_hard_cap = new_cap
        bot_status.save(update_fields=['max_lots_hard_cap'])

        LogEntry.objects.create(
            user=request.user,
            level='INFO',
            message=f"Max lots hard cap changed from {old_cap} → {new_cap}",
            details={'action': 'update_max_lots', 'new_value': new_cap}
        )

        return JsonResponse({'success': True, 'new_value': new_cap})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})


def signup(request):
    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, 'Account created successfully! Welcome.')
            return redirect('dashboard')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = SignUpForm()

    return render(request, 'registration/signup.html', {'form': form})


def user_logout(request):
    logout(request)
    messages.info(request, 'You have been logged out.')
    return redirect('home')


@login_required
def connect_zerodha(request):
    broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()

    if broker and broker.access_token:
        messages.info(request, "Zerodha is already connected!")
        return redirect('broker')

    messages.info(request, "Please fill in your Zerodha credentials on the Broker page.")
    return redirect('broker')


# ───────────────────────────────────────────────
# NEW VIEWS FOR STRATEGIES SECTION (added for client request)
# ───────────────────────────────────────────────

@login_required
def strategies_list(request):
    """
    New page: Shows 4 strategy cards
    Short Strangle is active, others are coming soon
    """
    context = {
        'strategies': [
            {
                'name': 'Short Strangle',
                'description': 'Hedged weekly NIFTY options selling with defensive adjustments',
                'status': 'active',
                'url_name': 'short_strangle_detail',
                'icon': 'fas fa-scissors',
                'color': 'success'
            },
            {
                'name': 'Delta BTCUSD',
                'description': 'Delta-neutral perpetual futures strategy on BTC/USD',
                'status': 'coming_soon',
                'url_name': 'delta_btcusd_detail',
                'icon': 'fab fa-bitcoin',
                'color': 'secondary'
            },
            {
                'name': 'Nifty Buy',
                'description': 'Momentum-based long entries on NIFTY with trend filters',
                'status': 'coming_soon',
                'url_name': 'nifty_buy_detail',
                'icon': 'fas fa-arrow-up',
                'color': 'secondary'
            },
            {
                'name': 'Nifty Sell',
                'description': 'Counter-trend short entries on NIFTY with mean-reversion signals',
                'status': 'coming_soon',
                'url_name': 'nifty_sell_detail',
                'icon': 'fas fa-arrow-down',
                'color': 'secondary'
            },
        ]
    }
    return render(request, 'trading/strategies_list.html', context)


@login_required
def coming_soon_placeholder(request):
    """
    Simple placeholder page for future strategies
    Shows a nice "Coming Soon" message with back button
    """
    # Get the strategy name from URL name for dynamic title
    strategy_name = request.resolver_match.url_name.replace('_detail', '').replace('-', ' ').title()
    
    context = {
        'title': strategy_name,
        'description': "This powerful strategy is currently under active development. We're working hard to bring it to you soon!",
        'expected': "Expected release: 2026"
    }
    return render(request, 'trading/coming_soon.html', context)