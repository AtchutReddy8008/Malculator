# trading/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login, logout, authenticate
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django.db.models import Sum, Avg, Count, Max, Min, Q
from django.utils import timezone
from datetime import date, timedelta
import json
import traceback
from calendar import monthcalendar
from decimal import Decimal

from django.contrib.auth.models import User
from django.db import transaction
from django.views import View

# Celery
from celery import current_app as celery_app

# Models & Forms
from .models import Broker, Trade, DailyPnL, BotStatus, LogEntry
from .forms import SignUpForm, ZerodhaConnectionForm

# Tasks
from .tasks import run_user_bot, generate_zerodha_token_task

# Auth helpers
from .core.auth import generate_and_set_access_token_db
from kiteconnect import KiteConnect

from django.contrib.auth.forms import AuthenticationForm


# ───────────────────────────────────────────────
# Combined Auth View (Login + Signup on single page)
# ───────────────────────────────────────────────
class AuthView(View):
    template_name = 'trading/auth_combined.html'  # Make sure this file exists!
    login_form_class = AuthenticationForm
    signup_form_class = SignUpForm

    def get(self, request):
        mode = request.GET.get('mode', 'login')  # default to login tab

        login_form = self.login_form_class()
        signup_form = self.signup_form_class()

        context = {
            'login_form': login_form,
            'signup_form': signup_form,
            'mode': mode,
        }
        return render(request, self.template_name, context)

    def post(self, request):
        mode = request.POST.get('form_type', 'login')

        if mode == 'login':
            form = self.login_form_class(data=request.POST)
            if form.is_valid():
                user = form.get_user()
                login(request, user)
                messages.success(request, "Welcome back! Dashboard loading...")
                return redirect('dashboard')
            else:
                messages.error(request, "Invalid credentials. Please check and try again.")
        else:  # signup
            form = self.signup_form_class(request.POST)
            if form.is_valid():
                user = form.save()
                # Auto-login immediately after signup
                login(request, user)
                # Ensure BotStatus exists for the new user
                BotStatus.objects.get_or_create(user=user)
                messages.success(request, "Account created successfully! Welcome to your dashboard.")
                return redirect('dashboard')
            else:
                messages.error(request, "Please correct the errors below.")
                print("Signup form errors:", form.errors)

        # If form invalid → re-render with the active tab preserved
        login_form = self.login_form_class() if mode != 'login' else form
        signup_form = self.signup_form_class() if mode != 'signup' else form

        context = {
            'login_form': login_form,
            'signup_form': signup_form,
            'mode': mode,
        }
        return render(request, self.template_name, context)


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
    try:
        bot_status, _ = BotStatus.objects.get_or_create(user=request.user)

        recent_trades = Trade.objects.filter(user=request.user).order_by('-entry_time')[:10]

        stats = get_user_stats(request.user)

        today_pnl = DailyPnL.objects.filter(user=request.user, date=date.today()).first()

        broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()
        broker_ready = broker and bool(broker.access_token)

        overall_win_rate = calculate_overall_day_win_rate(request.user)

        if today_pnl and today_pnl.total_trades > 0:
            today_win_rate = round((today_pnl.win_trades / today_pnl.total_trades) * 100, 2)
            today_win_trades = today_pnl.win_trades
            today_total_trades = today_pnl.total_trades
        else:
            today_win_rate = 0.0
            today_win_trades = 0
            today_total_trades = 0

        performance = {
            'total_trades': Trade.objects.filter(user=request.user).count(),
            'win_rate': overall_win_rate,
            'daily_win_trades': today_win_trades,
            'daily_total_trades': today_total_trades,
            'avg_daily_pnl': calculate_average_pnl(request.user),
            'best_day': get_best_day(request.user),
            'worst_day': get_worst_day(request.user),
            'current_streak': get_current_streak(request.user, 'win'),
            'current_loss_streak': get_current_streak(request.user, 'loss'),
        }

        context = {
            'bot_status': bot_status,
            'recent_trades': recent_trades,
            'daily_pnl': stats['daily_pnl'],
            'weekly_pnl': stats['weekly_pnl'],
            'weekly_trades': stats['weekly_trades'],
            'monthly_pnl': stats['monthly_pnl'],
            'monthly_trades': stats['monthly_trades'],
            'win_rate': overall_win_rate,
            'broker_ready': broker_ready,
            'broker': broker,
            'today': date.today(),
            'daily_target': float(bot_status.daily_profit_target or 0),
            'daily_stop_loss': float(bot_status.daily_stop_loss or 0),
            'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
            'current_margin': float(bot_status.current_margin or 0),
            'max_lots_hard_cap': bot_status.max_lots_hard_cap or 0,
            'performance': performance,
        }
        return render(request, 'trading/dashboard.html', context)

    except Exception as e:
        messages.error(request, f"Error loading dashboard: {str(e)}")
        LogEntry.objects.create(
            user=request.user,
            level='ERROR',
            message=f"Dashboard view failed: {str(e)}",
            details={'trace': traceback.format_exc()}
        )
        return render(request, 'trading/dashboard.html', {'error': str(e)})


@login_required
def dashboard_stats(request):
    """JSON endpoint for live dashboard updates"""
    user = request.user
    bot_status = BotStatus.objects.filter(user=user).first()
    now = timezone.now()

    data = {
        'bot_running': bot_status.is_running if bot_status else False,
        'last_heartbeat_ago': "Never",
        'current_unrealized_pnl': 0.0,
        'current_margin': 0.0,
        'daily_target': 0.0,
        'daily_stop_loss': 0.0,
        'timestamp': now.isoformat(),
        'pnl_source': 'cached',
        'overall_day_win_rate': calculate_overall_day_win_rate(user),
        'debug_today_record_exists': False,
        'debug_today_total_trades': 0,
        'debug_today_win_trades': 0,
        'debug_today_pnl': 0.0,
    }

    if bot_status:
        if bot_status.last_heartbeat:
            delta = now - bot_status.last_heartbeat
            data['last_heartbeat_ago'] = f"{delta.total_seconds() // 60:.0f} min ago"

        cached_pnl = float(bot_status.current_unrealized_pnl or 0)
        data['current_unrealized_pnl'] = cached_pnl
        data['current_margin'] = float(bot_status.current_margin or 0)
        data['daily_target'] = float(bot_status.daily_profit_target or 0)
        data['daily_stop_loss'] = float(bot_status.daily_stop_loss or 0)

    today_pnl = DailyPnL.objects.filter(user=user, date=date.today()).first()
    if today_pnl:
        data['debug_today_record_exists'] = True
        data['debug_today_total_trades'] = today_pnl.total_trades
        data['debug_today_win_trades'] = today_pnl.win_trades
        data['debug_today_pnl'] = float(today_pnl.pnl or 0)

    best = get_best_day(user)
    if best:
        data['best_day'] = {
            'pnl': float(best.pnl),
            'date': best.date.strftime('%d %b %Y')
        }

    worst = get_worst_day(user)
    if worst:
        data['worst_day'] = {
            'pnl': float(worst.pnl),
            'date': worst.date.strftime('%d %b %Y')
        }

    stats = get_user_stats(user)
    data['daily_pnl']   = float(stats['daily_pnl'].pnl if stats['daily_pnl'] else 0)
    data['weekly_pnl']  = float(stats['weekly_pnl'])
    data['monthly_pnl'] = float(stats['monthly_pnl'])

    return JsonResponse(data)


def calculate_overall_day_win_rate(user):
    qs = DailyPnL.objects.filter(user=user)
    total_days = qs.count()
    if total_days == 0:
        return 0.0
    winning_days = qs.filter(pnl__gt=0).count()
    return round((winning_days / total_days) * 100, 2)


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

            if not broker or not broker.access_token:
                generate_zerodha_token_task.delay(broker_obj.id)
                messages.success(
                    request,
                    'Zerodha credentials saved. Token generation started in background...'
                )
            else:
                messages.info(request, 'Zerodha credentials updated successfully.')

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
        if not (1900 <= year <= 2100 and 1 <= month <= 12):
            raise ValueError("Invalid date range")
    except ValueError:
        year = date.today().year
        month = date.today().month
        messages.warning(request, "Invalid year/month parameter — showing current month.")

    calendar_grid = monthcalendar(year, month)

    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)

    daily_records = DailyPnL.objects.filter(
        user=request.user,
        date__range=[month_start, month_end]
    ).order_by('date')

    today = date.today()
    bot_status = BotStatus.objects.filter(user=request.user).first()
    today_pnl = Decimal('0.00')
    today_record_exists = daily_records.filter(date=today).exists()

    if bot_status and bot_status.current_unrealized_pnl is not None:
        today_pnl = bot_status.current_unrealized_pnl

    pnl_by_date = {
        str(rec.date): {
            'pnl': float(rec.pnl),
            'total_trades': rec.total_trades,
            'win_trades': rec.win_trades,
            'win_rate': round((rec.win_trades / rec.total_trades * 100), 1) if rec.total_trades > 0 else None,
        }
        for rec in daily_records
    }

    today_str = today.strftime("%Y-%m-%d")
    if bot_status and bot_status.is_running:
        pnl_by_date[today_str] = {
            'pnl': float(today_pnl),
            'total_trades': 0,
            'win_trades': 0,
            'win_rate': None,
        }

    monthly_stats = daily_records.aggregate(
        total_pnl=Sum('pnl'),
        average_pnl=Avg('pnl'),
        positive_days=Count('id', filter=Q(pnl__gt=0)),
        negative_days=Count('id', filter=Q(pnl__lt=0)),
        max_pnl=Max('pnl'),
        min_pnl=Min('pnl'),
    ) or {
        'total_pnl': Decimal('0.00'), 'average_pnl': Decimal('0.00'),
        'positive_days': 0, 'negative_days': 0,
        'max_pnl': Decimal('0.00'), 'min_pnl': Decimal('0.00')
    }

    context = {
        'year': year,
        'month': month,
        'month_name': date(year, month, 1).strftime('%B'),
        'calendar': calendar_grid,
        'daily_records': daily_records,
        'pnl_by_date': pnl_by_date,
        'today_live_pnl': today_pnl,
        'today_record_exists': today_record_exists,
        'stats': monthly_stats,
        'prev_month': month - 1 if month > 1 else 12,
        'prev_year': year if month > 1 else year - 1,
        'next_month': month + 1 if month < 12 else 1,
        'next_year': year if month < 12 else year + 1,
        'today_str': today.strftime('%Y-%m-%d'),
        'current_month_str': date(year, month, 1).strftime('%B %Y'),
        'bot_running': bot_status.is_running if bot_status else False,
    }

    return render(request, 'trading/pnl_calendar.html', context)


@login_required
def algorithms_page(request):
    try:
        bot_status, _ = BotStatus.objects.get_or_create(user=request.user)

        algorithm_details = {
            'name': 'Hedged Short Strangle',
            'version': 'v9.8.3-fixed-v5',
            'description': 'NIFTY Weekly Options Trading Strategy',
            'underlying': 'NIFTY 50',
            'lot_size': 65,
            'entry_window': '09:21:00 - 09:26:00 IST',
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
            'Entry only in widened window 9:21–9:26',
            'VIX advisory range 6–35',
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
            'win_rate': calculate_overall_day_win_rate(request.user),
            'avg_daily_pnl': calculate_average_pnl(request.user),
            'best_day': get_best_day(request.user),
            'worst_day': get_worst_day(request.user),
            'current_streak': get_current_streak(request.user, 'win'),
            'current_loss_streak': get_current_streak(request.user, 'loss'),
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
            'max_lots_hard_cap': bot_status.max_lots_hard_cap or 0,
            'broker_ready': broker_ready,
            'broker': broker,
            'can_control_bot': request.user.is_staff,
        }

        return render(request, 'trading/algorithms.html', context)

    except Exception as e:
        messages.error(request, f"Error loading algorithms page: {str(e)}")
        LogEntry.objects.create(
            user=request.user,
            level='ERROR',
            message=f"Algorithms page failed: {str(e)}",
            details={'trace': traceback.format_exc()}
        )
        return render(request, 'trading/algorithms.html', {'error': str(e)})


# ───────────────────────────────────────────────
# Helper Functions
# ───────────────────────────────────────────────

def get_user_stats(user):
    today = date.today()
    week_ago = today - timedelta(days=7)
    month_start = date(today.year, today.month, 1)

    return {
        'daily_pnl': DailyPnL.objects.filter(user=user, date=today).first(),
        'weekly_pnl': DailyPnL.objects.filter(user=user, date__gte=week_ago).aggregate(total=Sum('pnl'))['total'] or 0,
        'monthly_pnl': DailyPnL.objects.filter(user=user, date__gte=month_start).aggregate(total=Sum('pnl'))['total'] or 0,
        'weekly_trades': Trade.objects.filter(user=user, entry_time__date__gte=week_ago, status='EXECUTED').count(),
        'monthly_trades': Trade.objects.filter(user=user, entry_time__date__gte=month_start, status='EXECUTED').count(),
    }


def calculate_average_pnl(user):
    qs = DailyPnL.objects.filter(user=user)
    count = qs.count()
    if count == 0:
        return 0.0
    total = qs.aggregate(total=Sum('pnl'))['total'] or 0
    return round(float(total) / count, 2)


def get_best_day(user):
    return DailyPnL.objects.filter(user=user).order_by('-pnl').first()


def get_worst_day(user):
    return DailyPnL.objects.filter(user=user).order_by('pnl').first()


def get_current_streak(user, streak_type='win'):
    streak = 0
    current = date.today()
    sign = 1 if streak_type == 'win' else -1

    while True:
        record = DailyPnL.objects.filter(user=user, date=current).first()
        if not record or record.pnl == 0:
            break
        if (record.pnl * sign) > 0:
            streak += 1
            current -= timedelta(days=1)
        else:
            break
    return streak


def get_zerodha_broker(user):
    return Broker.objects.filter(user=user, broker_name='ZERODHA').first()


def is_zerodha_ready(user):
    broker = get_zerodha_broker(user)
    return bool(broker and broker.access_token)


@login_required
@require_POST
@csrf_protect
def start_bot(request):
    user = request.user
    bot_status, _ = BotStatus.objects.get_or_create(user=user)

    if bot_status.is_running:
        return JsonResponse({
            'success': False,
            'message': 'Bot is already running. No new task launched.'
        }, status=400)

    try:
        with transaction.atomic():
            bot_status.refresh_from_db()
            if bot_status.is_running:
                return JsonResponse({
                    'success': False,
                    'message': 'Bot already running (race condition prevented).'
                }, status=409)

            # Launch task - let the TASK set is_running=True
            result = run_user_bot.delay(user.id)

            # Only save task ID and timestamps - do NOT set is_running here
            bot_status.celery_task_id = result.id
            bot_status.last_started = timezone.now()
            bot_status.last_error = None
            bot_status.save(update_fields=[
                'celery_task_id', 'last_started', 'last_error'
            ])

        LogEntry.objects.create(
            user=user,
            level='INFO',
            message='Bot start requested – Celery task launched',
            details={'task_id': result.id, 'time': str(timezone.now())}
        )

        return JsonResponse({
            'success': True,
            'message': f'Bot start requested. Task ID: {result.id[:8]}...',
            'task_id': result.id
        })

    except Exception as e:
        error_msg = f'Failed to start bot: {str(e)}'
        bot_status.last_error = error_msg[:500]
        bot_status.save(update_fields=['last_error'])

        LogEntry.objects.create(
            user=user,
            level='ERROR',
            message=error_msg,
            details={'action': 'start_bot_failed', 'trace': traceback.format_exc()[:1000]}
        )

        return JsonResponse({
            'success': False,
            'message': error_msg
        }, status=500)


@login_required
@require_POST
@csrf_protect
def stop_bot(request):
    user = request.user
    bot_status = get_object_or_404(BotStatus, user=user)

    if not bot_status.is_running:
        return JsonResponse({
            'success': False,
            'message': 'Bot is not running.'
        }, status=400)

    revoked = False
    if bot_status.celery_task_id:
        try:
            celery_app.control.revoke(
                bot_status.celery_task_id,
                terminate=True,
                signal='SIGTERM'
            )
            revoked = True
        except Exception as e:
            LogEntry.objects.create(
                user=user,
                level='WARNING',
                message=f'Failed to revoke task {bot_status.celery_task_id}: {str(e)}'
            )

    with transaction.atomic():
        bot_status.refresh_from_db()
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

    return JsonResponse({
        'success': True,
        'message': 'Stop signal sent. Bot should stop within 5–30 seconds.'
    })


@login_required
def bot_status(request):
    bot_status = BotStatus.objects.filter(user=request.user).first()
    now = timezone.now()

    heartbeat_ago = "Never"
    is_actually_alive = False

    if bot_status:
        if bot_status.last_heartbeat:
            delta = now - bot_status.last_heartbeat
            heartbeat_ago = f"{delta.total_seconds() // 60:.0f} min ago"
            is_actually_alive = bot_status.is_running and delta < timedelta(minutes=5)

    return JsonResponse({
        'is_running': is_actually_alive,
        'flag_set': bot_status.is_running if bot_status else False,
        'last_heartbeat_ago': heartbeat_ago,
        'last_started': bot_status.last_started.isoformat() if bot_status and bot_status.last_started else None,
        'last_stopped': bot_status.last_stopped.isoformat() if bot_status and bot_status.last_stopped else None,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0) if bot_status else 0.0,
        'current_margin': float(bot_status.current_margin or 0) if bot_status else 0.0,
        'daily_target': float(bot_status.daily_profit_target or 0) if bot_status else 0.0,
        'daily_stop_loss': float(bot_status.daily_stop_loss or 0) if bot_status else 0.0,
        'max_lots_hard_cap': bot_status.max_lots_hard_cap or 0 if bot_status else 0,
        'task_id': bot_status.celery_task_id or 'None' if bot_status else 'None',
        'last_error': bot_status.last_error or 'None' if bot_status else 'None'
    })


@login_required
@require_POST
@csrf_protect
def update_max_lots(request):
    try:
        data = json.loads(request.body)
        max_lots_str = data.get('max_lots_hard_cap')

        try:
            new_cap = int(max_lots_str)
            if not 1 <= new_cap <= 10:
                return JsonResponse({'success': False, 'error': 'Value must be between 1 and 10'}, status=400)
        except (ValueError, TypeError):
            return JsonResponse({'success': False, 'error': 'Invalid number format'}, status=400)

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
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def user_logout(request):
    logout(request)
    messages.info(request, 'You have been logged out.')
    return redirect('home')


@login_required
def strategies_list(request):
    context = {
        'strategies': [
            {
                'name': 'Short Strangle',
                'icon': 'fas fa-balance-scale',
                'status': 'active',
                'url_name': 'short_strangle_detail',
                'description': 'Sell OTM Call & Put options to collect premium. Profits from time decay in range-bound markets.',
            },
            {
                'name': 'Nifty Call Strategy',
                'icon': 'fas fa-chart-line',
                'status': 'coming_soon',
                'url_name': '',
                'description': 'Bullish directional strategy using ATM/ITM calls with defined risk. Perfect for trending markets.',
            },
            {
                'name': 'Nifty Put Strategy',
                'icon': 'fas fa-chart-bar',
                'status': 'coming_soon',
                'url_name': '',
                'description': 'Bearish directional strategy for downtrending markets. Defined risk with asymmetric reward.',
            },
            {
                'name': 'Positional Algo',
                'icon': 'fas fa-crosshairs',
                'status': 'coming_soon',
                'url_name': '',
                'description': 'Swing trading strategy holding positions for 3-10 days. Captures larger market moves.',
            },
            {
                'name': 'Intraday Algo',
                'icon': 'fas fa-bolt',
                'status': 'coming_soon',
                'url_name': '',
                'description': 'High-frequency intraday scalping. Multiple trades per day with quick profit booking.',
            },
       ]
    }
    return render(request, 'trading/strategies_list.html', context)


@login_required
def about_us(request):
    return render(request, 'trading/about.html')


@login_required
def coming_soon_placeholder(request):
    strategy_name = request.resolver_match.url_name.replace('_detail', '').replace('-', ' ').title()
    
    context = {
        'title': strategy_name,
        'description': "This powerful strategy is currently under active development. We're working hard to bring it to you soon!",
        'expected': "Expected release: Q3 2026"
    }
    return render(request, 'trading/coming_soon.html', context)


@login_required
def connect_zerodha(request):
    messages.info(request, "Zerodha connection is now managed on the Broker page.")
    return redirect('broker')


@login_required
def public_pnl_overview(request):
    return render(request, 'front/pnl.html')


@login_required
def public_algorithms(request):
    return render(request, 'front/algo.html')


@login_required
def public_brokers(request):
    return render(request, 'front/brok.html')


@login_required
def public_about(request):
    return render(request, 'front/about.html')