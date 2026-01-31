from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login, logout
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django.db.models import Sum, Count
from django.utils import timezone
from datetime import date, timedelta
import calendar
import requests
import pyotp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

# Celery control
from celery import current_app

from .models import Broker, Trade, DailyPnL, BotStatus, LogEntry
from .forms import SignUpForm, ZerodhaConnectionForm
from .tasks import run_user_bot  # the long-running task


# ────────────── HELPER: Auto-generate access_token after saving credentials ──────────────
def try_generate_access_token(broker: Broker) -> bool:
    """Automatically generate access_token after saving credentials in form"""
    try:
        kite = KiteConnect(api_key=broker.api_key)
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

        login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={broker.api_key}"
        resp = session.get(login_url, timeout=15)
        if resp.status_code != 200:
            raise Exception(f"Login page failed: {resp.status_code}")

        current_url = resp.url

        login_resp = session.post(
            "https://kite.zerodha.com/api/login",
            data={"user_id": broker.zerodha_user_id, "password": broker.password},
            timeout=15
        )
        login_data = login_resp.json()

        if login_data.get("status") != "success":
            raise Exception(f"Login failed: {login_data.get('message', login_data)}")

        request_id = login_data["data"]["request_id"]

        totp_code = pyotp.TOTP(broker.totp).now()

        twofa_resp = session.post(
            "https://kite.zerodha.com/api/twofa",
            data={
                "user_id": broker.zerodha_user_id,
                "request_id": request_id,
                "twofa_value": totp_code,
                "twofa_type": "totp"
            },
            timeout=15
        )
        twofa_data = twofa_resp.json()

        if twofa_data.get("status") != "success":
            raise Exception(f"2FA failed: {twofa_data.get('message', twofa_data)}")

        redirect_resp = session.get(current_url + "&skip_session=true", allow_redirects=True, timeout=15)
        final_url = redirect_resp.url

        parsed = urlparse(final_url)
        query = parse_qs(parsed.query)
        request_token = query.get("request_token", [None])[0]

        if not request_token:
            raise Exception(f"No request_token in redirect URL: {final_url}")

        data = kite.generate_session(request_token=request_token, api_secret=broker.secret_key)
        access_token = data.get("access_token")

        if not access_token:
            raise Exception("No access_token returned")

        kite.set_access_token(access_token)

        broker.access_token = access_token
        broker.token_generated_at = timezone.now()
        broker.save(update_fields=['access_token', 'token_generated_at'])

        return True

    except Exception as e:
        print(f"[AUTO AUTH FAILED] {str(e)}")
        return False


def home(request):
    if request.user.is_authenticated:
        return redirect('dashboard')

    context = {
        'total_users': 0,
        'total_trades': 0,
        'total_pnl': 0,
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

    has_broker = Broker.objects.filter(
        user=request.user, broker_name='ZERODHA', is_active=True
    ).exists()

    broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA').first()
    broker_ready = broker and broker.access_token  # Check if token exists

    context = {
        'bot_status': bot_status,
        'recent_trades': recent_trades,
        'daily_pnl': daily_pnl,
        'weekly_pnl': weekly_agg['total_pnl'] or 0,
        'weekly_trades': weekly_agg['total_trades'] or 0,
        'monthly_pnl': monthly_agg['total_pnl'] or 0,
        'monthly_trades': monthly_agg['total_trades'] or 0,
        'win_rate': round(win_rate, 2),
        'has_broker': has_broker,
        'broker_ready': broker_ready,
        'today': today,
        'daily_target': bot_status.daily_profit_target or 0,
        'daily_stop_loss': bot_status.daily_stop_loss or 0,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
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

            # Automatically try to generate access_token after save
            success = try_generate_access_token(broker_obj)
            if success:
                messages.success(request, 'Zerodha credentials saved and access token generated automatically!')
            else:
                messages.warning(
                    request,
                    'Credentials saved, but automatic token generation failed. '
                    'Check credentials/TOTP/clock sync or generate manually in admin.'
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

    cal = calendar.monthcalendar(year, month)
    pnl_dict = {r.date: r for r in daily_records}

    monthly_stats = {
        'total_pnl': sum(r.pnl for r in daily_records),
        'average_pnl': sum(r.pnl for r in daily_records) / len(daily_records) if daily_records else 0,
        'positive_days': sum(1 for r in daily_records if r.pnl > 0),
        'negative_days': sum(1 for r in daily_records if r.pnl < 0),
        'max_pnl': max((r.pnl for r in daily_records), default=0),
        'min_pnl': min((r.pnl for r in daily_records), default=0),
    }

    context = {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'calendar': cal,
        'pnl_dict': pnl_dict,
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
        'max_lots': 5,
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
        'Daily target: 2% of margin (high VIX) or net credit ÷ days',
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
    broker_ready = broker and broker.access_token

    context = {
        'bot_status': bot_status,
        'algorithm': algorithm_details,
        'strategy_rules': strategy_rules,
        'recent_logs': recent_logs,
        'performance': performance_stats,
        'daily_target': bot_status.daily_profit_target or 0,
        'daily_stop_loss': bot_status.daily_stop_loss or 0,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
        'broker_ready': broker_ready,
        'broker': broker,
    }
    return render(request, 'trading/algorithms.html', context)


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
def connect_zerodha(request):
    return redirect('broker')


@login_required
@require_POST
@csrf_protect
def start_bot(request):
    bot_status, created = BotStatus.objects.get_or_create(user=request.user)

    if bot_status.is_running and bot_status.celery_task_id:
        messages.warning(request, 'Bot appears to be already running (task ID exists).')
        return redirect('algorithms')

    broker = Broker.objects.filter(user=request.user, broker_name='ZERODHA', is_active=True).first()
    if not broker:
        messages.error(request, 'No Zerodha broker connection found. Please set up in Broker page.')
        return redirect('broker')

    # Check if access_token is present
    if not broker.access_token:
        messages.warning(
            request,
            'Zerodha access token missing. Credentials saved, but automatic generation failed. '
            'Check TOTP/clock sync or generate manually in admin.'
        )
        return redirect('broker')

    # Launch the long-running Celery task
    result = run_user_bot.delay(request.user.id)

    bot_status.is_running = True
    bot_status.celery_task_id = result.id
    bot_status.last_started = timezone.now()
    bot_status.last_error = None
    bot_status.save(update_fields=['is_running', 'celery_task_id', 'last_started', 'last_error'])

    LogEntry.objects.create(
        user=request.user,
        level='INFO',
        message='Bot started manually – Celery task launched',
        details={
            'action': 'start_bot',
            'task_id': result.id,
            'time': str(timezone.now())
        }
    )

    messages.success(request, 'Bot task launched! Monitoring Zerodha connection... (refresh in 10–20 sec)')
    return redirect('algorithms')


@login_required
@require_POST
@csrf_protect
def stop_bot(request):
    bot_status = get_object_or_404(BotStatus, user=request.user)

    if not bot_status.is_running:
        messages.warning(request, 'Bot is not running.')
        return redirect('algorithms')

    if bot_status.celery_task_id:
        try:
            current_app.control.revoke(
                bot_status.celery_task_id,
                terminate=True,
                signal='SIGTERM'
            )
            messages.info(request, 'Sent stop signal to running bot task.')
        except Exception as e:
            LogEntry.objects.create(
                user=request.user,
                level='WARNING',
                message=f'Failed to revoke Celery task {bot_status.celery_task_id}: {str(e)}'
            )
            messages.warning(request, 'Could not stop the background task cleanly.')

    bot_status.is_running = False
    bot_status.celery_task_id = None
    bot_status.last_stopped = timezone.now()
    bot_status.save(update_fields=['is_running', 'celery_task_id', 'last_stopped'])

    LogEntry.objects.create(
        user=request.user,
        level='INFO',
        message='Bot stopped manually',
        details={'action': 'stop_bot', 'time': str(timezone.now())}
    )

    messages.success(request, 'Bot stop requested. Refresh in 10–20 seconds to confirm.')
    return redirect('algorithms')


@login_required
def bot_status(request):
    bot_status = get_object_or_404(BotStatus, user=request.user)
    now = timezone.now()

    try:
        heartbeat_exists = hasattr(bot_status, 'last_heartbeat')
        is_actually_alive = (
            bot_status.is_running and
            heartbeat_exists and
            bot_status.last_heartbeat and
            (now - bot_status.last_heartbeat) < timedelta(minutes=5)
        )
        heartbeat_ago = (
            str(now - bot_status.last_heartbeat)
            if heartbeat_exists and bot_status.last_heartbeat
            else "Never"
        )
    except AttributeError:
        is_actually_alive = bot_status.is_running
        heartbeat_ago = "Field not available yet"

    return JsonResponse({
        'is_running': is_actually_alive,
        'flag_set': bot_status.is_running,
        'last_heartbeat_ago': heartbeat_ago,
        'last_started': bot_status.last_started.isoformat() if bot_status.last_started else None,
        'current_unrealized_pnl': float(bot_status.current_unrealized_pnl or 0),
        'current_margin': float(bot_status.current_margin or 0),
        'daily_target': float(bot_status.daily_profit_target or 0),
        'daily_stop_loss': float(bot_status.daily_stop_loss or 0),
        'status_message': 'Running' if is_actually_alive else 'Stopped',
    })


def signup(request):
    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, 'Account created! Welcome to NIFTY Trading Bot.')
            return redirect('dashboard')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = SignUpForm()
    return render(request, 'registration/signup.html', {'form': form})


@login_required
def user_logout(request):
    logout(request)
    messages.info(request, 'You have been logged out.')
    return redirect('home')