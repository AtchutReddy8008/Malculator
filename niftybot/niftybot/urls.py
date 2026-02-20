# niftybot/urls.py

from django.contrib import admin
from django.urls import path
from django.shortcuts import redirect                  # ← this was missing → fixes NameError

from django.contrib.auth import views as auth_views
from trading import views
from trading.views import AuthView                     # ← import your combined view

urlpatterns = [
    # ───────────────────────────────────────────────
    # Admin
    # ───────────────────────────────────────────────
    path('admin/', admin.site.urls),

    # ───────────────────────────────────────────────
    # Combined Auth (Login + Signup single page)
    # ───────────────────────────────────────────────
    path('auth/', AuthView.as_view(), name='auth'),

    # Redirect legacy /login and /signup to the new combined page
    path('login/', lambda request: redirect('/auth/?mode=login'), name='login'),
    path('signup/', lambda request: redirect('/auth/?mode=signup'), name='signup'),

    # Logout
    # Use your custom view instead of built-in
    path('logout/', views.user_logout, name='logout'),
    # If you prefer your custom logout view instead:
    # path('logout/', views.user_logout, name='logout'),

    # ───────────────────────────────────────────────
    # Public / Landing
    # ───────────────────────────────────────────────
    path('', views.home, name='home'),

    # ───────────────────────────────────────────────
    # Main Protected Pages
    # ───────────────────────────────────────────────
    path('dashboard/', views.dashboard, name='dashboard'),
    path('broker/', views.broker_page, name='broker'),
    path('pnl-calendar/', views.pnl_calendar, name='pnl_calendar'),

    # Bot control endpoints (AJAX/POST)
    path('start-bot/', views.start_bot, name='start_bot'),
    path('stop-bot/', views.stop_bot, name='stop_bot'),
    path('bot-status/', views.bot_status, name='bot_status'),
    path('update-max-lots/', views.update_max_lots, name='update_max_lots'),
    path('dashboard-stats/', views.dashboard_stats, name='dashboard_stats'),

    # ───────────────────────────────────────────────
    # Strategies & Info Pages
    # ───────────────────────────────────────────────
    path('strategies/', views.strategies_list, name='strategies_list'),

    # Active strategy detail
    path('strategies/short-strangle/', views.algorithms_page, name='short_strangle_detail'),

    # Coming soon placeholders
    path('strategies/delta-btcusd/', views.coming_soon_placeholder, name='delta_btcusd_detail'),
    path('strategies/nifty-buy/', views.coming_soon_placeholder, name='nifty_buy_detail'),
    path('strategies/nifty-sell/', views.coming_soon_placeholder, name='nifty_sell_detail'),

    path('about/', views.about_us, name='about'),

    # Optional legacy route (redirects to broker page now)
    path('connect-zerodha/', views.connect_zerodha, name='connect_zerodha'),
]

# # niftybot/urls.py

# from django.contrib import admin
# from django.urls import path
# from django.contrib.auth import views as auth_views
# from trading import views

# urlpatterns = [
#     # ───────────────────────────────────────────────
#     # Admin & Auth
#     # ───────────────────────────────────────────────
#     path('admin/', admin.site.urls),

#     # Public / Auth pages
#     path('', views.home, name='home'),
#     path('signup/', views.signup, name='signup'),
#     path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
#     path('logout/', auth_views.LogoutView.as_view(next_page='home'), name='logout'),

#     # ───────────────────────────────────────────────
#     # Main App Pages (login required - enforced in views)
#     # ───────────────────────────────────────────────
#     path('dashboard/', views.dashboard, name='dashboard'),
#     path('broker/', views.broker_page, name='broker'),                  # ← Fixed: uses broker_page (existing function)
#     path('pnl-calendar/', views.pnl_calendar, name='pnl_calendar'),

#     # Bot control endpoints (POST only, CSRF protected in views)
#     path('start-bot/', views.start_bot, name='start_bot'),
#     path('stop-bot/', views.stop_bot, name='stop_bot'),
#     path('bot-status/', views.bot_status, name='bot_status'),
#     path('update-max-lots/', views.update_max_lots, name='update_max_lots'),
#     path('dashboard-stats/', views.dashboard_stats, name='dashboard_stats'),

#     # ───────────────────────────────────────────────
#     # Strategies Section
#     # ───────────────────────────────────────────────
#     # Main Strategies overview page (shows 4 cards)
#     path('strategies/', views.strategies_list, name='strategies_list'),
#     path('about/', views.about_us, name='about'),

#     # Short Strangle – full detailed control page
#     path('strategies/short-strangle/', views.algorithms_page, name='short_strangle_detail'),

#     # Future strategies – placeholder pages
#     path('strategies/delta-btcusd/', views.coming_soon_placeholder, name='delta_btcusd_detail'),
#     path('strategies/nifty-buy/', views.coming_soon_placeholder, name='nifty_buy_detail'),
#     path('strategies/nifty-sell/', views.coming_soon_placeholder, name='nifty_sell_detail'),
# ]