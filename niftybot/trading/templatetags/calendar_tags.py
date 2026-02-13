# trading/templatetags/calendar_tags.py
from django import template
from datetime import datetime, timedelta

register = template.Library()


@register.filter(name='add_days')
def add_days(date_str, days):
    """
    Adds days to a date string in 'YYYY-MM-DD' format.
    Used to calculate exact date for monthcalendar day numbers.
    
    Usage:
        {{ '2025-02-01'|add_days:5 }} → '2025-02-06'
    """
    try:
        dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        new_dt = dt + timedelta(days=int(days) - 1)  # monthcalendar days start from 1
        return new_dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return date_str  # fallback to original if invalid