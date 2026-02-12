# trading/templatetags/math_filters.py
from django import template
from decimal import Decimal, InvalidOperation, DivisionByZero

register = template.Library()


@register.filter(name='div')
def divide(value, arg):
    """
    Safely divides value by arg.
    Returns Decimal('0') if division by zero, invalid types, or None values.
    
    Usage:
        {{ total_pnl|div:total_trades }}
        {{ 15000|div:3 }} → 5000
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        divisor = Decimal(str(arg)) if arg is not None else Decimal('0')
        
        if divisor == 0:
            return Decimal('0')
            
        return val / divisor
    except (InvalidOperation, DivisionByZero, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='multiply')
def multiply(value, arg):
    """
    Safely multiplies value by arg.
    Returns Decimal('0') on invalid input.
    
    Usage:
        {{ win_rate|multiply:100 }} → turns 0.75 into 75
        {{ pnl_per_trade|multiply:lot_size }}
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        factor = Decimal(str(arg)) if arg is not None else Decimal('0')
        return val * factor
    except (InvalidOperation, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='sub')
def subtract(value, arg):
    """
    Safely subtracts arg from value.
    Returns Decimal('0') on invalid input.
    
    Usage:
        {{ gross_pnl|sub:brokerage }}
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        subtrahend = Decimal(str(arg)) if arg is not None else Decimal('0')
        return val - subtrahend
    except (InvalidOperation, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='abs')
def absolute(value):
    """
    Returns the absolute value as Decimal.
    
    Usage:
        {{ pnl|abs }}
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        return abs(val)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='percentage')
def percentage(value, total):
    """
    Calculates percentage: (value / total) * 100
    Returns Decimal('0') if total is 0 or invalid.
    
    Usage:
        {{ wins|percentage:total_trades }} → e.g. 42.50
        {{ wins|percentage:total_trades|floatformat:1 }} → 42.5
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        tot = Decimal(str(total)) if total is not None else Decimal('0')
        
        if tot == 0:
            return Decimal('0')
            
        return (val / tot) * Decimal('100')
    except (InvalidOperation, DivisionByZero, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='round_decimal')
def round_decimal(value, places=2):
    """
    Rounds a number to the specified decimal places (default 2).
    Very useful for displaying PnL values cleanly.
    
    Usage:
        {{ pnl|round_decimal:2 }} → 1234.56
        {{ pnl|round_decimal:0 }} → 1235
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        return val.quantize(Decimal('1.' + '0' * places))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal('0')


@register.filter(name='format_money')
def format_money(value, places=2, currency='₹'):
    """
    Formats a number as money with Indian Rupee symbol (or any currency).
    Adds commas as thousand separators.
    
    Usage:
        {{ pnl|format_money }} → ₹1,234.56
        {{ pnl|format_money:0 }} → ₹1,235
    """
    try:
        val = Decimal(str(value)) if value is not None else Decimal('0')
        rounded = val.quantize(Decimal('1.' + '0' * places))
        formatted = f"{rounded:,}"
        return f"{currency}{formatted}"
    except (InvalidOperation, TypeError, ValueError):
        return f"{currency}0"