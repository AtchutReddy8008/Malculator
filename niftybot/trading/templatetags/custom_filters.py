# trading/templatetags/custom_filters.py
from django import template

register = template.Library()


@register.filter(name='dictlookup')
def dictlookup(dictionary, key):
    """
    Safely gets a value from a dictionary using a string key.
    Returns None if key doesn't exist or dictionary is invalid.
    
    Usage in template:
        {% with record=daily_records|dictlookup:day_date %}
            {% if record %}
                ... show record ...
            {% else %}
                ... show zero / no trade ...
            {% endif %}
        {% endwith %}
    """
    if not isinstance(dictionary, dict):
        return None
    return dictionary.get(key)