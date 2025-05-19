from django import template
import builtins

register = template.Library()

@register.filter
def divide(value, arg):
    """
    Divides the value by the argument
    """
    try:
        return float(value) / float(arg)
    except (ValueError, ZeroDivisionError):
        return 0 
    
@register.filter(name='abs')
def abs_filter(value):
    try:
        return builtins.abs(float(value))
    except (ValueError, TypeError) as e:
        return value