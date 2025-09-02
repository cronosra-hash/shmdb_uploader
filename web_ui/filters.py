# filters.py
from datetime import datetime
import pendulum

def datetimeformat(value, format="%Y-%m-%d %H:%M:%S"):
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.strftime(format)
    except Exception:
        return value

def ago(value):
    try:
        dt = pendulum.parse(value) if isinstance(value, str) else pendulum.instance(value)
        return dt.diff_for_humans()
    except Exception:
        return value

def to_timezone(value, tz="Europe/London"):
    try:
        dt = pendulum.parse(value) if isinstance(value, str) else pendulum.instance(value)
        return dt.in_timezone(tz).to_datetime_string()
    except Exception:
        return value

def timestamp_color(value):
    try:
        dt = pendulum.parse(value) if isinstance(value, str) else pendulum.instance(value)
        now = pendulum.now()
        diff = now.diff(dt).in_minutes()

        if diff < 5:
            return "color: #00FF00;"  # neon green
        elif diff < 60:
            return "color: #FFFF00;"  # yellow
        elif diff < 1440:
            return "color: #FFA500;"  # orange
        else:
            return "color: #FF4500;"  # red-orange
    except Exception:
        return "color: #999999;"  # fallback gray