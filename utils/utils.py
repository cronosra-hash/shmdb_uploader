# shmdb_uploader/utils/utils.py

from datetime import datetime

def parse_date(date_str):
    """
    Safely parses a YYYY-MM-DD string into a date object.
    Returns None if parsing fails or input is invalid.
    """
    if isinstance(date_str, str):
        try:
            return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        except ValueError:
            return None
    return date_str
