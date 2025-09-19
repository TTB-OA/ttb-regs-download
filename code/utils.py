from datetime import datetime, timezone, timedelta

def get_standard_timestamp():
    """
    Get a standardized timestamp string in EST with seconds precision.
    
    Returns:
        str: ISO formatted timestamp string in EST timezone with seconds precision.
             Format: "YYYY-MM-DD HH:MM:SS-05:00"
    
    Example:
        >>> get_standard_timestamp()
        '2025-07-13 14:30:45-05:00'
    """
    # EST is UTC-5 (UTC-4 during daylight saving time, but we'll use standard EST)
    est_timezone = timezone(timedelta(hours=-5))
    return datetime.now(est_timezone).isoformat(sep=" ", timespec="seconds")
