# pyflink/rules.py
def check_velocity(
    recent_timestamps_ms: list[int],
    current_ts_ms: int,
    window_seconds: int,
    threshold: int,
):
    """Returns (is_violation, updated_timestamp_list)."""
    window_start = current_ts_ms - (window_seconds * 1000)
    recent = [ts for ts in recent_timestamps_ms if ts >= window_start] + [current_ts_ms]
    return len(recent) >= threshold, recent


def check_amount_anomaly(
    running_avg: float | None, amount: float, use_chip: str, multiplier: float
) -> bool:
    """running_avg=None means no history yet - can never flag (documented limitation)."""
    if running_avg is None:
        return False
    return use_chip == "Online Transaction" and amount > multiplier * running_avg
