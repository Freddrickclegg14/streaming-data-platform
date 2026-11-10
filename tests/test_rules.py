# tests/test_rules.py
from flink_jobs.rules import check_amount_anomaly, check_velocity


def test_velocity_violation_triggers_at_threshold():
    base = 1_000_000
    timestamps = [base, base + 60_000, base + 120_000, base + 180_000]
    is_violation, recent = check_velocity(
        timestamps, base + 240_000, window_seconds=600, threshold=5
    )
    assert is_violation is True
    assert len(recent) == 5


def test_velocity_prunes_old_timestamps_outside_window():
    old_ts = 0
    is_violation, recent = check_velocity(
        [old_ts], current_ts_ms=10_000_000, window_seconds=600, threshold=2
    )
    assert old_ts not in recent
    assert is_violation is False  # only the current event remains


def test_amount_anomaly_flags_large_online_purchase():
    assert (
        check_amount_anomaly(
            running_avg=50.0,
            amount=150.0,
            use_chip="Online Transaction",
            multiplier=2.0,
        )
        is True
    )


def test_amount_anomaly_ignores_non_online():
    assert (
        check_amount_anomaly(
            running_avg=50.0, amount=150.0, use_chip="Swipe Transaction", multiplier=2.0
        )
        is False
    )


def test_amount_anomaly_no_history_never_flags():
    assert (
        check_amount_anomaly(
            running_avg=None,
            amount=10_000.0,
            use_chip="Online Transaction",
            multiplier=2.0,
        )
        is False
    )
