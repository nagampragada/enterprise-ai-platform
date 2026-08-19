from datetime import datetime, timedelta, timezone

import pytest

from application.services.connector_sync_schedule_service import calculate_next_future
from infrastructure.repositories.connector_sync_schedule_repository import InvalidSyncScheduleRequest

NOW = datetime(2026, 8, 24, 17, 20, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("anchor", "interval", "expected"),
    (
        (NOW + timedelta(hours=1), 3600, NOW + timedelta(hours=1)),
        (NOW, 3600, NOW + timedelta(hours=1)),
        (NOW - timedelta(hours=1), 3600, NOW + timedelta(hours=1)),
        (datetime(2026, 8, 24, 13, tzinfo=timezone.utc), 3600, datetime(2026, 8, 24, 18, tzinfo=timezone.utc)),
        (datetime(2000, 1, 1, tzinfo=timezone.utc), 900, datetime(2026, 8, 24, 17, 30, tzinfo=timezone.utc)),
    ),
)
def test_next_future_is_constant_time_strict_and_anchor_aligned(anchor, interval, expected):
    assert calculate_next_future(anchor, interval, NOW) == expected


@pytest.mark.parametrize(
    "anchor,now,interval",
    (
        (datetime(2026, 1, 1), NOW, 3600),
        (NOW, datetime(2026, 1, 1), 3600),
        (NOW, NOW, 899),
        (NOW, NOW, 2_592_001),
    ),
)
def test_next_future_rejects_naive_time_and_invalid_interval(anchor, now, interval):
    with pytest.raises(InvalidSyncScheduleRequest):
        calculate_next_future(anchor, interval, now)