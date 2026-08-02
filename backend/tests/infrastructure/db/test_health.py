from __future__ import annotations

from types import SimpleNamespace

import infrastructure.db.health as db_health


class FakeConnection:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.executed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        if self.should_fail:
            raise RuntimeError("database unavailable")
        self.executed = True
        return SimpleNamespace()


class FakeEngine:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail

    def connect(self):
        return FakeConnection(should_fail=self.should_fail)


def test_check_database_connection_reports_healthy(monkeypatch) -> None:
    monkeypatch.setattr(db_health, "engine", FakeEngine(should_fail=False))

    result = db_health.check_database_connection()

    assert result.healthy is True
    assert result.message == "Database connection is healthy."


def test_check_database_connection_reports_failure(monkeypatch) -> None:
    monkeypatch.setattr(db_health, "engine", FakeEngine(should_fail=True))

    result = db_health.check_database_connection()

    assert result.healthy is False
    assert result.message == "Database connection check failed."