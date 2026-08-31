from __future__ import annotations

import pytest

import infrastructure.db.health as db_health


class FakeResult:
    def __init__(self, revisions: tuple[str, ...]) -> None:
        self.revisions = revisions

    def scalars(self):
        return self

    def all(self) -> list[str]:
        return list(self.revisions)


class FakeConnection:
    def __init__(
        self,
        revisions: tuple[str, ...],
        should_fail: bool = False,
    ) -> None:
        self.revisions = revisions
        self.should_fail = should_fail
        self.executed = False
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        if self.should_fail:
            raise RuntimeError("database unavailable")
        self.executed = True
        self.statements.append(str(statement))
        return FakeResult(self.revisions)


class FakeEngine:
    def __init__(
        self,
        revisions: tuple[str, ...] = (db_health.EXPECTED_ALEMBIC_REVISION,),
        should_fail: bool = False,
    ) -> None:
        self.revisions = revisions
        self.should_fail = should_fail

    def connect(self):
        return FakeConnection(self.revisions, should_fail=self.should_fail)


@pytest.mark.parametrize(
    ("revision", "schema_current", "migration_required"),
    (
        ("20260828_000020", False, True),
        (db_health.EXPECTED_ALEMBIC_REVISION, True, False),
    ),
)
def test_known_transition_revisions_are_compatible(
    monkeypatch,
    revision: str,
    schema_current: bool,
    migration_required: bool,
) -> None:
    monkeypatch.setattr(db_health, "engine", FakeEngine((revision,)))

    result = db_health.check_database_connection()

    assert result.healthy is True
    assert result.schema_compatible is True
    assert result.schema_current is schema_current
    assert result.migration_required is migration_required
    assert result.message == "Database connection is healthy."


@pytest.mark.parametrize(
    "revisions",
    (
        ("20260801_000001",),
        ("20260901_000022",),
        ("not-an-alembic-revision",),
        (),
        ("20260828_000020", "20260831_000021"),
    ),
)
def test_unknown_missing_malformed_or_branched_revisions_are_incompatible(
    monkeypatch,
    revisions: tuple[str, ...],
) -> None:
    monkeypatch.setattr(db_health, "engine", FakeEngine(revisions))

    result = db_health.check_database_connection()

    assert result.healthy is True
    assert result.schema_compatible is False
    assert result.schema_current is False
    assert result.migration_required is False


def test_check_database_connection_reports_failure(monkeypatch) -> None:
    monkeypatch.setattr(db_health, "engine", FakeEngine(should_fail=True))

    result = db_health.check_database_connection()

    assert result.healthy is False
    assert result.schema_compatible is False
    assert result.schema_current is False
    assert result.migration_required is False
    assert result.message == "Database connection check failed."
