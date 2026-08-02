from __future__ import annotations

import infrastructure.db.session as db_session


class FakeSession:
    def __init__(self, fail_on_commit: bool = False) -> None:
        self.fail_on_commit = fail_on_commit
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self) -> None:
        if self.fail_on_commit:
            raise RuntimeError("commit failed")
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_session_scope_commits_and_closes_on_success(monkeypatch) -> None:
    fake_session = FakeSession()
    monkeypatch.setattr(db_session, "SessionLocal", lambda: fake_session)

    with db_session.session_scope() as session:
        assert session is fake_session

    assert fake_session.committed is True
    assert fake_session.rolled_back is False
    assert fake_session.closed is True


def test_session_scope_rolls_back_and_closes_on_exception(monkeypatch) -> None:
    fake_session = FakeSession()
    monkeypatch.setattr(db_session, "SessionLocal", lambda: fake_session)

    try:
        with db_session.session_scope():
            raise RuntimeError("boom")
    except RuntimeError as exc:
        assert str(exc) == "boom"

    assert fake_session.committed is False
    assert fake_session.rolled_back is True
    assert fake_session.closed is True


def test_session_scope_rolls_back_when_commit_fails(monkeypatch) -> None:
    fake_session = FakeSession(fail_on_commit=True)
    monkeypatch.setattr(db_session, "SessionLocal", lambda: fake_session)

    try:
        with db_session.session_scope():
            pass
    except RuntimeError as exc:
        assert str(exc) == "commit failed"

    assert fake_session.committed is False
    assert fake_session.rolled_back is True
    assert fake_session.closed is True