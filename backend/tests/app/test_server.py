from __future__ import annotations

from pathlib import Path

import app.server as server
from app.config import ApiProcessSettings, Settings


def test_uvicorn_is_a_declared_importable_runtime_dependency() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    assert '"uvicorn>=0.30,<1.0"' in pyproject.read_text(encoding="utf-8")


def test_server_uses_fixed_safe_uvicorn_arguments(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        server,
        "validate_api_process_environment",
        lambda: ApiProcessSettings(
            Settings("sandbox", "database", "jwt", 15, "refresh"),
            None,
            None,
            8080,
        ),
    )
    monkeypatch.setattr(server, "_run_uvicorn", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert server.main([]) == 0
    assert calls == [
        (
            ("app.main:app",),
            {"host": "0.0.0.0", "port": 8080, "reload": False, "workers": 1},
        )
    ]
    assert server.main(["--port", "9999"]) == 1


def test_module_invocation_rejects_real_process_arguments(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(server.sys, "argv", ["app.server", "--unexpected"])
    monkeypatch.setattr(server, "_run_uvicorn", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert server.main() == 1
    assert calls == []
