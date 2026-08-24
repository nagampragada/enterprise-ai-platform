from __future__ import annotations

from pathlib import Path


BACKEND = Path(__file__).resolve().parents[1]


def test_dockerfile_runs_common_image_as_non_root_without_secret_build_inputs() -> None:
    dockerfile = (BACKEND / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.12-slim-bookworm" in dockerfile
    assert 'USER 10001:10001' in dockerfile
    assert 'ENTRYPOINT ["python", "-m"]' in dockerfile
    assert 'CMD ["app.server"]' in dockerfile
    assert "ARG JWT" not in dockerfile
    assert "ARG DATABASE" not in dockerfile
    assert "ARG SECRET" not in dockerfile
    assert "alembic upgrade" not in dockerfile


def test_docker_build_context_excludes_sensitive_and_development_content() -> None:
    ignored = (BACKEND / ".dockerignore").read_text(encoding="utf-8").splitlines()
    required = {
        ".git", ".env", ".env.*", ".venv", "tests", "scripts", "__pycache__",
        ".pytest_cache", ".pytest-tmp-*", "pytest-tmp-*", "*.pem", "*.key",
        "*.db", "*.sqlite", "*.sqlite3", "credentials*.json",
        "service-account*.json", "service_account*.json",
    }
    assert required <= set(ignored)
    assert "!.env.example" not in ignored


def test_documented_image_command_modules_exist() -> None:
    assert (BACKEND / "src/app/server.py").is_file()
    assert (BACKEND / "src/infrastructure/workers/connector_sync_worker_host.py").is_file()
    assert (BACKEND / "src/infrastructure/workers/connector_sync_scheduler_host.py").is_file()
    assert (BACKEND / "src/infrastructure/bootstrap/sandbox.py").is_file()
    assert (BACKEND / "alembic.ini").is_file()
