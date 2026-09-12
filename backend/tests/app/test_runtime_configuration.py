from __future__ import annotations

from unittest.mock import Mock
from uuid import UUID

import pytest

import app.config as runtime_config

from app.config import (
    DEFAULT_API_PORT,
    DEVELOPMENT_DATABASE_URL,
    InvalidRuntimeConfiguration,
    load_api_port,
    load_application_settings,
    load_database_settings,
    load_github_processor_claim_target,
    validate_api_process_environment,
    validate_github_planner_process_environment,
    validate_migration_process_environment,
    validate_scheduler_process_environment,
    validate_worker_process_environment,
)


SAFE_DATABASE = "postgresql+psycopg://sandbox_user:S4ndboxDbValue9284@db.internal:5432/platform"
JWT_SECRET = "Sandbox-JWT-secret-value-1234567890!"
REFRESH_SECRET = "Sandbox-Refresh-secret-value-098765!"
TOKEN = "0123456789abcdef0123456789abcdef"
TARGET_VALUES = {
    "GITHUB_LEDGER_PLANNER_TARGET_ORGANIZATION_ID": "11111111-1111-4111-8111-111111111111",
    "GITHUB_LEDGER_PLANNER_TARGET_CONNECTOR_ID": "22222222-2222-4222-8222-222222222222",
    "GITHUB_LEDGER_PLANNER_TARGET_SCOPE_ID": "33333333-3333-4333-8333-333333333333",
    "GITHUB_LEDGER_PLANNER_TARGET_SYNC_JOB_ID": "44444444-4444-4444-8444-444444444444",
    "GITHUB_LEDGER_CONTROL_RESERVATION_ID": "55555555-5555-4555-8555-555555555555",
    "GITHUB_LEDGER_CONTROL_RESERVATION_OWNER_TOKEN": "A" * 43,
}
PROCESSOR_TARGET_VALUES = {
    "GITHUB_LEDGER_PROCESSOR_TARGET_ORGANIZATION_ID": "11111111-1111-4111-8111-111111111111",
    "GITHUB_LEDGER_PROCESSOR_TARGET_CONNECTOR_ID": "22222222-2222-4222-8222-222222222222",
    "GITHUB_LEDGER_PROCESSOR_TARGET_SCOPE_ID": "33333333-3333-4333-8333-333333333333",
    "GITHUB_LEDGER_PROCESSOR_TARGET_GENERATION_ID": "44444444-4444-4444-8444-444444444444",
    "GITHUB_LEDGER_PROCESSOR_TARGET_WORK_ITEM_ID": "66666666-6666-4666-8666-666666666666",
    "GITHUB_LEDGER_CONTROL_RESERVATION_ID": "55555555-5555-4555-8555-555555555555",
    "GITHUB_LEDGER_CONTROL_RESERVATION_OWNER_TOKEN": "A" * 43,
}


def _sandbox() -> dict[str, str]:
    return {
        "APP_ENVIRONMENT": "sandbox",
        "DATABASE_URL": SAFE_DATABASE,
        "JWT_SECRET_KEY": JWT_SECRET,
        "REFRESH_TOKEN_HASH_SECRET": REFRESH_SECRET,
        "PORT": "8080",
        "GITHUB_APP_ID": "12345",
        "GITHUB_APP_SLUG": "sandbox-app",
        "GITHUB_APP_CLIENT_ID": "Iv1.sandbox-client",
        "GITHUB_APP_CLIENT_SECRET_REFERENCE": (
            "gcp-secret-manager://projects/sandbox-proj-1/secrets/"
            f"eap-sm-{TOKEN}/versions/1"
        ),
        "GITHUB_APP_PRIVATE_KEY_REFERENCE": (
            "gcp-secret-manager://projects/sandbox-proj-1/secrets/"
            f"eap-sm-{TOKEN}/versions/2"
        ),
        "GITHUB_APP_CALLBACK_URL": (
            "https://sandbox-api.example.run.app/api/v1/connectors/github/callback"
        ),
        "GITHUB_APP_SETUP_URL": (
            "https://sandbox-api.example.run.app/api/v1/connectors/github/setup"
        ),
        "GCP_SECRET_MANAGER_PROJECT_ID": "sandbox-proj-1",
        "GCP_SECRET_MANAGER_SECRET_PREFIX": "eap",
        "GCP_SECRET_MANAGER_ENVIRONMENT": "sandbox",
        "OPENAI_API_KEY": "sk-sandbox-value-1234567890",
    }


def test_development_defaults_are_deliberate_and_api_composition_is_optional() -> None:
    settings = load_application_settings({"APP_ENVIRONMENT": "development"})
    assert settings.database_url == DEVELOPMENT_DATABASE_URL
    assert validate_api_process_environment({"APP_ENVIRONMENT": "development"}).port == DEFAULT_API_PORT


@pytest.mark.parametrize(
    "updates",
    (
        {"DATABASE_URL": None},
        {"DATABASE_URL": DEVELOPMENT_DATABASE_URL},
        {"JWT_SECRET_KEY": None},
        {"REFRESH_TOKEN_HASH_SECRET": None},
        {"REFRESH_TOKEN_HASH_SECRET": JWT_SECRET},
    ),
)
def test_sandbox_rejects_unsafe_core_configuration(updates) -> None:
    values = _sandbox()
    for name, value in updates.items():
        if value is None:
            values.pop(name)
        else:
            values[name] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        load_application_settings(values)


@pytest.mark.parametrize(
    "database_url",
    (
        "postgresql+psycopg://user:S4ndboxDbValue9284@database.invalid:5432/platform",
        "postgresql+psycopg://user:password@db.internal:5432/platform",
        "postgresql+psycopg://user:S4ndboxDbValue9284@placeholder-host:5432/platform",
    ),
)
def test_sandbox_rejects_placeholder_database_urls(database_url: str) -> None:
    values = _sandbox()
    values["DATABASE_URL"] = database_url
    with pytest.raises(InvalidRuntimeConfiguration):
        load_database_settings(values)


@pytest.mark.parametrize("value", ("0", "65536", "not-a-port", " 8080"))
def test_port_validation_rejects_malformed_or_out_of_range_values(value: str) -> None:
    values = _sandbox()
    values["PORT"] = value
    with pytest.raises(InvalidRuntimeConfiguration):
        load_api_port(values)


def test_safe_sandbox_api_and_worker_composition_passes_without_provider_calls() -> None:
    values = _sandbox()
    assert validate_api_process_environment(values).github is not None
    worker = validate_worker_process_environment(values)
    assert worker.github.private_key_reference.value.endswith("/versions/2")
    assert worker.github_sync_ledger_planning_enabled is False
    assert worker.github_sync_ledger_processing_enabled is False
    assert worker.github_sync_ledger_promotion_enabled is False
    assert worker.github_sync_ledger_reconciliation_enabled is False


@pytest.mark.parametrize(("value", "expected"), (("true", True), ("false", False)))
def test_worker_ledger_planning_flag_uses_strict_boolean_values(
    value: str, expected: bool
) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PLANNING_ENABLED"] = value
    assert (
        validate_worker_process_environment(values).github_sync_ledger_planning_enabled
        is expected
    )


@pytest.mark.parametrize("value", ("1", "TRUE", "False", " yes", "", "on"))
def test_worker_ledger_planning_flag_rejects_noncanonical_values(value: str) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PLANNING_ENABLED"] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_worker_process_environment(values)


@pytest.mark.parametrize(("value", "expected"), (("true", True), ("false", False)))
def test_worker_ledger_processing_flag_uses_strict_boolean_values(
    value: str, expected: bool
) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PROCESSING_ENABLED"] = value
    worker = validate_worker_process_environment(values)
    assert worker.github_sync_ledger_processing_enabled is expected
    assert worker.github_sync_ledger_planning_enabled is False


@pytest.mark.parametrize("value", ("1", "TRUE", "False", " yes", "", "on"))
def test_worker_ledger_processing_flag_rejects_noncanonical_values(value: str) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PROCESSING_ENABLED"] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_worker_process_environment(values)


@pytest.mark.parametrize(("value", "expected"), (("true", True), ("false", False)))
def test_worker_ledger_promotion_flag_uses_strict_boolean_values(
    value: str, expected: bool
) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PROMOTION_ENABLED"] = value
    worker = validate_worker_process_environment(values)
    assert worker.github_sync_ledger_promotion_enabled is expected
    assert worker.github_sync_ledger_processing_enabled is False


@pytest.mark.parametrize("value", ("1", "TRUE", "False", " yes", "", "on"))
def test_worker_ledger_promotion_flag_rejects_noncanonical_values(value: str) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_PROMOTION_ENABLED"] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_worker_process_environment(values)


@pytest.mark.parametrize(("value", "expected"), (("true", True), ("false", False)))
def test_worker_ledger_reconciliation_flag_uses_strict_boolean_values(
    value: str, expected: bool
) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED"] = value
    worker = validate_worker_process_environment(values)
    assert worker.github_sync_ledger_reconciliation_enabled is expected
    assert worker.github_sync_ledger_promotion_enabled is False


@pytest.mark.parametrize("value", ("1", "TRUE", "False", " yes", "", "on"))
def test_worker_ledger_reconciliation_flag_rejects_noncanonical_values(
    value: str,
) -> None:
    values = _sandbox()
    values["GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED"] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_worker_process_environment(values)


def test_api_and_worker_require_only_their_relevant_sandbox_inputs() -> None:
    values = _sandbox()
    values.pop("GITHUB_APP_CLIENT_SECRET_REFERENCE")
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_api_process_environment(values)
    assert validate_worker_process_environment(values).github.app_id == 12345

    values = _sandbox()
    values.pop("OPENAI_API_KEY")
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_worker_process_environment(values)

    planner = validate_github_planner_process_environment(values)
    assert planner.github_sync_ledger_planning_enabled is False


def test_github_planner_requires_no_openai_credential_and_parses_gate_strictly() -> None:
    values = _sandbox()
    values.pop("OPENAI_API_KEY")
    values.pop("GITHUB_APP_CLIENT_SECRET_REFERENCE")
    values["GITHUB_SYNC_LEDGER_PLANNING_ENABLED"] = "true"
    planner = validate_github_planner_process_environment(values)
    assert planner.github_sync_ledger_planning_enabled is True
    assert planner.github.private_key_reference.value.endswith("/versions/2")

    values["GITHUB_SYNC_LEDGER_PLANNING_ENABLED"] = "TRUE"
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_github_planner_process_environment(values)


def test_github_planner_target_is_optional_and_complete_tuple_is_canonical() -> None:
    values = _sandbox()
    assert validate_github_planner_process_environment(values).claim_target is None

    values.update(TARGET_VALUES)
    target = validate_github_planner_process_environment(values).claim_target
    assert target is not None
    assert target.organization_id == UUID(
        TARGET_VALUES["GITHUB_LEDGER_PLANNER_TARGET_ORGANIZATION_ID"]
    )
    assert target.connector_id == UUID(TARGET_VALUES["GITHUB_LEDGER_PLANNER_TARGET_CONNECTOR_ID"])
    assert target.connector_scope_id == UUID(
        TARGET_VALUES["GITHUB_LEDGER_PLANNER_TARGET_SCOPE_ID"]
    )
    assert target.sync_job_id == UUID(TARGET_VALUES["GITHUB_LEDGER_PLANNER_TARGET_SYNC_JOB_ID"])
    assert target.reservation_owner.reservation_id == UUID(
        TARGET_VALUES["GITHUB_LEDGER_CONTROL_RESERVATION_ID"]
    )


@pytest.mark.parametrize(
    "present_names",
    tuple(
        tuple(name for index, name in enumerate(TARGET_VALUES) if mask & (1 << index))
        for mask in range(1, (1 << len(TARGET_VALUES)) - 1)
    ),
)
def test_github_planner_target_rejects_every_partial_tuple(present_names) -> None:
    values = _sandbox()
    values.update({name: TARGET_VALUES[name] for name in present_names})
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_github_planner_process_environment(values)


@pytest.mark.parametrize("name", tuple(TARGET_VALUES)[:-1])
@pytest.mark.parametrize(
    "invalid_value",
    (
        "",
        " ",
        "not-a-uuid",
        "11111111111141118111111111111111",
        "{11111111-1111-4111-8111-111111111111}",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    ),
)
def test_github_planner_target_rejects_blank_malformed_and_noncanonical_values(
    name: str,
    invalid_value: str,
) -> None:
    values = _sandbox()
    values.update(TARGET_VALUES)
    values[name] = invalid_value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_github_planner_process_environment(values)


def test_github_planner_target_rejects_invalid_owner_capability() -> None:
    values = _sandbox()
    values.update(TARGET_VALUES)
    values["GITHUB_LEDGER_CONTROL_RESERVATION_OWNER_TOKEN"] = "short"
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_github_planner_process_environment(values)


def test_github_processor_target_is_all_or_none_and_canonical() -> None:
    assert load_github_processor_claim_target({}) is None
    target = load_github_processor_claim_target(PROCESSOR_TARGET_VALUES)
    assert target is not None
    assert str(target.organization_id) == PROCESSOR_TARGET_VALUES[
        "GITHUB_LEDGER_PROCESSOR_TARGET_ORGANIZATION_ID"
    ]
    assert str(target.work_item_id) == PROCESSOR_TARGET_VALUES[
        "GITHUB_LEDGER_PROCESSOR_TARGET_WORK_ITEM_ID"
    ]
    assert target.reservation_owner.owner_token_hash == (
        "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
    )


@pytest.mark.parametrize("missing", tuple(PROCESSOR_TARGET_VALUES))
def test_github_processor_target_rejects_every_partial_tuple(missing: str) -> None:
    values = dict(PROCESSOR_TARGET_VALUES)
    values.pop(missing)
    with pytest.raises(InvalidRuntimeConfiguration):
        load_github_processor_claim_target(values)


def test_invalid_planner_target_fails_before_other_configuration_loaders(monkeypatch) -> None:
    values = {"GITHUB_LEDGER_PLANNER_TARGET_ORGANIZATION_ID": "not-a-uuid"}
    database_loader = Mock(side_effect=AssertionError("database loader was called"))
    github_loader = Mock(side_effect=AssertionError("GitHub loader was called"))
    monkeypatch.setattr(runtime_config, "load_database_settings", database_loader)
    monkeypatch.setattr(
        runtime_config,
        "load_github_worker_settings_from_environment",
        github_loader,
    )
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        validate_github_planner_process_environment(values)
    database_loader.assert_not_called()
    github_loader.assert_not_called()


def test_planner_target_variables_are_ignored_by_api_and_general_worker() -> None:
    values = _sandbox()
    values["GITHUB_LEDGER_PLANNER_TARGET_ORGANIZATION_ID"] = "planner-only-invalid"

    assert validate_api_process_environment(values).port == 8080
    worker = validate_worker_process_environment(values)
    assert worker.github_sync_ledger_planning_enabled is False


def test_scheduler_and_migration_require_database_only() -> None:
    values = {"APP_ENVIRONMENT": "sandbox", "DATABASE_URL": SAFE_DATABASE}
    assert validate_scheduler_process_environment(values).database_url == SAFE_DATABASE
    assert validate_migration_process_environment(values).database_url == SAFE_DATABASE
    assert load_database_settings(values).runtime_environment == "sandbox"


def test_worker_only_ledger_flag_is_ignored_by_other_process_validators() -> None:
    api_values = _sandbox()
    api_values["GITHUB_SYNC_LEDGER_PLANNING_ENABLED"] = "not-a-worker-boolean"
    api_values["GITHUB_SYNC_LEDGER_PROCESSING_ENABLED"] = "not-a-worker-boolean"
    api_values["GITHUB_SYNC_LEDGER_PROMOTION_ENABLED"] = "not-a-worker-boolean"
    api_values["GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED"] = "not-a-worker-boolean"
    assert validate_api_process_environment(api_values).port == 8080

    database_values = {
        "APP_ENVIRONMENT": "sandbox",
        "DATABASE_URL": SAFE_DATABASE,
        "GITHUB_SYNC_LEDGER_PLANNING_ENABLED": "not-a-worker-boolean",
        "GITHUB_SYNC_LEDGER_PROCESSING_ENABLED": "not-a-worker-boolean",
        "GITHUB_SYNC_LEDGER_PROMOTION_ENABLED": "not-a-worker-boolean",
        "GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED": "not-a-worker-boolean",
    }
    assert (
        validate_scheduler_process_environment(database_values).database_url
        == SAFE_DATABASE
    )
    assert (
        validate_migration_process_environment(database_values).database_url
        == SAFE_DATABASE
    )
