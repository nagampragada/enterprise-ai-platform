from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from domain.connectors.sync_work_ledger import FileWorkCounters
import infrastructure.workers.github_sync_ledger_worker_host as host_module
from infrastructure.workers.github_sync_ledger_worker_host import (
    GitHubLedgerHostExitCode,
    GitHubLedgerShutdownState,
    GitHubLedgerWorkerSettings,
    GitHubSyncLedgerWorkerHost,
)
from infrastructure.workers.github_sync_work_item_worker import (
    GitHubFileWorkExecution,
)


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class ScriptedWorker:
    def __init__(
        self,
        outcomes,
        *,
        clock=None,
        advance=0.0,
        on_execute=None,
        reason_code=None,
    ):
        self.outcomes = list(outcomes)
        self.calls = 0
        self.clock = clock
        self.advance = advance
        self.on_execute = on_execute
        self.reason_code = reason_code

    def execute_one_result(self, *, claim_allowed):
        self.calls += 1
        if not claim_allowed():
            return GitHubFileWorkExecution("no_work")
        if self.on_execute is not None:
            self.on_execute()
        if self.clock is not None:
            self.clock.value += self.advance
        outcome = self.outcomes.pop(0)
        if outcome == "no_work":
            return GitHubFileWorkExecution(outcome)
        counters = (
            FileWorkCounters(86, 86, 1, 1)
            if outcome == "completed"
            else FileWorkCounters()
        )
        return GitHubFileWorkExecution(
            outcome,
            uuid4(),
            1,
            counters,
            self.reason_code,
        )


def _settings(**overrides):
    values = {
        "processing_enabled": True,
        "worker_id": "github-ledger-test",
        "max_items_per_execution": 3,
        "max_execution_duration": timedelta(seconds=100),
        "minimum_claim_time": timedelta(seconds=20),
        "lease_duration": timedelta(seconds=60),
        "heartbeat_interval": timedelta(seconds=5),
        "idle_interval": timedelta(seconds=1),
        "empty_poll_limit": 1,
        "shutdown_timeout": timedelta(seconds=30),
        "recovery_limit": 10,
        "provider_call_timeout": timedelta(seconds=5),
    }
    values.update(overrides)
    return GitHubLedgerWorkerSettings(**values)


def test_defaults_are_conservative_disabled_and_bounded():
    settings = GitHubLedgerWorkerSettings.from_environment(
        [], {}, processing_enabled=False
    )

    assert settings.processing_enabled is False
    assert settings.max_items_per_execution == 25
    assert settings.max_execution_duration == timedelta(minutes=20)
    assert settings.minimum_claim_time == timedelta(minutes=12)
    assert settings.lease_duration == timedelta(minutes=15)
    assert settings.heartbeat_interval == timedelta(minutes=1)
    assert settings.idle_interval == timedelta(seconds=5)
    assert settings.empty_poll_limit == 1
    assert settings.shutdown_timeout == timedelta(minutes=5)
    assert settings.recovery_limit == 10


@pytest.mark.parametrize(
    ("name", "value"),
    (
        ("GITHUB_LEDGER_WORKER_MAX_ITEMS_PER_EXECUTION", "0"),
        ("GITHUB_LEDGER_WORKER_MAX_EXECUTION_SECONDS", "-1"),
        ("GITHUB_LEDGER_WORKER_MINIMUM_CLAIM_SECONDS", "1.5"),
        ("GITHUB_LEDGER_WORKER_LEASE_SECONDS", "3601"),
        ("GITHUB_LEDGER_WORKER_HEARTBEAT_SECONDS", "0"),
        ("GITHUB_LEDGER_WORKER_IDLE_SECONDS", " 5"),
        ("GITHUB_LEDGER_WORKER_EMPTY_POLL_LIMIT", "101"),
        ("GITHUB_LEDGER_WORKER_SHUTDOWN_SECONDS", "901"),
        ("GITHUB_LEDGER_WORKER_RECOVERY_LIMIT", "501"),
        ("GITHUB_LEDGER_WORKER_ID", "unsafe worker"),
    ),
)
def test_environment_rejects_invalid_or_unbounded_values(name, value):
    environment = {name: value}
    with pytest.raises(ValueError):
        GitHubLedgerWorkerSettings.from_environment(
            [], environment, processing_enabled=True
        )


@pytest.mark.parametrize(
    "environment",
    (
        {
            "GITHUB_LEDGER_WORKER_LEASE_SECONDS": "120",
            "GITHUB_LEDGER_WORKER_HEARTBEAT_SECONDS": "60",
        },
        {
            "GITHUB_LEDGER_WORKER_MAX_EXECUTION_SECONDS": "120",
            "GITHUB_LEDGER_WORKER_MINIMUM_CLAIM_SECONDS": "121",
        },
        {
            "GITHUB_LEDGER_WORKER_MINIMUM_CLAIM_SECONDS": "719",
            "GITHUB_LEDGER_WORKER_HEARTBEAT_SECONDS": "60",
        },
        {
            "GITHUB_LEDGER_WORKER_LEASE_SECONDS": "120",
            "GITHUB_LEDGER_WORKER_SHUTDOWN_SECONDS": "121",
        },
    ),
)
def test_cross_field_timing_invariants_fail_closed(environment):
    with pytest.raises(ValueError):
        GitHubLedgerWorkerSettings.from_environment(
            [], environment, processing_enabled=True
        )


def test_explicit_cli_options_override_environment_deterministically():
    settings = GitHubLedgerWorkerSettings.from_environment(
        [
            "--worker-id",
            "cli-worker",
            "--max-items",
            "7",
            "--max-seconds",
            "900",
            "--minimum-claim-seconds",
            "720",
        ],
        {
            "GITHUB_LEDGER_WORKER_ID": "environment-worker",
            "GITHUB_LEDGER_WORKER_MAX_ITEMS_PER_EXECUTION": "5",
            "GITHUB_LEDGER_WORKER_MAX_EXECUTION_SECONDS": "900",
        },
        processing_enabled=True,
    )

    assert settings.worker_id == "cli-worker"
    assert settings.max_items_per_execution == 7
    assert settings.max_execution_duration == timedelta(minutes=15)
    assert settings.minimum_claim_time == timedelta(minutes=12)


def test_disabled_host_is_successful_noop_with_structured_outcome():
    worker = Mock()
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(processing_enabled=False),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "disabled-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.DISABLED
    worker.execute_one_result.assert_not_called()
    assert host.last_summary.stop_reason == "disabled"
    assert host.last_summary.empty_queue is False
    assert host.last_summary.run_status == "disabled"
    assert host.last_summary.graceful_shutdown is False
    assert host.last_summary.partial_drain is False
    assert host.last_summary.exit_code == 0


def test_item_bound_stops_before_another_claim_and_summarizes_counters():
    worker = ScriptedWorker(["completed", "completed", "completed"])
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(max_items_per_execution=2),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "bounded-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.CLEAN_DRAIN
    assert worker.calls == 2
    assert host.last_summary.items_claimed == 2
    assert host.last_summary.succeeded == 2
    assert host.last_summary.downloaded_bytes == 172
    assert host.last_summary.extracted_characters == 172
    assert host.last_summary.chunks == 2
    assert host.last_summary.embedding_batches == 2
    assert host.last_summary.stop_reason == "item_limit"
    assert host.last_summary.run_status == "partial"
    assert host.last_summary.partial_drain is True


def test_runtime_bound_prevents_new_claim_but_allows_active_item_to_finish():
    clock = Clock()
    worker = ScriptedWorker(["completed", "completed"], clock=clock, advance=81)
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        monotonic=clock,
        run_id_factory=lambda: "deadline-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.CLEAN_DRAIN
    assert worker.calls == 1
    assert host.last_summary.succeeded == 1
    assert host.last_summary.stop_reason == "runtime_limit"
    assert host.last_summary.duration_seconds == 81


def test_exact_runtime_claim_boundary_prevents_claim():
    monotonic = Mock(side_effect=(0.0, 80.0, 80.0))
    worker = Mock()
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        monotonic=monotonic,
        run_id_factory=lambda: "exact-boundary-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.SUCCESS
    worker.execute_one_result.assert_not_called()
    assert host.last_summary.stop_reason == "runtime_limit"


def test_empty_poll_wait_never_crosses_runtime_claim_boundary():
    clock = Clock()
    worker = ScriptedWorker(["no_work"], clock=clock, advance=79.5)

    def wait(timeout):
        clock.value += timeout
        return False

    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(empty_poll_limit=2),
        monotonic=clock,
        wait=Mock(side_effect=wait),
        run_id_factory=lambda: "bounded-poll-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.SUCCESS
    assert worker.calls == 1
    host._wait.assert_called_once_with(0.5)
    assert host.last_summary.stop_reason == "runtime_limit"


def test_empty_queue_waits_bounded_number_of_times_without_busy_loop():
    worker = ScriptedWorker(["no_work", "no_work"])
    wait = Mock(return_value=False)
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(empty_poll_limit=2),
        monotonic=lambda: 0.0,
        wait=wait,
        run_id_factory=lambda: "empty-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.EMPTY_QUEUE
    assert worker.calls == 2
    wait.assert_called_once_with(1.0)
    assert host.last_summary.empty_queue is True
    assert host.last_summary.stop_reason == "empty_queue"
    assert host.last_summary.run_status == "succeeded"
    assert host.last_summary.exit_code == 0


def test_shutdown_before_claim_is_distinct_and_does_not_call_worker():
    shutdown = __import__("threading").Event()
    shutdown.set()
    worker = Mock()
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        shutdown_event=shutdown,
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "shutdown-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.SHUTDOWN
    worker.execute_one_result.assert_not_called()
    assert host.last_summary.stop_reason == "shutdown_requested"
    assert host.last_summary.run_status == "stopped"
    assert host.last_summary.graceful_shutdown is True
    assert host.last_summary.exit_code == 0


def test_shutdown_during_processing_finishes_active_item_and_claims_no_more():
    shutdown = __import__("threading").Event()
    worker = ScriptedWorker(
        ["completed", "completed"], on_execute=shutdown.set
    )
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        shutdown_event=shutdown,
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "graceful-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.SHUTDOWN
    assert worker.calls == 1
    assert host.last_summary.succeeded == 1
    assert host.last_summary.stop_reason == "shutdown_requested"
    assert host.last_summary.graceful_shutdown is True


def test_shutdown_state_enforces_monotonic_grace_window_at_safe_boundary():
    clock = Clock()
    shutdown = GitHubLedgerShutdownState(
        timedelta(seconds=5), monotonic=clock
    )
    shutdown.set()
    clock.value = 2.0
    shutdown.set()
    clock.value = 4.999
    shutdown.raise_if_grace_expired()
    clock.value = 5.0

    with pytest.raises(host_module.FileWorkGracefulShutdownExpired):
        shutdown.raise_if_grace_expired()


def test_grace_expiry_retry_transition_reports_shutdown_exit():
    shutdown = __import__("threading").Event()
    worker = ScriptedWorker(
        ["retry_scheduled"],
        on_execute=shutdown.set,
        reason_code="shutdown_grace_expired",
    )
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        shutdown_event=shutdown,
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "expired-grace-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.SHUTDOWN
    assert host.last_summary.retry_scheduled == 1
    assert host.last_summary.stop_reason == "shutdown_grace_expired"
    assert host.last_summary.run_status == "retry_scheduled"
    assert host.last_summary.graceful_shutdown is True
    assert host.last_summary.exit_code == 0


def test_fence_loss_during_shutdown_remains_a_true_process_failure():
    shutdown = __import__("threading").Event()
    worker = ScriptedWorker(["lost_lease"], on_execute=shutdown.set)
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        shutdown_event=shutdown,
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "shutdown-fence-loss-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.FAILURE
    assert host.last_summary.stop_reason == "lease_or_fence_lost"
    assert host.last_summary.run_status == "failed"
    assert host.last_summary.graceful_shutdown is True
    assert host.last_summary.lease_or_fence_lost == 1


@pytest.mark.parametrize(
    ("outcome", "exit_code", "field", "stop_reason"),
    (
        (
            "retry_scheduled",
            GitHubLedgerHostExitCode.RETRY_SCHEDULED,
            "retry_scheduled",
            "retry_scheduled",
        ),
        (
            "lost_lease",
            GitHubLedgerHostExitCode.LEASE_OR_FENCE_LOST,
            "lease_or_fence_lost",
            "lease_or_fence_lost",
        ),
        (
            "failed",
            GitHubLedgerHostExitCode.ITEM_FAILED,
            "failed",
            "item_failed",
        ),
    ),
)
def test_stop_outcomes_are_deterministic(outcome, exit_code, field, stop_reason):
    worker = ScriptedWorker([outcome, "completed"])
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "failure-run",
    )

    assert host.run() == exit_code
    assert worker.calls == 1
    assert getattr(host.last_summary, field) == 1
    assert host.last_summary.stop_reason == stop_reason


def test_terminal_quarantine_and_cancellation_do_not_block_other_items():
    worker = ScriptedWorker(
        ["quarantined", "cancelled", "completed", "no_work"]
    )
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(max_items_per_execution=4),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "terminal-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.CLEAN_DRAIN
    assert host.last_summary.quarantined == 1
    assert host.last_summary.cancelled == 1
    assert host.last_summary.succeeded == 1
    assert host.last_summary.stop_reason == "queue_drained"


def test_summary_and_item_logs_are_structured_and_do_not_include_payloads():
    logger = Mock()
    worker = ScriptedWorker(["completed"])
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(max_items_per_execution=1),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "telemetry-run",
        logger=logger,
    )

    assert host.run() == GitHubLedgerHostExitCode.CLEAN_DRAIN
    rendered = "\n".join(
        call.args[0] % call.args[1:] for call in logger.info.call_args_list
    )
    assert "event=github_ledger_item_finished" in rendered
    assert "event=github_ledger_execution_summary" in rendered
    assert "run_id=telemetry-run" in rendered
    assert "downloaded_bytes=86" in rendered
    assert "run_status=partial" in rendered
    assert "graceful_shutdown=false" in rendered
    assert "partial_drain=true" in rendered
    for forbidden in (
        "DATABASE_URL",
        "postgresql://",
        "Authorization",
        "token",
        "chunk text",
        "vector",
        "provider payload",
    ):
        assert forbidden not in rendered


def test_main_disabled_does_not_compose_ledger_services(monkeypatch):
    process = SimpleNamespace(github_sync_ledger_processing_enabled=False)
    monkeypatch.setattr(
        host_module, "validate_worker_process_environment", Mock(return_value=process)
    )
    compose = Mock()
    monkeypatch.setattr(host_module, "compose_github_sync_ledger_worker_host", compose)
    monkeypatch.setattr(host_module, "install_shutdown_signal_handlers", Mock())

    assert host_module.main([]) == GitHubLedgerHostExitCode.DISABLED
    compose.assert_not_called()


def test_dedicated_composition_builds_only_file_work_processor_when_enabled(
    monkeypatch,
):
    process = SimpleNamespace(
        github_sync_ledger_processing_enabled=True,
        secret_manager=Mock(),
        github=Mock(),
    )
    preparation = Mock()
    preparation.profile.fingerprint = "github:profile"
    monkeypatch.setattr(
        host_module,
        "GitHubSynchronizationPreparationService",
        Mock(return_value=preparation),
    )
    for name in (
        "OpenAI",
        "GoogleSecretManagerSecretStore",
        "GitHubAppRestClient",
        "OpenAIEmbeddingProvider",
        "create_default_content_extractor_registry",
        "DeterministicTextChunker",
        "GitHubRepositoryContentService",
        "ConnectorSyncRetryPolicy",
        "GitHubSyncWorkItemWorker",
    ):
        monkeypatch.setattr(host_module, name, Mock())

    host = host_module.compose_github_sync_ledger_worker_host(
        _settings(), process_settings=process, session_factory=Mock()
    )

    assert isinstance(host, GitHubSyncLedgerWorkerHost)
    host_module.GitHubSyncWorkItemWorker.assert_called_once()
    host_module.OpenAI.assert_called_once_with(timeout=600.0, max_retries=0)
    assert "ConnectorSyncJobRepository" not in vars(host_module)
    assert "GitHubSyncWorker" not in vars(host_module)


def test_composition_rejects_disabled_processing_before_provider_objects(
    monkeypatch,
):
    secret_store = Mock()
    monkeypatch.setattr(host_module, "GoogleSecretManagerSecretStore", secret_store)
    process = SimpleNamespace(
        github_sync_ledger_processing_enabled=False,
        secret_manager=Mock(),
        github=Mock(),
    )

    with pytest.raises(ValueError, match="disabled"):
        host_module.compose_github_sync_ledger_worker_host(
            _settings(processing_enabled=False), process_settings=process
        )

    secret_store.assert_not_called()


def test_unknown_worker_outcome_fails_closed_without_logging_exception_message():
    logger = Mock()
    worker = ScriptedWorker(["future-unsafe-outcome"])
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "fatal-run",
        logger=logger,
    )

    assert host.run() == GitHubLedgerHostExitCode.FATAL
    assert host.last_summary.stop_reason == "fatal_error"
    assert host.last_summary.empty_queue is False
    rendered = "\n".join(
        call.args[0] % call.args[1:] for call in logger.error.call_args_list
    )
    assert "error_type=RuntimeError" in rendered
    assert "invalid outcome" not in rendered
    assert host.last_summary.run_status == "failed"
    assert host.last_summary.exit_code == 1


def test_contradictory_noncommitted_counters_fail_closed():
    worker = Mock()
    worker.execute_one_result.return_value = GitHubFileWorkExecution(
        "retry_scheduled",
        uuid4(),
        1,
        FileWorkCounters(86, 86, 1, 1),
    )
    host = GitHubSyncLedgerWorkerHost(
        worker,
        _settings(),
        monotonic=lambda: 0.0,
        run_id_factory=lambda: "contradictory-run",
    )

    assert host.run() == GitHubLedgerHostExitCode.FAILURE
    assert host.last_summary.stop_reason == "fatal_error"
    assert host.last_summary.retry_scheduled == 0
    assert host.last_summary.downloaded_bytes == 0


def test_startup_failure_is_nonzero_and_emits_one_sanitized_summary(monkeypatch):
    logger = Mock()
    monkeypatch.setattr(host_module, "LOGGER", logger)
    monkeypatch.setattr(
        host_module,
        "validate_worker_process_environment",
        Mock(side_effect=ValueError("unsafe configuration detail")),
    )

    assert host_module.main([]) == GitHubLedgerHostExitCode.FAILURE
    assert logger.info.call_count == 1
    rendered = logger.info.call_args.args[0] % logger.info.call_args.args[1:]
    assert "stop_reason=startup_failure" in rendered
    assert "run_status=failed" in rendered
    assert "unsafe configuration detail" not in rendered
