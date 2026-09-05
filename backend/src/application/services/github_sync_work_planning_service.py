"""Feature-gated shadow planning for durable GitHub repository file work."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
import re
from uuid import UUID

from application.services.github_repository_content_service import (
    MAX_GITHUB_BLOB_BYTES,
    MAX_REPOSITORY_PATH_BYTES,
    MAX_REPOSITORY_PATH_SEGMENT_BYTES,
    MAX_REPOSITORY_PATH_SEGMENTS,
    SUPPORTED_CONTENT_EXTENSIONS,
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
)
from domain.connectors.sync_work_ledger import (
    DiscoveryRegistrationResult,
    FileWorkManifestEntry,
    GenerationObservationDisposition,
    GenerationSourceObservation,
    MAX_MANIFEST_BATCH_SIZE,
    RepositoryGenerationRegistration,
    RepositoryGenerationView,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


GITHUB_SYNC_LEDGER_PLANNING_ENVIRONMENT_VARIABLE = (
    "GITHUB_SYNC_LEDGER_PLANNING_ENABLED"
)
GITHUB_PROVIDER_KEY = "github"
_GITHUB_OBJECT_ID = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_GITHUB_FILE_ENTRY_TYPES = frozenset({"regular_blob", "symlink", "submodule"})
GITHUB_PLANNING_MIME_TYPES = {
    ".pdf": "application/pdf",
    ".docx": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ),
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
}


class InvalidGitHubSyncWorkPlanningRequest(ValueError):
    """Raised when shadow-planning attribution or manifest data is invalid."""


@dataclass(frozen=True)
class GitHubManifestPlanningResult:
    generation_id: UUID
    generation_created: bool
    eligible_count: int
    created_count: int
    existing_count: int
    observed_count: int = 0
    created_observation_count: int = 0


class GitHubSyncWorkPlanningService:
    """Register bounded shadow work without claiming or processing it."""

    def __init__(self, repository: ConnectorSyncWorkLedgerRepository) -> None:
        if not isinstance(repository, ConnectorSyncWorkLedgerRepository):
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub work-ledger repository is invalid"
            )
        self._repository = repository

    def ensure_generation(
        self,
        *,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        sync_job_id: UUID,
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
        profile_fingerprint: str,
        now: datetime,
    ) -> tuple[RepositoryGenerationView, bool]:
        _validate_attribution(
            organization_id,
            connector_id,
            connector_scope_id,
            authorization,
            snapshot,
        )
        try:
            request = RepositoryGenerationRegistration(
                organization_id=organization_id,
                connector_id=connector_id,
                connector_scope_id=connector_scope_id,
                sync_job_id=sync_job_id,
                provider_key=GITHUB_PROVIDER_KEY,
                repository_identity=snapshot.canonical_repository_identity,
                branch_name=snapshot.default_branch_name,
                commit_object_id=snapshot.commit_object_id,
                root_tree_object_id=snapshot.root_tree_object_id,
                profile_fingerprint=profile_fingerprint,
                created_at=now,
            )
        except ValueError as exc:
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub generation registration is invalid"
            ) from exc
        return self._repository.register_generation(request)

    def register_manifest_batch(
        self,
        *,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        sync_job_id: UUID,
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
        profile_fingerprint: str,
        entries: Sequence[GitHubRepositoryEntry],
        now: datetime,
    ) -> GitHubManifestPlanningResult:
        if isinstance(entries, (str, bytes)) or not isinstance(entries, Sequence):
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub planning entries are invalid"
            )
        if len(entries) > MAX_MANIFEST_BATCH_SIZE:
            raise InvalidGitHubSyncWorkPlanningRequest(
                f"GitHub planning batch must not exceed {MAX_MANIFEST_BATCH_SIZE} entries"
            )
        _validate_attribution(
            organization_id,
            connector_id,
            connector_scope_id,
            authorization,
            snapshot,
        )
        observations = _observation_entries(snapshot, profile_fingerprint, entries)
        manifest = _manifest_entries(snapshot, profile_fingerprint, entries)
        generation, generation_created = self.ensure_generation(
            organization_id=organization_id,
            connector_id=connector_id,
            connector_scope_id=connector_scope_id,
            sync_job_id=sync_job_id,
            authorization=authorization,
            snapshot=snapshot,
            profile_fingerprint=profile_fingerprint,
            now=now,
        )
        if not observations:
            return GitHubManifestPlanningResult(
                generation.generation_id,
                generation_created,
                0,
                0,
                0,
            )
        result = self._repository.register_discovery_batch(
            organization_id,
            generation.generation_id,
            observations,
            manifest,
            now=now,
        )
        _validate_registration_result(
            generation.generation_id, observations, manifest, result
        )
        return GitHubManifestPlanningResult(
            generation.generation_id,
            generation_created,
            len(manifest),
            result.created_work_count,
            result.existing_work_count,
            len(observations),
            result.created_observation_count,
        )

    def mark_discovery_complete(
        self,
        *,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        sync_job_id: UUID,
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
        profile_fingerprint: str,
        now: datetime,
    ) -> RepositoryGenerationView:
        generation, _created = self.ensure_generation(
            organization_id=organization_id,
            connector_id=connector_id,
            connector_scope_id=connector_scope_id,
            sync_job_id=sync_job_id,
            authorization=authorization,
            snapshot=snapshot,
            profile_fingerprint=profile_fingerprint,
            now=now,
        )
        return self._repository.mark_discovery_complete(
            organization_id, generation.generation_id, now=now
        )


def _manifest_entries(
    snapshot: GitHubRepositorySnapshot,
    profile_fingerprint: str,
    entries: Sequence[GitHubRepositoryEntry],
) -> tuple[FileWorkManifestEntry, ...]:
    keyed: dict[str, FileWorkManifestEntry] = {}
    for entry in entries:
        _validate_entry(snapshot, entry)
        extension = PurePosixPath(entry.path).suffix.casefold()
        if (
            entry.entry_type != "regular_blob"
            or extension not in SUPPORTED_CONTENT_EXTENSIONS
            or entry.size_bytes is None
            or entry.size_bytes > MAX_GITHUB_BLOB_BYTES
        ):
            continue
        source_item_key = _source_identity(snapshot.repository_id, entry.path)
        try:
            planned = FileWorkManifestEntry(
                source_item_key=source_item_key,
                repository_path=entry.path,
                provider_blob_id=entry.object_id,
                provider_revision_id=snapshot.commit_object_id,
                profile_fingerprint=profile_fingerprint,
                file_size_bytes=entry.size_bytes,
                file_extension=extension,
                mime_type=GITHUB_PLANNING_MIME_TYPES[extension],
            )
        except ValueError as exc:
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub planning manifest entry is invalid"
            ) from exc
        previous = keyed.get(source_item_key)
        if previous is not None and previous != planned:
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub planning source identity is duplicated"
            )
        keyed[source_item_key] = planned
    return tuple(keyed.values())


def _observation_entries(
    snapshot: GitHubRepositorySnapshot,
    profile_fingerprint: str,
    entries: Sequence[GitHubRepositoryEntry],
) -> tuple[GenerationSourceObservation, ...]:
    keyed: dict[str, GenerationSourceObservation] = {}
    for entry in entries:
        _validate_entry(snapshot, entry)
        extension = PurePosixPath(entry.path).suffix.casefold()
        if entry.entry_type != "regular_blob":
            disposition = GenerationObservationDisposition.UNSUPPORTED_OBJECT_TYPE
        elif extension not in SUPPORTED_CONTENT_EXTENSIONS:
            disposition = GenerationObservationDisposition.UNSUPPORTED_FORMAT
        elif entry.size_bytes is not None and entry.size_bytes > MAX_GITHUB_BLOB_BYTES:
            disposition = GenerationObservationDisposition.OVERSIZED
        else:
            disposition = GenerationObservationDisposition.ELIGIBLE
        try:
            observation = GenerationSourceObservation(
                source_item_key=_source_identity(snapshot.repository_id, entry.path),
                repository_path=entry.path,
                provider_object_id=entry.object_id,
                provider_revision_id=snapshot.commit_object_id,
                profile_fingerprint=profile_fingerprint,
                entry_type=entry.entry_type,
                disposition=disposition,
                file_size_bytes=entry.size_bytes,
            )
        except ValueError as exc:
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub planning observation is invalid"
            ) from exc
        previous = keyed.get(observation.source_item_key)
        if previous is not None and previous != observation:
            raise InvalidGitHubSyncWorkPlanningRequest(
                "GitHub planning source identity is duplicated"
            )
        keyed[observation.source_item_key] = observation
    return tuple(keyed.values())


def _validate_attribution(
    organization_id: UUID,
    connector_id: UUID,
    connector_scope_id: UUID,
    authorization: GitHubRepositoryContentAuthorization,
    snapshot: GitHubRepositorySnapshot,
) -> None:
    if (
        not isinstance(authorization, GitHubRepositoryContentAuthorization)
        or not isinstance(snapshot, GitHubRepositorySnapshot)
        or authorization.organization_id != organization_id
        or authorization.connector_id != connector_id
        or authorization.scope_id != connector_scope_id
        or snapshot.connector_id != connector_id
        or snapshot.scope_id != connector_scope_id
        or snapshot.repository_id != authorization.repository_id
        or snapshot.canonical_repository_identity
        != authorization.canonical_repository_identity
        or snapshot.default_branch_name != authorization.default_branch_name
        or isinstance(snapshot.repository_id, bool)
        or not isinstance(snapshot.repository_id, int)
        or snapshot.repository_id <= 0
        or snapshot.canonical_repository_identity
        != f"github:repository:{snapshot.repository_id}"
        or not _valid_object_id(snapshot.commit_object_id)
        or not _valid_object_id(snapshot.root_tree_object_id)
    ):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning attribution is invalid"
        )


def _validate_entry(
    snapshot: GitHubRepositorySnapshot, entry: GitHubRepositoryEntry
) -> None:
    if (
        not isinstance(entry, GitHubRepositoryEntry)
        or entry.connector_id != snapshot.connector_id
        or entry.scope_id != snapshot.scope_id
        or entry.repository_id != snapshot.repository_id
        or entry.canonical_repository_identity
        != snapshot.canonical_repository_identity
        or entry.commit_object_id != snapshot.commit_object_id
        or entry.root_tree_object_id != snapshot.root_tree_object_id
        or entry.entry_type not in _GITHUB_FILE_ENTRY_TYPES
        or not _valid_object_id(entry.parent_tree_object_id)
        or not _valid_object_id(entry.object_id)
        or entry.executable not in {True, False}
    ):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning entry attribution is invalid"
        )
    _validate_repository_path(entry.path)
    if (
        not isinstance(entry.name, str)
        or entry.name != entry.path.rsplit("/", 1)[-1]
        or (entry.entry_type in {"regular_blob", "symlink"} and (
            isinstance(entry.size_bytes, bool)
            or not isinstance(entry.size_bytes, int)
            or entry.size_bytes < 0
        ))
        or (entry.entry_type == "submodule" and entry.size_bytes is not None)
        or (entry.entry_type != "regular_blob" and entry.executable)
    ):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning entry descriptor is invalid"
        )


def _validate_repository_path(path: object) -> None:
    if not isinstance(path, str):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning repository path is invalid"
        )
    try:
        encoded = path.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning repository path is invalid"
        ) from exc
    segments = path.split("/")
    if (
        not path
        or path.startswith("/")
        or path.endswith("/")
        or "\\" in path
        or len(encoded) > MAX_REPOSITORY_PATH_BYTES
        or len(segments) > MAX_REPOSITORY_PATH_SEGMENTS
        or any(
            segment in {"", ".", ".."}
            or len(segment.encode("utf-8")) > MAX_REPOSITORY_PATH_SEGMENT_BYTES
            for segment in segments
        )
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in path)
    ):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub planning repository path is invalid"
        )


def _valid_object_id(value: object) -> bool:
    return isinstance(value, str) and _GITHUB_OBJECT_ID.fullmatch(value) is not None


def _validate_registration_result(
    generation_id: UUID,
    observations: tuple[GenerationSourceObservation, ...],
    manifest: tuple[FileWorkManifestEntry, ...],
    result: DiscoveryRegistrationResult,
) -> None:
    if (
        not isinstance(result, DiscoveryRegistrationResult)
        or result.generation_id != generation_id
        or result.created_observation_count + result.existing_observation_count
        != len(observations)
        or result.created_work_count + result.existing_work_count != len(manifest)
        or len(result.observation_ids) != len(observations)
        or len(result.work_item_ids) != len(manifest)
    ):
        raise InvalidGitHubSyncWorkPlanningRequest(
            "GitHub manifest registration result is invalid"
        )


def _source_identity(repository_id: int, path: str) -> str:
    return f"github:repository:{repository_id}:path:{path}"
