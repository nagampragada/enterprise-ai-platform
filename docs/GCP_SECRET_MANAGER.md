# Google Cloud Secret Manager deployment and security

## Scope and current status

The backend implements a production Google Cloud Secret Manager adapter for the application `SecretStore` port. It supports the GitHub App private key and OAuth client-secret lookups plus ephemeral OAuth PKCE store/retrieve/delete operations. The adapter is code-complete and tested only with injected fake clients. No Google Cloud project, IAM role, service account, secret, Cloud Run service, domain, or other production resource has been created by this repository work.

Customer isolation is enforced by the application's tenant-safe database ownership and authorization of opaque references. The `SecretStore.store()` contract has no organization context, so this design does **not** claim one GCP IAM principal, secret namespace, or IAM boundary per customer. Anyone who can obtain and submit a valid opaque reference has a capability to that exact version; application and database controls must prevent cross-tenant reference substitution.

## Runtime identity and configuration

Run Cloud Run with a dedicated user-managed service account. The official Google client obtains Application Default Credentials (ADC) from that Cloud Run service identity. Do not create, mount, upload, or configure a service-account JSON key. Do not put credentials, GitHub private keys, OAuth client secrets, PKCE verifiers, or tokens in application settings, the database, source control, container images, `.env`, or plaintext production fallbacks.

The only Secret Manager settings are nonsecret resource configuration:

```text
GCP_SECRET_MANAGER_PROJECT_ID=<platform-secrets-project-id>
GCP_SECRET_MANAGER_SECRET_PREFIX=eap
GCP_SECRET_MANAGER_ENVIRONMENT=production
```

GitHub continues to receive only immutable references:

```text
GITHUB_APP_CLIENT_SECRET_REFERENCE=gcp-secret-manager://projects/<platform-secrets-project-id>/secrets/eap-sm-<32-lowercase-hex>/versions/<numeric-version>
GITHUB_APP_PRIVATE_KEY_REFERENCE=gcp-secret-manager://projects/<platform-secrets-project-id>/secrets/eap-sm-<32-lowercase-hex>/versions/<numeric-version>
```

All three GCP settings and the complete GitHub App settings must be valid before production composition occurs. Adapter initialization uses ADC. Missing configuration, unavailable credentials, invalid references, or initialization failure leaves GitHub operations fail-closed with the existing generic provider-unavailable response. Application import, health endpoints, and unrelated APIs remain usable without GCP configuration. There is no in-memory or plaintext production fallback.

## Reference and resource invariants

The accepted canonical form is exactly:

```text
gcp-secret-manager://projects/{configured_project_id}/secrets/{configured_prefix}-sm-{32-lowercase-hex}/versions/{positive_numeric_version}
```

The parser runs before provider calls and rejects other projects, names outside the prefix and adapter naming pattern, zero or leading-zero versions, `latest`, aliases, query strings, fragments, percent escaping, missing or extra segments, case changes, and other noncanonical forms. References never contain payloads.

Each `store()` call creates a cryptographically random 128-bit name, a new secret container, and exactly one immutable version. Names contain no organization, connector, user, email, repository, account, provider payload, or secret value. Created secrets carry only these non-sensitive labels:

```text
managed-by=enterprise-ai-platform
eap-secret-policy=single-version
environment=<configured-environment>
```

The adapter validates UTF-8 size before calling Google and accepts at most 65,536 bytes. It sends CRC32C with every version add, requires Google to acknowledge the client checksum, and validates returned CRC32C when present. Unicode and multiline PEM values are preserved byte-for-byte through UTF-8 encode/decode. Public checksum failures use a fixed message.

## Exact request, retry, and cleanup bounds

- Every provider RPC has a timeout of at most 5 seconds and explicitly disables the Google client's automatic retries.
- `access_secret_version` and the deletion preflight `get_secret` retry only deadline-exceeded, service-unavailable, and resource-exhausted failures. They make at most 3 calls within a 12-second total retry deadline.
- Read retry delay uses full jitter: at most 0.1 seconds before attempt 2 and 0.2 seconds before attempt 3. The implementation cap is 1 second, though the three-attempt bound never reaches it.
- Authentication and authorization failures are never retried.
- `create_secret` tries at most 3 independently random names on `AlreadyExists`; it does not retry the same name.
- `add_secret_version`, `destroy_secret_version`, and `delete_secret` each make one attempt. Ambiguous non-idempotent version writes are never retried.
- If container creation succeeds and the first version operation fails or returns invalid integrity metadata, one 5-second best-effort exact-container delete is attempted. Cleanup never replaces the original failure.
- Delete first parses the reference, reads that exact container's metadata, and requires the adapter-managed single-version/environment labels. It then destroys only the referenced numeric version and deletes only that exact container. A missing container succeeds. A missing referenced version succeeds without deleting the container, which prevents a forged version number from deleting a valid version. Already-destroyed state and a missing final container are idempotent success.
- The adapter never lists secrets or versions, accepts wildcards or aliases, or performs bulk deletion. Under normal operation, a store operation has at most 5 provider calls including three collisions, one add, and one cleanup; a delete has at most three metadata reads, one destroy, and one container delete.

## Least-privilege IAM

Create a custom runtime role containing only the permissions exercised by this implementation:

```text
secretmanager.secrets.create
secretmanager.secrets.get
secretmanager.secrets.delete
secretmanager.versions.add
secretmanager.versions.access
secretmanager.versions.destroy
```

`secretmanager.secrets.get` is required only for deletion preflight label validation. The client constructs resource names directly, so it does not exercise resource-manager lookup permissions. It does not need secret/version list, update, disable, enable, IAM-policy, rotation, or replication-management permissions.

Grant the custom role only to the dedicated Cloud Run service identity in a dedicated secrets project or tightly scoped environment project. Do not grant Owner, Editor, project administrator, Secret Manager Admin, or broad service-account administration. Use separate production and nonproduction projects. Apply IAM Conditions restricting existing resources to the configured secret-name prefix where Secret Manager and the particular permission support such conditions; verify create semantics separately because create authorization is evaluated on the parent resource. Restrict who can deploy a revision under the runtime identity.

Enable and retain appropriate Data Access audit logs. Alert on denied access, unexpected create/delete volume, access from unexpected principals, and activity outside the expected service/revision. Establish rotation, break-glass, incident response, and recovery procedures before production use.

## Safe operator provisioning for GitHub App secrets

Provision the GitHub App private key and OAuth client secret as separate, single-version secrets. Use placeholders in documentation and tickets. Generate each non-identifying secret ID with 128 bits of randomness so it matches `<prefix>-sm-<32-lowercase-hex>`. Do not include customer or GitHub account identifiers in names or labels.

An operator may create only metadata on the command line:

```text
gcloud secrets create <random-secret-id> --project=<platform-secrets-project-id> --replication-policy=automatic --labels=environment=production,purpose=github-app
gcloud secrets versions add <random-secret-id> --project=<platform-secrets-project-id> --data-file=-
```

Supply the value through stdin or a protected non-command-line input source and use the terminal's EOF sequence. Never place the value in command arguments, shell history, source control, screenshots, logs, tickets, `.env`, or clipboard automation. Do not use the adapter-owned `eap-secret-policy=single-version` label on operator-owned long-lived GitHub secrets; that label is the adapter's deletion proof for its own ephemeral containers.

After creation, record only the numeric version and configure the version-pinned reference. Never configure `latest` or an alias, and never print or access the secret merely to verify provisioning. Validate metadata and version state without requesting payload output. Remove any protected temporary input according to the organization's secure media procedure.

## Remaining production prerequisites

- Provision the dedicated secrets/environment project, API enablement, custom role, Cloud Run service identity, and audited IAM bindings.
- Provision the two GitHub App secrets through the safe operator process and configure their immutable references.
- Configure Cloud Run nonsecret environment settings and deploy only after security review.
- Establish key/client-secret rotation and safe old-version retirement procedures.
- Validate IAM Conditions, audit logging, alerts, quotas, and incident response in the chosen GCP organization.

No domain is required for this infrastructure slice. A domain and exact public URLs become an operator concern only when deploying the already-configured GitHub browser callback/setup endpoints. GitHub repository discovery remains the next GitHub feature slice after this foundation.
