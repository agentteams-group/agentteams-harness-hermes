# agentteams-harness-hermes

Hermes runtime integration for [AgentTeams](https://github.com/agentscope-ai/AgentTeams).

Native tool approvals render as inline Dashboard cards. With Hermes' approval
policy enabled (`HERMES_YOLO_MODE=0`), approving resumes the exact native waiter;
rejecting or timing out denies execution. Replies are bound to the requesting
human, room and request. Native settlement sends the final card receipt.
Existing permission defaults are unchanged. `make smoke` tests approve, deny and
expiry against the image's pinned Hermes approval service.
Migrated from `hermes/` at AgentTeams commit
`41ef2358abc2af7519356d1cd5b9f72392f70af6` (Apache-2.0).
The upstream [Hermes Agent](https://github.com/NousResearch/hermes-agent) is MIT-licensed.

The image pins **Hermes Agent v0.23.0** and uses Python 3.14 with the upstream
`uv.lock` and `matrix` extra. `versions.env` records the Hermes tag and the
AgentTeams commit supplying shared scripts and telemetry. The Controller CLI
defaults to the published `v1.2.6` image; it can be overridden at build time.

This harness runs AgentTeams Worker and Team Leader roles through the `hermes`
runtime. Manager runtime support is not implemented here.

## Contents

- `src/hermes_worker/`: configuration bridge, MinIO/OSS workspace sync and gateway startup.
- `src/hermes_matrix/`: allowlists, group history, mentions and vision policy over Hermes' native mautrix transport.
- `plugins/platforms/matrix/`: user plugin overriding the bundled Matrix registration.
- `scripts/`: container entrypoint.
- `tests/`: bridge, policy, persistence, image and upstream compatibility checks.

Controller/CRDs, Helm, installers, role templates and full-stack integration
tests remain in AgentTeams. AgentTeams consumes this repository as its pinned
`hermes/` Git submodule, retaining the existing build paths and image names.

## Build and test

Docker BuildKit is required. A standalone checkout can build without an
adjacent AgentTeams checkout:

```sh
make build
make smoke
```

For local changes to shared AgentTeams code:

```sh
make build SHARED_CONTEXT=../AgentTeams/shared/lib \
  TRACE_CONTEXT=../AgentTeams/plugins/teamharness/adapters/qwenpaw \
  TEAMHARNESS_CONTEXT=../AgentTeams/plugins/teamharness
```

Unit tests need Python 3.11+ and the worker's Python dependencies:

```sh
python3 -m pip install pytest pyyaml httpx rich typer
make test
```

The image smoke check uses the installed v0.23.0 SDK, validates the native
Matrix adapter interfaces and plugin registration, and loads a bridged gateway
configuration. Full Matrix/LLM delivery is covered by AgentTeams integration
tests and requires a running stack.

CI builds and smoke-tests the image on pull requests. Version tags (`v*`)
publish `linux/amd64` and `linux/arm64` images to
`ghcr.io/agentteams-group/agentteams-harness-hermes`.
`make push VERSION=v0.1.0` publishes the same independent image manually.

Worker startup continues to use `AGENTTEAMS_WORKER_NAME`, `AGENTTEAMS_FS_*`
and `AGENTTEAMS_RUNTIME`; see `scripts/hermes-worker-entrypoint.sh` for the
environment contract. Configurations, sessions, user plugins and Matrix crypto
state remain in the existing per-worker workspace across an image upgrade.

For an OpenAI-compatible gateway connection that requires the Responses API,
set the member environment variable `HERMES_API_MODE=codex_responses`.
The bridge selects a native named provider with that transport and references
the projected `OPENAI_API_KEY`; it does not copy the credential into YAML.
Without this setting, the existing Chat Completions routing stays unchanged.

## Upgrading an existing Worker

Wait for active turns to finish and the final workspace sync to complete before
replacing the Worker image. Reuse the existing worker name and MinIO/OSS prefix.
Preserve any container-local changes that have not reached object storage before
replacement; a fresh container can only restore persisted state.
Startup restores the workspace, reads the Matrix device ID from the persisted
crypto account (the Manager's config may contain an older registration device),
and refreshes the token for that same device. No Matrix account or room needs
to be recreated.

This is a restart-based upgrade with a brief interruption. Interrupting an active
turn can leave the native Hermes conversation paused for explicit recovery;
receiving its reply alone does not mean the turn has finished committing.

CI runs `tests/upgrade_e2e.py` against the released v1.2.6 image (Hermes v0.22.7).
It creates plain and encrypted Matrix conversations, restores into a fresh
v0.23.0 container solely from MinIO, continues both sessions, restarts again,
and decrypts a pre-upgrade encrypted event with the restored Megolm keys.
Matrix, MinIO and both Hermes runtimes are real; the model endpoint is a
deterministic fixture so loss of history causes an exact assertion failure.

```sh
python3 tests/upgrade_e2e.py --new-image ghcr.io/agentteams-group/agentteams-harness-hermes:v0.1.0
```

## Worker recovery candidate (#1631)

The 1.2.8 candidate requires the pinned Core recovery APIs. Before Matrix
connect admits new work, it registers the actual Worker start and claims its
assigned Tasks. Task progress remains authoritative in the Task system.
Native session reuse reads the common `runtime/sessions/hermes/<work-key>.json`
descriptor and verifies actual SDK database content, expiry, current checkpoint
and configuration generation. The former private recovery marker is not used
as authority. Missing/stale native state creates a fresh session for the same
Task; unknown side effects remain in verification state. Native `state.db` and
Matrix crypto backups include committed WAL data. Workspace sync cannot write
Controller-owned common recovery records.

The candidate now uses Core's `WorkerMatrix.accept` for decoded accepted text,
media, retained group context, controls and verified Task assignments. Shared
input persistence precedes native enqueue/history buffering or Task claim;
authorized approval decisions also wait for their control input to be saved.
Each callback retains its actual Matrix source timestamp independently of
overlapping callbacks. Managed Workers suppress the stock mention-time read
marker; existing custom read receipts still use actual persisted SDK rows.
The pinned SDK's turn prologue now saves and reads back the actual current user
row, then commits its common native fact before plugin hooks, compaction or
inference. It wraps the call site outside the SDK's fail-open plugin dispatcher.
Missing input, failed/disabled persistence, an old row or failed common CAS
stops execution. Managed custom read delivery independently waits for that
matching fact. Startup without a source event does not manufacture read facts.
This per-event hook does not advance a shared sync cursor; batch/cursor/backfill,
legacy consumption and ordinary-message execution integration remain open.

Host tests pass57 with1 native-SDK skip. The candidate installed-SDK checks
pass15 callback/session/direct-control tests, the native decoded-input smoke,
the2 native approval queue tests (including failed input persistence), native
Task/session recovery and native Task-tool discovery. Controller and Matrix
transport are mocked; these are not new real-model E2E results.

Five further native-SDK tests verify actual row IDs/readback, repeated flush
deduplication, storage/fact failures and a full native turn against a local
streaming fixture. The native fact precedes compaction and inference, and
failed CAS permits neither. This test does not establish real Task continuation.

Host tests and the installed-SDK recovery smoke cover these boundaries with
mocked Controller requests. They do not establish real Task/model continuation,
ordinary-room message recovery, native sync cursor/backfill integration, the
complete fault suite or cross-Harness switching. Those remain tracked in
[AgentTeams #1631](https://github.com/agentscope-ai/AgentTeams/issues/1631).
# Matrix standalone notifications

With `e2ee_mode: required` (or the equivalent Matrix encryption setting),
standalone cron delivery returns an explicit failure before sending HTTP.
Use the running encrypted gateway for these messages. The standalone sender
has no crypto store and cannot safely deliver required-E2EE messages; an
explicit `e2ee_mode: off` retains the native plaintext delivery behavior.
This guard belongs to this Harness's native Matrix plugin. The retired Core
Hermes adapter is a separate implementation and is not used by this image.

