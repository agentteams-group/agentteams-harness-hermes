# agentteams-harness-hermes

Hermes runtime integration for [AgentTeams](https://github.com/agentscope-ai/AgentTeams).
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
  TRACE_CONTEXT=../AgentTeams/plugins/teamharness/adapters/qwenpaw
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

## Upgrading an existing Worker

Wait for active turns to finish and the final workspace sync to complete before
replacing the Worker image. Reuse the existing worker name and MinIO/OSS prefix.
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
