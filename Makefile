include versions.env

IMAGE ?= ghcr.io/agentteams-group/agentteams-harness-hermes
VERSION ?= v0.1.0
PLATFORMS ?= linux/amd64,linux/arm64
SHARED_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF):shared/lib
TRACE_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF):plugins/teamharness/adapters/qwenpaw
TEAMHARNESS_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF):plugins/teamharness
AT_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF)
BUILD_ARGS = --build-context agentteams=$(AT_CONTEXT) --build-context shared=$(SHARED_CONTEXT) --build-context teamharness_trace=$(TRACE_CONTEXT) --build-context teamharness=$(TEAMHARNESS_CONTEXT) --build-arg HERMES_GIT_REF=$(HERMES_GIT_REF)

.PHONY: build push test smoke
build:
	docker build $(BUILD_ARGS) $(DOCKER_BUILD_ARGS) -t $(IMAGE):$(VERSION) .

push:
	docker buildx build --platform $(PLATFORMS) $(BUILD_ARGS) $(DOCKER_BUILD_ARGS) -t $(IMAGE):$(VERSION) --push .

test:
	python3 -m pytest tests -q

smoke:
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -v "$(CURDIR)/tests/upstream_smoke.py:/tmp/upstream_smoke.py:ro" $(IMAGE):$(VERSION) /tmp/upstream_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -v "$(CURDIR)/tests/native_approvals_smoke.py:/tmp/native_approvals_smoke.py:ro" $(IMAGE):$(VERSION) /tmp/native_approvals_smoke.py

direct-control-smoke:
	docker run --rm --entrypoint /bin/sh -v "$(CURDIR):/dm:ro" -e PYTHONPATH=/dm/src:/opt/agentteams/scripts/lib $(IMAGE):$(VERSION) -ec '/opt/venv/hermes/bin/python -m pip install pytest pytest-asyncio; /opt/venv/hermes/bin/python -m pytest /dm/tests/test_direct_control.py -q'

# Run the installed SDK admission boundary without a model or external service.
native-coordination-smoke:
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_coordination_smoke.py

native-recovery-smoke:
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_manager_receipt_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_ordinary_recovery_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_sync_history_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_sync_state_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_input_commit_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_upgrade_fixture_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_recovery_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_shared_input_smoke.py
	docker run --rm --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_task_tools_smoke.py
	docker run --rm --network none --entrypoint /opt/venv/hermes/bin/python -e PYTHONPATH=/opt/agentteams/scripts/lib/telemetry:/opt/agentteams/scripts/lib -e AGENTTEAMS_PLUGIN_DIR=/opt/agentteams/plugins/teamharness -v "$(CURDIR)/tests:/smoke:ro" $(IMAGE):$(VERSION) /smoke/native_public_tasks_smoke.py

storage-config-smoke:
	python3 tests/storage_config_smoke.py --image $(IMAGE):$(VERSION)
