include versions.env

IMAGE ?= ghcr.io/agentteams-group/agentteams-harness-hermes
VERSION ?= v0.1.0
PLATFORMS ?= linux/amd64,linux/arm64
SHARED_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF):shared/lib
TRACE_CONTEXT ?= https://github.com/agentscope-ai/AgentTeams.git\#$(AT_REF):plugins/teamharness/adapters/qwenpaw
BUILD_ARGS = --build-context shared=$(SHARED_CONTEXT) --build-context teamharness_trace=$(TRACE_CONTEXT) --build-arg HERMES_GIT_REF=$(HERMES_GIT_REF)

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
