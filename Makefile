.PHONY: up down logs e2e e2e-cli

COMPOSE = docker compose
INGEST_URL ?= http://localhost:4317
HEALTHZ = $(INGEST_URL)/healthz

## up: build images and start all services in the background
up:
	$(COMPOSE) up -d --build

## down: stop and remove all containers (keeps volumes)
down:
	$(COMPOSE) down

## logs: follow logs for all services
logs:
	$(COMPOSE) logs -f

## e2e: spin up compose, wait for ingest to be ready, run e2e tests, tear down
e2e:
	@echo "==> Starting services"
	$(COMPOSE) up -d --build

	@echo "==> Waiting for ingest server at $(HEALTHZ)"
	@for i in $$(seq 1 30); do \
		if curl -sf $(HEALTHZ) > /dev/null 2>&1; then \
			echo "    ingest is ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "    timed out waiting for ingest"; \
			$(COMPOSE) down; \
			exit 1; \
		fi; \
		echo "    waiting... ($$i/30)"; \
		sleep 2; \
	done

	@echo "==> Running e2e tests"
	INGEST_URL=$(INGEST_URL) go test -v -tags e2e ./ingest/e2e/... ; TEST_EXIT=$$? ; \
	echo "==> Tearing down services" ; \
	$(COMPOSE) down ; \
	exit $$TEST_EXIT

QUERY_URL ?= http://localhost:8081

## e2e-cli: spin up compose, wait for query service, run CLI e2e tests, tear down
e2e-cli:
	@echo "==> Starting services"
	$(COMPOSE) up -d --build

	@echo "==> Waiting for query service at $(QUERY_URL)/healthz"
	@for i in $$(seq 1 60); do \
		if curl -sf $(QUERY_URL)/healthz > /dev/null 2>&1; then \
			echo "    query service is ready"; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "    timed out waiting for query service"; \
			$(COMPOSE) down; \
			exit 1; \
		fi; \
		echo "    waiting... ($$i/60)"; \
		sleep 2; \
	done

	@echo "==> Running CLI e2e tests"
	JL_SERVER=$(QUERY_URL) go test -v -tags e2e ./cli/e2e/... ; TEST_EXIT=$$? ; \
	echo "==> Tearing down services" ; \
	$(COMPOSE) down ; \
	exit $$TEST_EXIT
