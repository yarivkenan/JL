.PHONY: up down logs e2e

COMPOSE        = docker compose -f ../docker-compose.yml
DATABASE_URL  ?= postgres://otel:otel@localhost:5432/otel_metrics

## up: start TimescaleDB in the background
up:
	$(COMPOSE) up -d db

## down: stop and remove containers (keeps volumes)
down:
	$(COMPOSE) down

## logs: follow db logs
logs:
	$(COMPOSE) logs -f db

## e2e: spin up TimescaleDB, wait until healthy, run e2e tests, tear down
e2e:
	@echo "==> Starting TimescaleDB"
	$(COMPOSE) up -d db

	@echo "==> Waiting for TimescaleDB to be ready"
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T db pg_isready -U otel -d otel_metrics > /dev/null 2>&1; then \
			echo "    db is ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "    timed out waiting for db"; \
			$(COMPOSE) down; \
			exit 1; \
		fi; \
		echo "    waiting... ($$i/30)"; \
		sleep 2; \
	done

	@echo "==> Running e2e tests"
	DATABASE_URL=$(DATABASE_URL) go test -v -tags e2e ./e2e/... ; TEST_EXIT=$$? ; \
	echo "==> Tearing down services" ; \
	$(COMPOSE) down ; \
	exit $$TEST_EXIT
