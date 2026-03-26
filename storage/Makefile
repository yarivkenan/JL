.PHONY: up down logs e2e

COMPOSE        = docker compose -f ../docker-compose.yml
DATABASE_URL  ?= postgres://otel:otel@localhost:5432/otel_metrics
KAFKA_BROKERS ?= localhost:19092
KAFKA_TOPIC   ?= otel.metrics

## up: start TimescaleDB and Redpanda in the background
up:
	$(COMPOSE) up -d db redpanda redpanda-init

## down: stop and remove containers (keeps volumes)
down:
	$(COMPOSE) down

## logs: follow db and redpanda logs
logs:
	$(COMPOSE) logs -f db redpanda

## e2e: spin up infrastructure, wait until healthy, run e2e tests, tear down
e2e:
	@echo "==> Starting TimescaleDB and Redpanda"
	$(COMPOSE) up -d db redpanda redpanda-init

	@echo "==> Waiting for TimescaleDB"
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

	@echo "==> Waiting for Redpanda"
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T redpanda rpk cluster info --brokers localhost:9092 > /dev/null 2>&1; then \
			echo "    redpanda is ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "    timed out waiting for redpanda"; \
			$(COMPOSE) down; \
			exit 1; \
		fi; \
		echo "    waiting... ($$i/30)"; \
		sleep 2; \
	done

	@echo "==> Running e2e tests"
	DATABASE_URL=$(DATABASE_URL) KAFKA_BROKERS=$(KAFKA_BROKERS) KAFKA_TOPIC=$(KAFKA_TOPIC) \
		go test -v -tags e2e ./e2e/... ; TEST_EXIT=$$? ; \
	echo "==> Tearing down services" ; \
	$(COMPOSE) down ; \
	exit $$TEST_EXIT
