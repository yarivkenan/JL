.PHONY: up down logs e2e

COMPOSE        = docker compose -f ../docker-compose.yml
STORAGE_ADDR  ?= localhost:8081
KAFKA_BROKERS ?= localhost:19092
KAFKA_TOPIC   ?= otel.metrics

## up: start all infrastructure + consumer in the background
up:
	$(COMPOSE) up -d consumer

## down: stop and remove containers (keeps volumes)
down:
	$(COMPOSE) down

## logs: follow consumer and dependency logs
logs:
	$(COMPOSE) logs -f consumer db redpanda

## e2e: spin up infrastructure + consumer, run e2e tests, tear down
e2e:
	@echo "==> Starting services (consumer pulls in db + redpanda via depends_on)"
	$(COMPOSE) up -d consumer

	@echo "==> Running e2e tests (TestMain waits for consumer /healthz)"
	STORAGE_ADDR=$(STORAGE_ADDR) KAFKA_BROKERS=$(KAFKA_BROKERS) KAFKA_TOPIC=$(KAFKA_TOPIC) \
		go test -v -tags e2e -timeout 180s ./e2e/... ; TEST_EXIT=$$? ; \
	echo "==> Tearing down services" ; \
	$(COMPOSE) down ; \
	exit $$TEST_EXIT
