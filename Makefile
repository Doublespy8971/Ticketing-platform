COMPOSE := docker compose -f infra/docker-compose.yml
COMPOSE_FULL := docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml
COMPOSE_OBS := docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml -f infra/docker-compose.observability.yml
HEALTHY_SERVICES := kafka postgres-inventory postgres-order postgres-payment redis
APP_SERVICES := gateway-service inventory-service order-service payment-service notification-service
OBS_SERVICES := prometheus grafana kafka-exporter redis-exporter postgres-inventory-exporter postgres-order-exporter postgres-payment-exporter

.PHONY: infra-up infra-down infra-reset infra-topics infra-psql-inventory infra-psql-order infra-psql-payment infra-redis-cli \
        smoke-inventory smoke-order smoke-payment smoke-gateway run-gateway smoke-notification run-notification \
        stack-build stack-up stack-down stack-reset stack-logs stack-ps smoke-all smoke-selftest \
        e2e e2e-fast e2e-chaos e2e-full \
        obs-up obs-down obs-reset obs-urls obs-rules-check obs-targets-check metrics-check

infra-up:
	@test -f infra/.env || cp infra/.env.example infra/.env
	$(COMPOSE) up -d --wait $(HEALTHY_SERVICES)
	$(COMPOSE) up kafka-init --exit-code-from kafka-init

infra-down:
	$(COMPOSE) down

infra-reset:
	$(COMPOSE) down -v
	$(MAKE) infra-up

infra-topics:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

infra-psql-inventory:
	$(COMPOSE) exec postgres-inventory sh -c 'psql -U "$$POSTGRES_USER" -d inventory_db'

infra-psql-order:
	$(COMPOSE) exec postgres-order sh -c 'psql -U "$$POSTGRES_USER" -d order_db'

infra-psql-payment:
	$(COMPOSE) exec postgres-payment sh -c 'psql -U "$$POSTGRES_USER" -d payment_db'

infra-redis-cli:
	$(COMPOSE) exec redis redis-cli

smoke-inventory:
	bash scripts/smoke-inventory.sh

smoke-order:
	bash scripts/smoke-order.sh

smoke-payment:
	bash scripts/smoke-payment.sh

smoke-gateway:
	bash scripts/smoke-gateway.sh

run-gateway:
	mvn spring-boot:run -pl gateway-service -Dspring-boot.run.profiles=local

smoke-notification:
	bash scripts/smoke-notification.sh

run-notification:
	cd notification-service && go run ./cmd/notification-service

stack-build:
	@test -f infra/.env || cp infra/.env.example infra/.env
	DOCKER_BUILDKIT=1 $(COMPOSE_FULL) build

stack-up:
	@test -f infra/.env || cp infra/.env.example infra/.env
	DOCKER_BUILDKIT=1 $(COMPOSE_FULL) up -d --build
	@echo "Waiting up to 180s for full stack to become healthy..."
	@start_time=$$(date +%s); \
	while true; do \
		ps_json=$$($(COMPOSE_FULL) ps --format json 2>/dev/null || true); \
		unhealthy=$$(echo "$$ps_json" | jq -r 'select(.Health == "unhealthy") | .Name' 2>/dev/null || true); \
		dead=$$(echo "$$ps_json" | jq -r 'select(.State == "dead") | .Name' 2>/dev/null || true); \
		if [ -n "$$unhealthy" ] || [ -n "$$dead" ]; then \
			echo "\n[ERROR] Containers reported unhealthy or dead: $$unhealthy $$dead"; \
			$(COMPOSE_FULL) ps; \
			exit 1; \
		fi; \
		exited=$$(echo "$$ps_json" | jq -r 'select(.State == "exited" and .Service != "kafka-init") | .Name' 2>/dev/null || true); \
		if [ -n "$$exited" ]; then \
			echo "\n[ERROR] Containers exited unexpectedly: $$exited"; \
			$(COMPOSE_FULL) ps; \
			exit 1; \
		fi; \
		starting=$$(echo "$$ps_json" | jq -r 'select(.Health == "starting") | .Name' 2>/dev/null || true); \
		healthy_count=$$(echo "$$ps_json" | jq -r 'select(.Health == "healthy") | .Name' 2>/dev/null | wc -l | tr -d ' '); \
		if [ -z "$$starting" ] && [ "$$healthy_count" -ge 10 ]; then \
			echo "\n[OK] All services are healthy!"; \
			$(COMPOSE_FULL) ps; \
			break; \
		fi; \
		now=$$(date +%s); \
		elapsed=$$((now - start_time)); \
		if [ $$elapsed -ge 180 ]; then \
			echo "\n[ERROR] Timed out waiting for containers to become healthy (180s limit)."; \
			echo "Unhealthy / starting containers:"; \
			$(COMPOSE_FULL) ps; \
			exit 1; \
		fi; \
		sleep 2; \
	done

stack-down:
	$(COMPOSE_OBS) down

stack-reset:
	$(COMPOSE_OBS) down -v
	$(MAKE) stack-up

stack-logs:
	$(COMPOSE_FULL) logs -f $(APP_SERVICES)

stack-ps:
	$(COMPOSE_FULL) ps

smoke-all:
	@bash -c ' \
		set -e; \
		scripts=("smoke-inventory" "smoke-order" "smoke-payment" "smoke-notification" "smoke-gateway"); \
		commands=("scripts/smoke-inventory.sh" "scripts/smoke-order.sh" "scripts/smoke-payment.sh" "scripts/smoke-notification.sh" "scripts/smoke-gateway.sh"); \
		declare -a statuses; \
		declare -a durations; \
		overall_exit=0; \
		for i in "$${!scripts[@]}"; do \
			name="$${scripts[$$i]}"; \
			cmd="$${commands[$$i]}"; \
			echo "================================================================="; \
			echo "Running $$name ($$cmd)..."; \
			echo "================================================================="; \
			t_start=$$(date +%s); \
			if bash "$$cmd"; then \
				statuses[$$i]="PASS"; \
			else \
				statuses[$$i]="FAIL"; \
				overall_exit=1; \
			fi; \
			t_end=$$(date +%s); \
			durations[$$i]="$$((t_end - t_start))s"; \
		done; \
		echo ""; \
		echo "====================== SMOKE TEST SUMMARY ======================"; \
		printf "| %-20s | %-8s | %-10s |\n" "Test Suite" "Status" "Duration"; \
		echo "|----------------------|----------|------------|"; \
		for i in "$${!scripts[@]}"; do \
			printf "| %-20s | %-8s | %-10s |\n" "$${scripts[$$i]}" "$${statuses[$$i]}" "$${durations[$$i]}"; \
		done; \
		echo "================================================================"; \
		if [ $$overall_exit -ne 0 ]; then \
			echo "One or more smoke test suites FAILED."; \
			exit 1; \
		else \
			echo "All smoke test suites PASSED successfully."; \
		fi \
	'

smoke-selftest:
	bash scripts/test-kafka-delivery.sh

e2e:
	mvn -pl e2e-tests -Pe2e verify

e2e-fast:
	mvn -pl e2e-tests -Pe2e -Dgroups=fast verify

e2e-chaos:
	mvn -pl e2e-tests -Pe2e -Dgroups=chaos verify

e2e-full:
	$(MAKE) stack-reset
	$(MAKE) e2e
	@echo "E2E report written to: $$(pwd)/e2e-tests/target/e2e-report.md"

obs-up:
	@test -f infra/.env || cp infra/.env.example infra/.env
	$(COMPOSE_OBS) up -d --wait
	@echo "[OK] Stack + observability services are healthy!"
	@$(MAKE) --no-print-directory obs-urls

obs-down:
	$(COMPOSE_OBS) rm -s -f $(OBS_SERVICES)

obs-reset:
	$(COMPOSE_OBS) rm -s -f -v $(OBS_SERVICES)
	docker volume rm -f ticketing-infra_prometheus-data ticketing-infra_grafana-data 2>/dev/null || true
	$(MAKE) obs-up

obs-urls:
	@echo "Observability URLs (bound to 127.0.0.1 only):"
	@echo "  Prometheus: http://127.0.0.1:9090"
	@echo "  Grafana:    http://127.0.0.1:3000 (credentials in infra/.env)"

obs-rules-check:
	docker run --rm --entrypoint promtool -v "$(CURDIR)/infra/observability/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
	docker run --rm --entrypoint promtool -v "$(CURDIR)/infra/observability/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/rules/slo.rules.yml /etc/prometheus/rules/alerts.rules.yml

obs-targets-check:
	bash scripts/check-obs-targets.sh

metrics-check:
	bash scripts/check-metrics.sh




