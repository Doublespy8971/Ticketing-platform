COMPOSE := docker compose -f infra/docker-compose.yml
COMPOSE_FULL := docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml
COMPOSE_OBS := docker compose -f infra/docker-compose.yml -f infra/docker-compose.full.yml -f infra/docker-compose.observability.yml
HEALTHY_SERVICES := kafka postgres-inventory postgres-order postgres-payment redis
APP_SERVICES := gateway-service inventory-service order-service payment-service notification-service
OBS_SERVICES := prometheus grafana kafka-exporter redis-exporter postgres-inventory-exporter postgres-order-exporter postgres-payment-exporter
CAFFEINATE := $(shell command -v caffeinate >/dev/null 2>&1 && echo "caffeinate -is" || true)

.PHONY: infra-up infra-down infra-reset infra-topics infra-psql-inventory infra-psql-order infra-psql-payment infra-redis-cli \
        smoke-inventory smoke-order smoke-payment smoke-gateway run-gateway smoke-notification run-notification \
        stack-build stack-up stack-down stack-reset stack-logs stack-ps smoke-all smoke-selftest \
        e2e e2e-fast e2e-chaos e2e-full e2e-k8s e2e-k8s-fast e2e-k8s-chaos \
        obs-up obs-down obs-reset obs-urls obs-rules-check obs-targets-check metrics-check \
        dashboards-gen dashboards-check traffic traffic-long \
        load-flash load-journey load-all load-fresh load-report load-audit jvm-check \
        load-preflight load-breakpoint-hold load-breakpoint-journey load-breakpoint \
        load-steady load-sustained load-sustained-x3 load-soak \
        k8s-lint k8s-images k8s-secrets k8s-up k8s-status k8s-forward k8s-forward-stop k8s-logs k8s-down k8s-destroy k8s-redeploy

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
		unhealthy=$$(printf '%s\n' "$$ps_json" | jq -r 'select(.Health == "unhealthy") | .Name' 2>/dev/null || true); \
		dead=$$(printf '%s\n' "$$ps_json" | jq -r 'select(.State == "dead") | .Name' 2>/dev/null || true); \
		if [ -n "$$unhealthy" ] || [ -n "$$dead" ]; then \
			echo "\n[ERROR] Containers reported unhealthy or dead: $$unhealthy $$dead"; \
			$(COMPOSE_FULL) ps; \
			exit 1; \
		fi; \
		exited=$$(printf '%s\n' "$$ps_json" | jq -r 'select(.State == "exited" and .Service != "kafka-init") | .Name' 2>/dev/null || true); \
		if [ -n "$$exited" ]; then \
			echo "\n[ERROR] Containers exited unexpectedly: $$exited"; \
			$(COMPOSE_FULL) ps; \
			exit 1; \
		fi; \
		starting=$$(printf '%s\n' "$$ps_json" | jq -r 'select(.Health == "starting") | .Name' 2>/dev/null || true); \
		healthy_count=$$(printf '%s\n' "$$ps_json" | jq -r 'select(.Health == "healthy") | .Name' 2>/dev/null | wc -l | tr -d ' '); \
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

e2e-k8s-fast:
	@k3d cluster list 2>/dev/null | grep -q "^ticketing[[:space:]]" || { echo "[ERROR] k3d cluster 'ticketing' does not exist. Run 'make k8s-up' first." >&2; exit 1; }
	@test -f /tmp/ticketing-k8s-forward/pids || { echo "[ERROR] Kubernetes port-forwards are not running. Run 'make k8s-forward' first." >&2; exit 1; }
	E2E_BACKEND=k8s E2E_FAILOVER_BUDGET_SECONDS=180 mvn -pl e2e-tests -Pe2e -Dgroups=fast verify

e2e-k8s-chaos:
	@k3d cluster list 2>/dev/null | grep -q "^ticketing[[:space:]]" || { echo "[ERROR] k3d cluster 'ticketing' does not exist. Run 'make k8s-up' first." >&2; exit 1; }
	@test -f /tmp/ticketing-k8s-forward/pids || { echo "[ERROR] Kubernetes port-forwards are not running. Run 'make k8s-forward' first." >&2; exit 1; }
	E2E_BACKEND=k8s E2E_FAILOVER_BUDGET_SECONDS=180 mvn -pl e2e-tests -Pe2e -Dgroups=chaos verify

e2e-k8s:
	$(MAKE) e2e-k8s-fast
	$(MAKE) k8s-down
	$(MAKE) k8s-up
	$(MAKE) k8s-forward
	$(MAKE) e2e-k8s-chaos

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

dashboards-gen:
	python3 scripts/gen-dashboards.py

dashboards-check:
	bash scripts/check-dashboards.sh

traffic:
	python3 scripts/traffic.py --duration 180 --rate 5 --force-expiry

traffic-long:
	python3 scripts/traffic.py --duration 900 --rate 5 --force-expiry

load-flash:
	$(CAFFEINATE) bash scripts/loadtest-run.sh flash

load-journey:
	$(CAFFEINATE) bash scripts/loadtest-run.sh journey

load-all:
	$(CAFFEINATE) bash scripts/loadtest-run.sh all

load-fresh:
	$(CAFFEINATE) sh -c '$(MAKE) stack-reset && $(MAKE) obs-up && $(MAKE) load-all'

load-preflight:
	bash scripts/loadtest-run.sh preflight

load-breakpoint-hold:
	$(CAFFEINATE) bash scripts/loadtest-run.sh breakpoint-hold

load-breakpoint-journey:
	$(CAFFEINATE) bash scripts/loadtest-run.sh breakpoint-journey

load-breakpoint:
	$(CAFFEINATE) bash scripts/loadtest-run.sh breakpoint

load-steady:
	$(CAFFEINATE) bash scripts/loadtest-run.sh steady

load-sustained:
	$(CAFFEINATE) bash scripts/loadtest-run.sh sustained

load-sustained-x3:
	$(CAFFEINATE) bash scripts/loadtest-run.sh sustained-x3

load-soak:
	$(CAFFEINATE) bash scripts/loadtest-run.sh soak

load-report:
	bash scripts/loadtest-run.sh report

load-audit:
	bash scripts/loadtest-run.sh audit

jvm-check:
	bash scripts/check-jvm-flags.sh

# -----------------------------------------------------------------------------
# Kubernetes (k3d + Kustomize) targets
# -----------------------------------------------------------------------------
k8s-lint:
	bash scripts/k8s-lint.sh

k8s-images:
	bash scripts/k8s-images.sh

k8s-secrets:
	bash scripts/k8s-secrets.sh

k8s-up:
	bash scripts/k8s-up.sh

k8s-status:
	bash scripts/k8s-status.sh

k8s-forward:
	K8S_FORWARD_MAX_RETRIES=$${K8S_FORWARD_MAX_RETRIES:-120} bash scripts/k8s-forward.sh start

k8s-forward-stop:
	bash scripts/k8s-forward.sh stop

k8s-logs:
	@test -n "$(SERVICE)" || (echo "Usage: make k8s-logs SERVICE=<service-name>" >&2; exit 1)
	kubectl -n ticketing logs -l app.kubernetes.io/name=$(SERVICE) --all-containers=true --tail=200 -f

k8s-down:
	-kubectl -n ticketing delete deployment,statefulset,job,ingress,pvc --all --grace-period=0 --force >/dev/null 2>&1
	bash scripts/k8s-down.sh

k8s-destroy:
	bash scripts/k8s-cluster.sh destroy

k8s-redeploy:
	@test -n "$(SERVICE)" || (echo "Usage: make k8s-redeploy SERVICE=<service-name>" >&2; exit 1)
	bash scripts/k8s-redeploy.sh "$(SERVICE)"





