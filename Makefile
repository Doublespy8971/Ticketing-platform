COMPOSE := docker compose -f infra/docker-compose.yml
HEALTHY_SERVICES := kafka postgres-inventory postgres-order postgres-payment redis

.PHONY: infra-up infra-down infra-reset infra-topics infra-psql-inventory infra-psql-order infra-psql-payment infra-redis-cli smoke-inventory

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
