SHELL := /bin/bash
.DEFAULT_GOAL := help

# Auto-create .env from example if missing
.env:
	@cp .env.example .env
	@echo "Created .env from .env.example. Review before pushing or deploying."

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: .env ## Start all services (postgres, redis, app, worker)
	docker compose up -d

down: ## Stop all services
	docker compose down

build: ## (Re)build images
	docker compose build

rebuild: ## Force-rebuild without cache
	docker compose build --no-cache

logs: ## Tail logs (all services)
	docker compose logs -f --tail=100

logs-app: ## Tail app logs only
	docker compose logs -f --tail=100 app

logs-worker: ## Tail worker logs only
	docker compose logs -f --tail=100 worker

ps: ## Show service status
	docker compose ps

restart: ## Restart all services
	docker compose restart

shell-app: ## Bash into app container
	docker compose exec app bash

shell-pg: ## psql into postgres
	docker compose exec postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro}

shell-redis: ## redis-cli into redis
	docker compose exec redis redis-cli

verify-health: ## Curl /health endpoint
	@curl -fsS http://localhost:$${API_PORT:-8000}/health | python3 -m json.tool

verify-tsdb: ## Confirm TimescaleDB extension is loaded
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT extname, extversion FROM pg_extension WHERE extname='timescaledb';"

migrate: ## Apply all pending alembic migrations
	docker compose exec -T app alembic upgrade head

migrate-down: ## Roll back one migration step
	docker compose exec -T app alembic downgrade -1

migrate-status: ## Show current migration revision
	docker compose exec -T app alembic current

migrate-history: ## Show full migration history
	docker compose exec -T app alembic history --verbose

seed-calendar: ## Seed market_calendar (NYSE + crypto, 2017-2030)
	docker compose exec -T app python -m scripts.seed_market_calendar

backfill-equities: ## Backfill MSTR/MSTU/MSTY/MSTZ via yfinance (2017-today)
	docker compose exec -T app python -m scripts.backfill_equities

backfill-btc-dvol: ## Backfill BTC DVOL from Deribit (2020-today, clamped to ~2021-03 launch)
	docker compose exec -T app python -m scripts.backfill_btc_dvol

backfill-btc-daily: ## Backfill BTC-USD daily OHLCV from Coinbase (2017-today)
	docker compose exec -T app python -m scripts.backfill_btc_daily

backfill-mstr-holdings: ## Backfill MSTR BTC holdings from SEC EDGAR 8-K filings
	docker compose exec -T app python -m scripts.backfill_mstr_holdings

backfill-binance-funding: ## Backfill Binance BTC perp funding (2020-today, 8h cadence)
	docker compose exec -T app python -m scripts.backfill_binance_funding

backfill-hyperliquid-funding: ## Backfill Hyperliquid BTC perp funding (2023-today, 1h cadence)
	docker compose exec -T app python -m scripts.backfill_hyperliquid_funding

backfill-polygon-options: ## Backfill MSTR options chain (small slice; pass START/END for custom range)
	docker compose exec -T app python -m scripts.backfill_polygon_options $(if $(START),--start $(START)) $(if $(END),--end $(END))

backfill-polygon-options-2y: ## Full 2-year MSTR options backfill (~16h overnight)
	docker compose exec -T app python -m scripts.backfill_polygon_options --start $$(date -d '2 years ago' '+%Y-%m-%d') --end $$(date '+%Y-%m-%d')

verify-equities: ## Show equity coverage (counts, date range per ticker)
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT ticker, COUNT(*) AS days, MIN(ts) AS first, MAX(ts) AS last FROM equity_ohlcv GROUP BY ticker ORDER BY ticker;" \
		-c "SELECT ticker, type, COUNT(*) AS rows, MIN(ex_date) AS first, MAX(ex_date) AS last FROM distributions GROUP BY ticker, type ORDER BY ticker, type;"

verify-btc-dvol: ## Show DVOL coverage (count, date range, recent values)
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT COUNT(*) AS rows, MIN(ts)::date AS first, MAX(ts)::date AS last, ROUND(AVG(dvol)::numeric, 2) AS avg_dvol, ROUND(MIN(dvol)::numeric, 2) AS min_dvol, ROUND(MAX(dvol)::numeric, 2) AS max_dvol FROM btc_dvol;" \
		-c "SELECT ts::date, ROUND(dvol::numeric, 2) AS dvol FROM btc_dvol ORDER BY ts DESC LIMIT 5;"

verify-btc-daily: ## Show Coinbase BTC daily coverage
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT source, COUNT(*) AS days, MIN(date) AS first, MAX(date) AS last, ROUND(MIN(close)::numeric, 0) AS min_close, ROUND(MAX(close)::numeric, 0) AS max_close FROM btc_ohlcv_daily GROUP BY source ORDER BY source;" \
		-c "SELECT date, ROUND(close::numeric, 2) AS close, ROUND(volume::numeric, 1) AS volume FROM btc_ohlcv_daily ORDER BY date DESC LIMIT 5;"

verify-mstr-holdings: ## Show MSTR BTC holdings progression
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last, MAX(btc_qty) AS current_btc FROM mstr_btc_holdings;" \
		-c "SELECT date, btc_qty, ROUND((cumulative_cost/1e9)::numeric, 2) AS cost_usd_b, source_filing FROM mstr_btc_holdings ORDER BY date DESC LIMIT 10;"

verify-options: ## Show MSTR options coverage
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT COUNT(*) AS rows, COUNT(DISTINCT expiry) AS expiries, COUNT(DISTINCT strike) AS strikes, MIN(ts) AS first, MAX(ts) AS last FROM options_chain WHERE underlying='MSTR';" \
		-c "SELECT type, COUNT(*) AS rows FROM options_chain WHERE underlying='MSTR' GROUP BY type;" \
		-c "SELECT ts, expiry, strike, type, ROUND(last::numeric, 2) AS last, volume FROM options_chain WHERE underlying='MSTR' ORDER BY ts DESC, expiry, strike LIMIT 10;"

verify-funding: ## Show crypto perp funding coverage by venue
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT venue, symbol, COUNT(*) AS events, MIN(ts)::date AS first, MAX(ts)::date AS last, ROUND(AVG(funding_rate)::numeric * 1e4, 4) AS avg_rate_bp FROM crypto_perp_funding GROUP BY venue, symbol ORDER BY venue;" \
		-c "SELECT venue, ts, ROUND(funding_rate::numeric * 1e4, 4) AS rate_bp FROM crypto_perp_funding ORDER BY ts DESC LIMIT 10;"

verify-runs: ## Show last 10 ingestion runs
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT id, source, mode, status, rows_ingested, ROUND(EXTRACT(EPOCH FROM (ended_at - started_at))::numeric, 1) AS duration_s, started_at FROM ingestion_runs ORDER BY id DESC LIMIT 10;"

db-tables: ## List tables and hypertables
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "\dt" \
		-c "SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;"

db-reset: ## Roll back ALL migrations (destructive — confirms first)
	@read -p "Roll back ALL migrations? [y/N] " ans && \
		[ "$$ans" = "y" ] && docker compose exec -T app alembic downgrade base || echo "Aborted."

jupyter-up: .env ## Start Jupyter alongside core stack (research env)
	docker compose -f docker-compose.yml -f docker-compose.jupyter.yml up -d

jupyter-down: ## Stop Jupyter only
	docker compose -f docker-compose.yml -f docker-compose.jupyter.yml stop jupyter

clean: ## Stop and REMOVE volumes (DESTROYS DATA!)
	@read -p "This will delete all DB data. Continue? [y/N] " ans && \
		[ "$$ans" = "y" ] && docker compose down -v || echo "Aborted."

.PHONY: help up down build rebuild logs logs-app logs-worker ps restart \
        shell-app shell-pg shell-redis verify-health verify-tsdb \
        migrate migrate-down migrate-status migrate-history \
        seed-calendar \
        backfill-equities backfill-btc-dvol backfill-btc-daily backfill-mstr-holdings \
        backfill-polygon-options backfill-polygon-options-2y \
        backfill-binance-funding backfill-hyperliquid-funding \
        verify-equities verify-btc-dvol verify-btc-daily verify-mstr-holdings \
        verify-options verify-funding verify-runs \
        db-tables db-reset \
        jupyter-up jupyter-down clean
