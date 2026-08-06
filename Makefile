.PHONY: build rebuild start stop attach logs status clean discovery-build discovery-test discovery-test-raydium discovery-test-raydium-clmm discovery-test-meteora discovery-test-binance discovery-test-bybit discovery-test-okx discovery-test-bitget discovery-test-gateio discovery-test-entity-detection discovery-test-rpc-crosscheck discovery-start discovery-stop discovery-status discovery-attach discovery-dashboard

CEX_BUILD_DIR := $(CURDIR)/build
DEX_DIR := $(CURDIR)/ingestor-dex-solana
DISCOVERY_DIR := $(CURDIR)/discovery
DISCOVERY_VENV := $(DISCOVERY_DIR)/.venv

CEX_PID_FILE := /tmp/zora_cex.pid
DEX_PID_FILE := /tmp/zora_dex.pid
SPR_PID_FILE := /tmp/zora_spr.pid
CEX_LOG := /tmp/zora_cex.log
DEX_LOG := /tmp/zora_dex.log
SPR_LOG := $(CURDIR)/logs/zora_spr.log

DISCOVERY_SCHED_PID_FILE := /tmp/zora_discovery_sched.pid
DISCOVERY_SCHED_LOG := $(CURDIR)/logs/zora_discovery_sched.log

build:
	@echo "Building CEX ingestor (C++, release)..."
	@mkdir -p $(CEX_BUILD_DIR)
	@if command -v brew >/dev/null 2>&1; then \
		cd $(CEX_BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$$(brew --prefix)" ..; \
	else \
		cd $(CEX_BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Release ..; \
	fi
	@cmake --build $(CEX_BUILD_DIR)
	@echo "Building DEX ingestor (Rust, release)..."
	@cd $(DEX_DIR) && cargo build --release
	@echo "Building spread detector (Rust, release)..."
	@cargo build --release --manifest-path spread-detector/Cargo.toml

rebuild: clean build

start:
	@bash "$(CURDIR)/scripts/zora.sh"

stop:
	@bash "$(CURDIR)/scripts/stop.sh"

attach:
	@tmux attach -t zora

logs:
	@echo "ZORA logs:"; \
	echo "  1) CEX  ($(CEX_LOG))"; \
	echo "  2) DEX  ($(DEX_LOG))"; \
	echo "  3) SPR  ($(SPR_LOG))"; \
	echo "  4) SPR  (attach to the live tmux session instead)"; \
	read -p "Select: " choice; \
	case "$$choice" in \
		1) tail -f $(CEX_LOG) ;; \
		2) tail -f $(DEX_LOG) ;; \
		3) tail -f $(SPR_LOG) ;; \
		4) tmux attach -t zora ;; \
		*) echo "Cancelled." ;; \
	esac

status:
	@echo "--- ZORA status ---"; \
	check() { \
		if [ -f "$$2" ] && kill -0 "$$(cat $$2)" 2>/dev/null; then \
			printf "%-6s running (PID %s)\n" "$$1" "$$(cat $$2)"; \
		else \
			printf "%-6s not running\n" "$$1"; \
		fi; \
	}; \
	check "CEX:" "$(CEX_PID_FILE)"; \
	check "DEX:" "$(DEX_PID_FILE)"; \
	check "SPR:" "$(SPR_PID_FILE)"

clean:
	@echo "Cleaning C++ build artifacts..."
	@rm -rf $(CEX_BUILD_DIR)
	@echo "Cleaning Rust build artifacts..."
	@cd $(DEX_DIR) && cargo clean
	@cargo clean --manifest-path spread-detector/Cargo.toml

# --- MEV discovery subsystem (Phase 0 + 1a, see docs/mev-discovery-plan.md) ---
# Separate from the C++/Rust ZORA paper-trading pipeline above: a
# one-shot Python script + ClickHouse, not (yet) a tmux-managed service.
#
# Uses podman + podman-compose, not docker: rootless, no background
# daemon required. `podman compose` (space, not hyphen) is podman's own
# subcommand -- it shells out to the podman-compose binary as an
# "external compose provider" rather than requiring Docker's compose
# plugin, which is what makes `docker-compose.yml` work unmodified here.
DISCOVERY_COMPOSE := podman compose -f $(DISCOVERY_DIR)/docker-compose.yml

discovery-build:
	@echo "Setting up discovery Python venv..."
	@python3 -m venv $(DISCOVERY_VENV)
	@$(DISCOVERY_VENV)/bin/pip install -q --upgrade pip
	@$(DISCOVERY_VENV)/bin/pip install -q -r $(DISCOVERY_DIR)/requirements.txt
	@echo "Starting ClickHouse (podman compose)..."
	@$(DISCOVERY_COMPOSE) up -d
	@echo "Waiting for ClickHouse to accept connections..."
	@for i in $$(seq 1 30); do \
		if curl -s -o /dev/null "http://127.0.0.1:8123/ping"; then break; fi; \
		sleep 1; \
	done
	@echo "Applying migrations..."
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/migrations/run_migrations.py

discovery-test:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_orca_snapshot.py

# Separate target rather than folding into discovery-test: this one
# also cross-checks against live Solana RPC (see
# run_raydium_snapshot.py), so it needs SOLANA_RPC_URL from .env the
# same way scripts/zora.sh loads it for the Rust DEX ingestor -- falls
# back to the public endpoint if .env doesn't exist.
discovery-test-raydium:
	@set -a; [ -f "$(CURDIR)/.env" ] && . "$(CURDIR)/.env"; set +a; \
	$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_raydium_snapshot.py

# No .env sourcing needed here, unlike discovery-test-raydium: CLMM
# validation cross-checks against Binance's public ticker instead of
# on-chain RPC (see run_raydium_clmm_snapshot.py's docstring for why),
# so there's no SOLANA_RPC_URL involved at all.
discovery-test-raydium-clmm:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_raydium_clmm_snapshot.py

# Same as discovery-test-raydium-clmm: no .env/SOLANA_RPC_URL involved,
# validation cross-checks against Binance's public ticker instead.
discovery-test-meteora:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_meteora_snapshot.py

# First CEX provider -- no SOLANA_RPC_URL involved (CEX-only, no
# Solana RPC call exists in this path at all).
discovery-test-binance:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_binance_snapshot.py

# Remaining four CEX providers -- same no-SOLANA_RPC_URL note as
# discovery-test-binance applies to all of these.
discovery-test-bybit:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_bybit_snapshot.py

discovery-test-okx:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_okx_snapshot.py

discovery-test-bitget:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_bitget_snapshot.py

discovery-test-gateio:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_gateio_snapshot.py

# Detection pass over whatever's currently in dex_pool_snapshots /
# cex_symbol_snapshots -- run the provider snapshot targets above at
# least once first, otherwise this has nothing to check.
discovery-test-entity-detection:
	@$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_entity_detection.py

# --- Tier 2 RPC PoolStateProvider (Phase 1h) ---
# Three-way cross-check (REST vs. live Rust production code vs. this
# new Python RPC provider) for solana_rpc_state.py -- see that
# module's docstring and run_rpc_state_crosscheck.py's own docstring.
# Needs the Rust validation binary built first (not part of the normal
# `make build`/`make rebuild` targets -- see that binary's own
# docstring for why it's standalone), and SOLANA_RPC_URL from .env
# (falls back to the public endpoint otherwise, same as
# discovery-test-raydium).
discovery-test-rpc-crosscheck:
	@cd "$(CURDIR)/ingestor-dex-solana" && cargo build --release --bin live_price_check
	@set -a; [ -f "$(CURDIR)/.env" ] && . "$(CURDIR)/.env"; set +a; \
	$(DISCOVERY_VENV)/bin/python3 $(DISCOVERY_DIR)/src/run_rpc_state_crosscheck.py

# --- Tier 1 scheduler (Phase 1e) ---
# Same naming/behavior convention as start/stop/status/attach above,
# in its own 'zora-discovery' tmux session (see scripts/zora_discovery.sh)
# so it starts, stops, and status-checks independently of the CEX/DEX/
# spread-detector paper-trading session. Assumes `make discovery-build`
# already provisioned the venv and ClickHouse, same assumption every
# discovery-test-* target above already makes.
discovery-start:
	@bash "$(CURDIR)/scripts/zora_discovery.sh"

discovery-stop:
	@bash "$(CURDIR)/scripts/stop_discovery.sh"

discovery-attach:
	@tmux attach -t zora-discovery

discovery-status:
	@echo "--- ZORA discovery status ---"; \
	if [ -f "$(DISCOVERY_SCHED_PID_FILE)" ] && kill -0 "$$(cat $(DISCOVERY_SCHED_PID_FILE))" 2>/dev/null; then \
		printf "%-6s running (PID %s)\n" "SCHED:" "$$(cat $(DISCOVERY_SCHED_PID_FILE))"; \
	else \
		printf "%-6s not running\n" "SCHED:"; \
	fi

# --- Ops dashboard (Phase 1i) ---
# Read-only Streamlit observer for the running scheduler/48h test --
# see discovery/ops_dashboard.py's own docstring for the read-only
# guarantee. Reachable the same way as ClickHouse's own Play UI
# (0.0.0.0, fixed port), independent of whether the scheduler itself
# is running (it'll just show a red health badge / "no data" if not).
# Foreground process, same as `discovery-attach` -- Ctrl-C to stop it;
# it never needs its own PID file/tmux session since it controls
# nothing and there's nothing to leave running unattended.
DISCOVERY_DASHBOARD_PORT := 8501
discovery-dashboard:
	@$(DISCOVERY_VENV)/bin/streamlit run "$(DISCOVERY_DIR)/ops_dashboard.py" \
		--server.port $(DISCOVERY_DASHBOARD_PORT) --server.address 0.0.0.0 --server.headless true
