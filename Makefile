.PHONY: build rebuild start stop attach logs status clean discovery-build discovery-test discovery-test-raydium discovery-test-raydium-clmm discovery-test-meteora discovery-test-binance discovery-test-bybit discovery-test-okx discovery-test-bitget discovery-test-gateio

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
