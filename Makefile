.PHONY: build run stop status logs clean

CEX_BUILD_DIR := $(CURDIR)/build
DEX_DIR := $(CURDIR)/ingestor-dex-solana

CEX_PID_FILE := /tmp/zora_cex.pid
DEX_PID_FILE := /tmp/zora_dex.pid
SUPERVISOR_PID_FILE := /tmp/zora_supervisor.pid
LOG_FILE := /tmp/zora.log

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

run:
	@nohup bash "$(CURDIR)/scripts/start.sh" >/dev/null 2>&1 &
	@sleep 1
	@echo "ZORA starting in the background (a fresh build can take a minute)."
	@echo "Use 'make logs' to follow progress, 'make status' to check, 'make stop' to stop."

stop:
	@bash "$(CURDIR)/scripts/stop.sh"

status:
	@echo "--- ZORA status ---"; \
	check() { \
		if [ -f "$$2" ] && kill -0 "$$(cat $$2)" 2>/dev/null; then \
			printf "%-12s running (PID %s)\n" "$$1" "$$(cat $$2)"; \
		else \
			printf "%-12s not running\n" "$$1"; \
		fi; \
	}; \
	check "Supervisor:" "$(SUPERVISOR_PID_FILE)"; \
	check "CEX:" "$(CEX_PID_FILE)"; \
	check "DEX:" "$(DEX_PID_FILE)"

logs:
	@touch $(LOG_FILE)
	@tail -f $(LOG_FILE)

clean:
	@echo "Cleaning C++ build artifacts..."
	@rm -rf $(CEX_BUILD_DIR)
	@echo "Cleaning Rust build artifacts..."
	@cd $(DEX_DIR) && cargo clean
	@cargo clean --manifest-path spread-detector/Cargo.toml
