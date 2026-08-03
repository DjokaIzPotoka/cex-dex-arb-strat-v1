.PHONY: build rebuild start stop attach logs status clean

CEX_BUILD_DIR := $(CURDIR)/build
DEX_DIR := $(CURDIR)/ingestor-dex-solana

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
