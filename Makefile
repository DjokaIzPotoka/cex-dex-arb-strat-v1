.PHONY: build rebuild start stop attach clean

CEX_BUILD_DIR := $(CURDIR)/build
DEX_DIR := $(CURDIR)/ingestor-dex-solana

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

rebuild: clean build

start:
	@bash "$(CURDIR)/scripts/zora.sh"

stop:
	@bash "$(CURDIR)/scripts/stop.sh"

attach:
	@tmux attach -t zora

clean:
	@echo "Cleaning C++ build artifacts..."
	@rm -rf $(CEX_BUILD_DIR)
	@echo "Cleaning Rust build artifacts..."
	@cd $(DEX_DIR) && cargo clean
