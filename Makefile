# Raft Performance Benchmarking
# Follows Hardware Sympathy rules: Compile in Release, run from RAM-disk (/tmp)

SHELL := /bin/bash
CARGO := $(HOME)/.cargo/bin/cargo
TARGET_DIR := /tmp/arbitro

.PHONY: bench clean

bench:
	@echo "--- Building TCP Benchmark (Release) ---"
	$(CARGO) build --release --bench tcp_raft_bench
	@echo "--- Preparing RAM-disk environment ---"
	mkdir -p $(TARGET_DIR)
	@BIN=$$(ls -t target/release/deps/tcp_raft_bench-* | grep -v '\.d' | head -n 1); \
	echo "Copying binary $$BIN to $(TARGET_DIR)/bench"; \
	cp $$BIN $(TARGET_DIR)/bench
	@echo "--- Executing Benchmark from $(TARGET_DIR) ---"
	cd $(TARGET_DIR) && ./bench --bench

clean:
	rm -rf $(TARGET_DIR)
	cargo clean
