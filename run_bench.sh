#!/bin/bash
set -e
# Cargar el entorno de Rust en WSL
source $HOME/.cargo/env
cd /mnt/d/zenozaga/Github/accounts/zenozaga/projects/@automatizadovip/capabilities/arbitro-io/arbitro-raft
cargo bench --bench async_tcp_bench --no-run
BIN=$(ls -t target/release/deps/async_tcp_bench-* | grep -v "\.d$" | head -n 1)
cp "$BIN" /tmp/async_tcp_bench
chmod +x /tmp/async_tcp_bench
# Ejecución rápida
/tmp/async_tcp_bench --bench --warm-up-time 1 --measurement-time 2
