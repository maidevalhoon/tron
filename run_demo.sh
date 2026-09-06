#!/usr/bin/env bash
set -e

# TRON Judge-Ready Demonstration Script
# Usage:
#   ./run_demo.sh wow      - Run the 100 MB 1-byte WOW demo (99.98% bandwidth savings)
#   ./run_demo.sh scale    - Run the 100-node cluster scalability demo (<30ms convergence)
#   ./run_demo.sh failure  - Run the node crash, disconnect & catch-up recovery demo
#   ./run_demo.sh cdc      - Run FastCDC vs Fixed Chunking boundary-shift demo
#   ./run_demo.sh all      - Run all 4 demos back-to-back (default)

MODE="${1:-all}"
go run ./cmd/demo -mode "$MODE"
