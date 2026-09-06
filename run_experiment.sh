#!/usr/bin/env bash
set -e

echo "Building project binaries..."
go build -o sync-node ./cmd/sync
go build -o sync-experiment ./cmd/experiment

echo ""
echo "Running the 100 MB Quantitative Benchmark across all change deltas (1B to 50MB)..."
./sync-experiment
