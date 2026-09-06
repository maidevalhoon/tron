.PHONY: all build test bench bench-cdc bench-iblt bench-bandwidth bench-scalability bench-failures charts fmt clean help

# Default target
all: fmt test build

help:
	@echo "TRON Build and Benchmark Targets:"
	@echo "  make build             - Compile main sync daemon binary"
	@echo "  make test              - Run all unit and correctness test suites"
	@echo "  make bench             - Run all validation benchmarks and generate CSVs"
	@echo "  make bench-cdc         - Validate FastCDC vs Fixed Chunking"
	@echo "  make bench-iblt        - Validate IBLT peeling and capacity limits"
	@echo "  make bench-bandwidth   - Benchmark delta bandwidth scaling (10MB, 100MB, 500MB)"
	@echo "  make bench-scalability - Benchmark cluster gossip scalability (2 to 100 nodes)"
	@echo "  make bench-failures    - Validate fault tolerance and failure recovery"
	@echo "  make charts            - Generate SVG benchmark charts in docs/results/"
	@echo "  make fmt               - Run go fmt across all packages"
	@echo "  make clean             - Clean temporary artifacts and cache"

build:
	@mkdir -p bin
	go build -o bin/sync-node ./cmd/sync

test:
	go test -v -race ./tests/...

bench: bench-cdc bench-iblt bench-bandwidth bench-scalability bench-failures charts
	@echo "All benchmarks completed. Results in bench/results/ and charts in docs/results/"

bench-cdc:
	go run ./cmd/validate_cdc

bench-iblt:
	go run ./cmd/validate_iblt

bench-bandwidth:
	go run ./cmd/validate_bandwidth

bench-scalability:
	go run ./cmd/validate_scalability

bench-failures:
	go run ./cmd/validate_failures

charts:
	go run ./cmd/generate_charts

fmt:
	go fmt ./...

clean:
	rm -rf bin/ .sync_cache/ node1_dir/ node2_dir/ *.log sync-experiment sync-node
