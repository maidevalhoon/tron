# TRON Validation & Benchmarking Suite

This directory contains the automated validation and benchmarking harnesses for **TRON** (FastCDC + IBLT + Gossip File Synchronization).

## Prerequisites

* **Go**: Version 1.22 or newer
* **OS**: Linux / macOS (tested on Linux x86_64)
* **Storage**: At least 1.5 GB free disk space for temporary benchmark files (cleaned up automatically)

---

## Directory Structure

```
bench/
├── README.md               # Benchmark documentation & reproduction instructions
└── results/                # Recorded benchmark output CSVs
    ├── bandwidth_benchmark.csv    # Bandwidth & latency scaling (10MB, 100MB, 500MB)
    ├── failure_testing.csv        # Fault tolerance & resilience test results
    ├── fastcdc_validation.csv     # FastCDC vs Fixed Chunking boundary-shift data
    ├── iblt_validation.csv        # IBLT peeling success rates vs delta counts
    └── scalability_benchmark.csv  # Multi-node cluster gossip convergence (2 to 100 nodes)
```

---

## Quick Start: Running Benchmarks

### 1. Run All Tests and Benchmarks
You can execute the entire suite using `make`:

```bash
# Run all unit and integration correctness tests
make test

# Run all benchmark suites and generate CSVs in bench/results/
make bench

# Regenerate visual SVG charts in docs/results/
make charts
```

### 2. Run Individual Benchmarks

Each benchmark is an independent, self-contained Go CLI program located in `cmd/`:

#### Bandwidth & Delta Scaling Benchmark
Tests synchronization latency, bytes transferred, and percentage savings across 10 MB, 100 MB, and 500 MB files with deltas ranging from 1 byte to 10 MB:
```bash
go run ./cmd/validate_bandwidth
```
*Output:* `bench/results/bandwidth_benchmark.csv`

#### FastCDC vs. Fixed-Size Chunking Benchmark
Demonstrates boundary-shift resistance by comparing FastCDC chunk reusability against fixed 16 KB chunking upon 1-byte and 100-byte insertions:
```bash
go run ./cmd/validate_cdc
```
*Output:* `bench/results/fastcdc_validation.csv`

#### Invertible Bloom Lookup Table (IBLT) Peeling Benchmark
Evaluates IBLT peel success rate, peeling duration, and knotting limits across delta item counts from 0 to 1,000:
```bash
go run ./cmd/validate_iblt
```
*Output:* `bench/results/iblt_validation.csv`

#### Cluster Scalability Benchmark
Simulates epidemic gossip synchronization across 2, 5, 10, 25, 50, and 100 nodes, measuring cluster convergence latency and gossip payload overhead:
```bash
go run ./cmd/validate_scalability
```
*Output:* `bench/results/scalability_benchmark.csv`

#### Fault Tolerance & Network Failure Benchmark
Executes 6 resilience scenarios:
1. Mid-Transfer Receiver Crash & Resume
2. Mid-Transfer Sender Crash & Recovery
3. Network Disconnection & Catch-Up Sync
4. Duplicate Gossip Event Deduplication
5. Split-Brain Divergence & Lamport Resolution
6. Out-of-Order Packet Delivery
```bash
go run ./cmd/validate_failures
```
*Output:* `bench/results/failure_testing.csv`

#### Chart Generator
Generates publication-quality SVG vector charts illustrating the empirical results:
```bash
go run ./cmd/generate_charts
```
*Outputs in `docs/results/`:*
* `bandwidth_comparison.svg`
* `cdc_boundary_shift.svg`
* `iblt_peel_rate.svg`
* `scalability_convergence.svg`

---

## Metric Definitions

| Field Name | Description | Unit |
| :--- | :--- | :--- |
| `File Size` | Total uncompressed file size | Bytes / MB |
| `Delta Size` | Number of bytes modified or inserted | Bytes / KB / MB |
| `Chunks Reused` | Percentage of chunks preserved without retransfer | Percentage (%) |
| `Bytes Transferred` | Total network payload exchanged to synchronize | Bytes / KB / MB |
| `Bandwidth Savings` | $(1 - \frac{\text{Bytes Transferred}}{\text{File Size}}) \times 100$ | Percentage (%) |
| `Sync Latency` | Wall-clock time from write trigger to full peer integrity verification | Milliseconds / Seconds |
| `SHA256 Match` | Bitwise verification between source and replica files | Boolean (`true`/`false`) |
