# DuckDB vs. MotherDuck TPC-H Benchmark

Replication package for:

> **"Isolating Deployment Effects in In-Process Analytics:
> A Controlled Benchmark of DuckDB Local vs. MotherDuck Using TPC-H"**
> Stephen Edlin, Jarot S. Suroso — Pradita University, 2025

## Repository Contents

| File | Description |
|------|-------------|
| `benchmark_duckdb_v3.py` | Benchmark script for DuckDB local execution |
| `motherduck_benchmark_v6.py` | Benchmark script for MotherDuck cloud execution |
| `duckdb_results.csv` | Raw results — 1,800 rows / 30 runs (DuckDB Local) |
| `motherduck_results.csv` | Raw results — 1,800 rows / 30 runs (MotherDuck) |

## Repository Structure

duckdb-motherduck-tpch-benchmark/
├── benchmark_duckdb_v3.py           # Local benchmark script
├── motherduck_benchmark_v6.py       # Cloud benchmark script
├── duckdb_results.csv               # Raw results - DuckDB Local (1,800 rows, n=30)
├── motherduck_results.csv           # Raw results - MotherDuck (1,800 rows, n=30)
└── logs/
├── log_DuckDB.log    # Full execution log with timestamps
└── log_MotherDuck.log      # Full execution log with timestamps


Log files include per-run timestamps, platform info (OS, Python, DuckDB version),
RAM availability, cooling intervals, cache flush status, and progress tracking
per scale factor — enabling full audit trail for reproducibility review.

## Experimental Setup

- **Engine:** DuckDB v1.2.2 (both platforms)
- **Workload:** 15 TPC-H-derived queries × 4 scale factors (SF1, SF5, SF10, SF20)
- **Runs:** 30 measured runs + 1 warm-up per query-SF pair (3,600 total executions per platform)
- **Local hardware:** Intel Core i5-12400F, 16 GB DDR4-3200, NVMe PCIe Gen3
- **Cloud platform:** MotherDuck Business Plan, EU region (aws-eu-central-1)
- **Statistical analysis:** Mann-Whitney U test, Bonferroni correction (adjusted α ≈ 0.0008), rank-biserial r effect size

## Key Findings

- SF1: DuckDB local 3.55× faster (fixed cloud dispatch overhead ~0.170s)
- SF10: Crossover point — MotherDuck faster in 10/15 queries
- SF20: MotherDuck faster in 14/15 queries (mean 2.54× advantage)
- Q18_window anomaly: DK scalability 62.21× vs MD 5.83× (RAM exhaustion + disk spill)

## Requirements

```bash
pip install duckdb pandas psutil
```

For MotherDuck, set your token:
```bash
$env:MOTHERDUCK_TOKEN="your_token_here"
```

## Usage

```bash
# Local benchmark
python benchmark_duckdb_v3.py

# MotherDuck benchmark
python motherduck_benchmark_v6.py
```

## TPC-H Data

Generate using the official TPC-H dbgen tool at scale factors 1, 5, 10, 20.
Convert to Parquet format before running benchmarks.
