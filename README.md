# DuckDB vs. MotherDuck TPC-H Benchmark

Replication package for:

> **"Isolating Deployment Effects in In-Process Analytics: 
> A Controlled Benchmark of DuckDB Local vs. MotherDuck Using TPC-H"**  
> Stephen Edlin, Jarot S. Suroso — Pradita University, 2025

## Repository Contents

| File | Description |
|------|-------------|
| `benchmark_duckdb_v2.py` | Benchmark script for DuckDB local execution |
| `motherduck_benchmark_v4.py` | Benchmark script for MotherDuck cloud execution |
| `duckdb_results.csv` | Raw results — 1,200 runs (DuckDB Local) |
| `motherduck_results.csv` | Raw results — 1,200 runs (MotherDuck) |

## Experimental Setup

- **Engine:** DuckDB v1.2.2 (both platforms)
- **Workload:** 15 TPC-H-derived queries × 4 scale factors (SF1, SF5, SF10, SF20)
- **Runs:** 10 measured runs + 1 warm-up per query-SF pair
- **Local hardware:** Intel Core i5-12400F, 16 GB DDR4-3200, NVMe PCIe Gen3
- **Cloud platform:** MotherDuck Business Plan, EU region (aws-eu-central-1)

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
python benchmark_duckdb_v2.py

# MotherDuck benchmark  
python motherduck_benchmark_v4.py
```

## TPC-H Data

Generate using the official TPC-H dbgen tool at scale factors 1, 5, 10, 20.  
Convert to Parquet format before running benchmarks.
