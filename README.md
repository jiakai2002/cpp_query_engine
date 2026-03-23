# TPC-H Q13 Benchmark

A fast single-query processor for TPC-H Query 13, benchmarked against DuckDB.

## Hardware

- **Machine**: Apple iMac (2021)
- **CPU**: Apple M1 (4 performance cores, 4 efficiency cores)
- **RAM**: 8 GB unified memory
- **Cache**: 128 KB L1 data cache per core, 12 MB L2 shared
- **OS**: macOS Sonoma 14.5

## Results

Average query time (ms), single-threaded (1 warmup + 5 measured runs):

| Engine  | SF 0.5 | SF 1   | SF 2   | SF 5   |
|---------|--------|--------|--------|--------|
| Custom  | 68.26  | 135.63 | 273.39 | 693.52 |
| DuckDB  | 173.00 | 321.00 | 640.00 | 1623.00 |
| Speedup | 2.53×  | 2.37×  | 2.34×  | 2.34×  |

### Time breakdown (SF 1, average across 5 runs)

| Phase         | Time (ms) |
|---------------|-----------|
| Orders read   | 68.44     |
| Orders loop   | 65.76     |
| Customer scan | 1.22      |
| **Total**     | **135.63** |

Read and loop time are nearly equal at every scale factor (~51% / 49% split), indicating the workload is evenly balanced between Parquet I/O and computation — a strong candidate for read/compute pipelining.

## Optimizations

- Early-exit string filter — `reject_comment` scans for `'s'` before `memcmp("special", 7)`, then searches for `"requests"` only after a confirmed `"special"` match; early-exits on `len < 16`
- Flat array aggregation — uses `counts[]` instead of a hash map for O(1) indexing by custkey
- Smaller element type — `int8_t counts[750001]` (~730 KB) keeps the entire scatter array within L2 cache so all random writes hit cache
- Cache-aligned arrays — `alignas(64)` avoids false sharing and aligns to cache line boundaries
- Branchless counting — `counts[custkey] += !reject_comment(...)` avoids branch mispredictions on the hot path

## Query

```sql
SELECT c_count, COUNT(*) AS custdist
FROM (
    SELECT c_custkey, COUNT(o_orderkey) AS c_count
    FROM customer LEFT JOIN orders
    ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) GROUP BY c_count ORDER BY custdist DESC, c_count DESC;
```

## Requirements

- C++20, `g++`
- Apache Arrow + Parquet (`pkg-config` accessible)
- Python 3 + `duckdb` (for benchmarking and validation)

## Data

Place Parquet files under `data/sf{N}/`:
```
data/
  sf1/
    customer.parquet
    orders.parquet
```

## Usage

```bash
# Run query — prints result table, writes results/result_sf1.csv
./run.sh --data data/sf1

# Benchmark mode — 1 warmup + 5 measured runs, prints per-run and average timings
./run.sh --data data/sf1 --benchmark
```

The output CSV is named after the data folder (e.g. `data/sf1` → `results/result_sf1.csv`).

## DuckDB Benchmark

Runs the same query via DuckDB (single-threaded) across SF 0.5, 1, 2, 5 and prints a timing table:

```bash
pip install duckdb
python duckdb_benchmark.py
```

Output:
```
SF | Run 1    Run 2    Run 3    Run 4    Run 5    Run 6  | Avg (ms)
```

## Validation

```bash
python test.py
```

This finds all `results/result_sf*.csv` files and checks each against its corresponding `validation/duckdb_sf*.csv`, reporting any mismatches:

```
[results/result_sf1.csv] Output matches DuckDB ✅
[results/result_sf5.csv] Mismatch at c_count=3: my=4150 duck=4200 ❌
```
