# TPC-H Q13 Benchmark

A fast single-query processor for TPC-H Query 13, benchmarked against DuckDB.

## Hardware

- **Machine**: Apple iMac (2021)
- **CPU**: Apple M1 (4 performance cores, 4 efficiency cores)
- **RAM**: 8 GB unified memory
- **Cache**: 128 KB L1 data cache per core, 12 MB L2 shared
- **OS**: macOS Sonoma 14.5

## Results

Average query time (ms), single-threaded:

| Engine  | SF 0.5 | SF 1   | SF 2   | SF 5   |
|---------|--------|--------|--------|--------|
| Custom  | 62.59  | 125.51 | 251.06 | 631.10 |
| DuckDB  | 121.76 | 258.63 | 554.40 | 1470.22 |
| Speedup | 1.94×  | 2.06×  | 2.21×  | 2.33×  |

## Optimizations

- Early-exit string filter — `reject_comment` checks `len < 23` upfront then scans for `'s'` before `memcmp("special", 7)`, — avoiding full string scan for 98.6% of rows that don't match
- Flat array aggregation — uses `counts[]` instead of a hash map for O(1) indexing by custkey
- Smaller element type — int8_t counts[750001] (~730 KB) keeps the entire scatter array within L2 cache so all 1.5M random writes hit cache
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
