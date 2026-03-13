# TPC-H Q13 Benchmark

A fast single-query processor for TPC-H Query 13, benchmarked against DuckDB.

## Results

Average query time (ms), single-threaded:

| Engine  | SF 0.5 | SF 1   | SF 2   | SF 5    |
|---------|--------|--------|--------|---------|
| Custom  | 68.14  | 135.99 | 273.72 | 687.56  |
| DuckDB  | 121.76 | 258.63 | 554.40 | 1470.22 |
| Speedup | 1.79×  | 1.90×  | 2.03×  | 2.14×   |

## Optimizations

- Early-exit string filter — reject_comment checks length < 16 upfront, then scans for 's' guard char before memcmp("special", 7), reducing full string scans
- Flat array aggregation — uses counts[] instead of a hash map for O(1) indexing
- Cache-aligned arrays — alignas(64) avoids false sharing and aligns to cache lines
- Smaller arrays — int8_t counts[] reduces cache misses by shrinking the scatter array
- Branchless counting — counts[custkey] += !reject_comment(view) avoids branch mispredictions

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

Run 1 is a warmup and excluded from the average.

## Validation

Place DuckDB reference CSVs in `validation/` (e.g. `validation/duckdb_sf1.csv`), then run:

```bash
python test.py
```

This auto-discovers all `results/result_sf*.csv` files and checks each against its corresponding `validation/duckdb_sf*.csv`, reporting any mismatches:

```
[results/result_sf1.csv] Output matches DuckDB ✅
[results/result_sf5.csv] Mismatch at c_count=3: my=4150 duck=4200 ❌
```

## Output Format

```
c_count,custdist
0,5000
1,4200
...
```
