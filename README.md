# TPC-H Q13 Benchmark

A fast single-threaded C++ implementation of TPC-H Query 13 (customer order distribution), benchmarked against DuckDB.

## Query

Computes the distribution of customers by their number of non-special orders:

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
- Python 3 + `duckdb` (for Python benchmark)

## Data

Place Parquet files under `data/sf{N}/`:
```
data/
  sf1/
    customer.parquet
    orders.parquet
  sf5/
    ...
```

## Usage

**C++ engine:**
```bash
./run.sh --data data/sf1 --out result.csv
./run.sh --data data/sf1 --out result.csv --benchmark   # 1 warmup + 5 measured runs
```

**DuckDB benchmark:**
```bash
python duckdb.py   # runs SF 0.5, 1, 2, 5
```

## Output

Results are written to a CSV file:
```
c_count,custdist
0,5000
1,4200
...
```

Benchmark mode also prints per-run timings broken down into read, loop, and customer scan phases.
