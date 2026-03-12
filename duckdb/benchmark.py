# duckdb_benchmark_q13_csv.py
import duckdb
import pandas as pd
import time
import os

# --- Configuration ---
SF_LIST = [0.5, 1, 2, 5]
BASE_DATA_DIR = "../data"
DUCKDB_RESULTS_DIR = "results_duckdb"
os.makedirs(DUCKDB_RESULTS_DIR, exist_ok=True)

WARMUP_RUNS = 1
MEASURE_RUNS = 5

QUERY_TEMPLATE = """
SELECT c_count, count(*) AS custdist
FROM (
    SELECT c_custkey, count(o_orderkey) AS c_count
    FROM customer
    LEFT OUTER JOIN orders
    ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
"""

# --- Benchmark function ---
def benchmark_sf(sf):
    data_dir = os.path.join(BASE_DATA_DIR, f"sf{sf}")
    customer_file = os.path.join(data_dir, "customer.parquet")
    orders_file = os.path.join(data_dir, "orders.parquet")

    print(f"=== Processing {sf} ===")
    con = duckdb.connect(database=':memory:')

    # Register parquet files as views
    con.execute(f"CREATE VIEW customer AS SELECT * FROM '{customer_file}'")
    con.execute(f"CREATE VIEW orders AS SELECT * FROM '{orders_file}'")

    # Warmup
    for _ in range(WARMUP_RUNS):
        con.execute(QUERY_TEMPLATE).fetchall()

    # Measured runs
    run_times = []
    last_result = None
    for run in range(1, MEASURE_RUNS + 1):
        start = time.perf_counter()
        last_result = con.execute(QUERY_TEMPLATE).fetchdf()
        end = time.perf_counter()
        elapsed_ms = (end - start) * 1000
        run_times.append(elapsed_ms)
        print(f"Run {run}: {elapsed_ms:.2f} ms, rows={len(last_result)}")

    avg_time = sum(run_times) / len(run_times)
    print(f"SF={sf} Average: {avg_time:.2f} ms\n")

    # Save last run result to CSV
    out_file = os.path.join(DUCKDB_RESULTS_DIR, f"result_sf{sf}.csv")
    last_result.to_csv(out_file, index=False)
    print(f"Saved DuckDB results to {out_file}\n")


# --- Main ---
if __name__ == "__main__":
    for sf in SF_LIST:
        benchmark_sf(sf)