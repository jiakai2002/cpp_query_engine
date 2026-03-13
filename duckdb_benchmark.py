import duckdb
import time

# Scale factors to benchmark
scale_factors = [0.5, 1, 2, 5]

# Query template
query_template = """
SELECT c_count, COUNT(*) AS custdist
FROM (
    SELECT c.c_custkey, COUNT(o.o_orderkey) AS c_count
    FROM read_parquet('data/sf{sf}/customer.parquet') AS c
    LEFT OUTER JOIN read_parquet('data/sf{sf}/orders.parquet') AS o
    ON c.c_custkey = o.o_custkey
    AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
"""

TOTAL_RUNS = 6  # first run is warmup

def benchmark_query(con, query):
    times = []
    for run in range(TOTAL_RUNS):
        start = time.perf_counter()
        con.execute(query)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # ms
    return times

# Connect to DuckDB (in-memory)
con = duckdb.connect()

# Force single-threaded execution
con.execute("PRAGMA threads=1;")

print("\nDuckDB\n")
print(f"{'SF':>5} | {'Run 1':>8} {'Run 2':>8} {'Run 3':>8} {'Run 4':>8} {'Run 5':>8} {'Run 6':>8} | {'Avg (ms)':>8}")
print("-" * 72)

for sf in scale_factors:
    query = query_template.format(sf=sf)
    times = benchmark_query(con, query)
    avg_ms = sum(times[1:]) / 5  # average of last 5 runs
    times_str = " ".join(f"{t:8.2f}" for t in times)
    print(f"{sf:5} | {times_str} | {avg_ms:8.2f}")