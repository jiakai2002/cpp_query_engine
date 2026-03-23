import duckdb
import time
import os

# Scale factors
scale_factors = [0.5, 1, 2, 5]

# Query 13 (direct parquet)
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

TOTAL_RUNS = 6  # first = warmup

con = duckdb.connect()

# single threaded
con.execute("PRAGMA threads=1;")

# output folder
os.makedirs("profiles", exist_ok=True)

for sf in scale_factors:
    print(f"Running SF {sf}")

    query = query_template.format(sf=sf)

    # warmup
    con.execute(query).fetchall()

    # enable profiling AFTER warmup
    profile_path = f"profiles/q13_sf{sf}.json"

    con.execute("PRAGMA enable_profiling='json';")
    con.execute(f"PRAGMA profiling_output='{profile_path}';")

    # profiled run
    con.execute(query).fetchall()

    # disable profiling
    con.execute("PRAGMA disable_profiling;")

print("Done. Profiles written to profiles/")