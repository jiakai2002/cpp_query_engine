import duckdb
import pandas as pd
import os

# List of scale factors
scale_factors = ["sf0.5", "sf1", "sf2", "sf5"]

# Base folder where your TPC-H Parquet files are stored
base_data_dir = "../data"

# Folder to save DuckDB CSV results
duckdb_results_dir = "../duckdb_results"
os.makedirs(duckdb_results_dir, exist_ok=True)

for sf in scale_factors:
    data_dir = os.path.join(base_data_dir, sf)
    customer_file = os.path.join(data_dir, "customer.parquet")
    orders_file = os.path.join(data_dir, "orders.parquet")
    
    print(f"Processing {sf}...")
    
    con = duckdb.connect()
    # Register parquet files as views
    con.execute(f"CREATE VIEW customer AS SELECT * FROM '{customer_file}'")
    con.execute(f"CREATE VIEW orders AS SELECT * FROM '{orders_file}'")
    
    # TPC-H Q13 query
    query = """
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
    
    result = con.execute(query).fetchdf()
    
    # Save to CSV
    out_file = os.path.join(duckdb_results_dir, f"duckdb_q13_{sf}.csv")
    result.to_csv(out_file, index=False)
    print(f"Saved DuckDB results to {out_file}\n")