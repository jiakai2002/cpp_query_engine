import duckdb
import os
import sys

# scale factors you want to generate
scale_factors = [0.5, 1, 2, 5]

# root data folder
root = "../data"

# create root directory if needed
os.makedirs(root, exist_ok=True)

for sf in scale_factors:
    print(f"Generating TPC-H data at scale factor {sf}…")

    # folder for this scale factor
    out_dir = os.path.join(root, f"sf{sf}")
    os.makedirs(out_dir, exist_ok=True)

    # connect
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")

    # remove any previous generation
    con.execute("DROP TABLE IF EXISTS customer;")
    con.execute("DROP TABLE IF EXISTS orders;")

    # generate tables at this scale factor
    con.execute(f"CALL dbgen(sf={sf});")

    # export only the two tables as Parquet
    con.execute(f"""
        COPY (SELECT * FROM customer)
        TO '{out_dir}/customer.parquet'
        (FORMAT PARQUET);
    """)
    con.execute(f"""
        COPY (SELECT * FROM orders)
        TO '{out_dir}/orders.parquet'
        (FORMAT PARQUET);
    """)

    con.close()
    print(f"Finished SF{sf}, files in: {out_dir}\n")