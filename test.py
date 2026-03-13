import csv
import sys
import os

def read_csv(path):
    data = {}
    with open(path, newline='') as f:
        next(f)  # skip header
        for row in csv.reader(f):
            key = int(row[0])
            val = int(row[1])
            data[key] = val
    return data

def check_correctness(myfile, sf):
    mydata = read_csv(myfile)
    duckdata = read_csv(sf)

    ok = True
    for k, v in mydata.items():
        if k not in duckdata:
            print(f"Extra row in my output: c_count={k}")
            ok = False
        elif duckdata[k] != v:
            print(f"Mismatch at c_count={k}: my={v} duck={duckdata[k]}")
            ok = False
    for k in duckdata:
        if k not in mydata:
            print(f"Missing row in my output: c_count={k}")
            ok = False

    if ok:
        print("Output matches DuckDB ✅")
    else:
        print("Output does NOT match DuckDB ❌")
    return ok

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("python test.py results/result_sf0.5.csv validation/duckdb_sf0.5.csv")
        print("python test.py results/result_sf1.csv validation/duckdb_sf1.csv")
        sys.exit(1)

    myfile = sys.argv[1]
    sf = sys.argv[2]  # e.g., "0.5", "1", "2", "5"
    check_correctness(myfile, sf)