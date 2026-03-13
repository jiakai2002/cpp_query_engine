import csv
import os

RESULTS_DIR = "results"
VALIDATION_DIR = "validation"

def read_csv(path):
    data = {}
    with open(path, newline='') as f:
        next(f)  # skip header
        for row in csv.reader(f):
            key = int(row[0])
            val = int(row[1])
            data[key] = val
    return data

def check_correctness(myfile, duckfile):
    if not os.path.exists(duckfile):
        print(f"Validation file not found: {duckfile}")
        return False

    mydata = read_csv(myfile)
    duckdata = read_csv(duckfile)

    ok = True
    for k, v in mydata.items():
        if k not in duckdata:
            print(f"[{myfile}] Extra row: c_count={k}")
            ok = False
        elif duckdata[k] != v:
            print(f"[{myfile}] Mismatch at c_count={k}: my={v} duck={duckdata[k]}")
            ok = False
    for k in duckdata:
        if k not in mydata:
            print(f"[{myfile}] Missing row: c_count={k}")
            ok = False

    if ok:
        print(f"[{myfile}] Output matches DuckDB ✅")
    else:
        print(f"[{myfile}] Output does NOT match DuckDB ❌")
    return ok

def main():
    # Find all result CSV files
    result_files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("result_sf") and f.endswith(".csv")]
    if not result_files:
        print("No result CSV files found in results/")
        return

    for rf in sorted(result_files):
        # extract SF from filename, e.g., result_sf1.csv -> 1
        sf = rf.replace("result_sf", "").replace(".csv", "")
        myfile = os.path.join(RESULTS_DIR, rf)
        duckfile = os.path.join(VALIDATION_DIR, f"duckdb_sf{sf}.csv")
        check_correctness(myfile, duckfile)

if __name__ == "__main__":
    main()