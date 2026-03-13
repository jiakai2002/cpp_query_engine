#!/bin/bash
# ----------------------------
# run.sh - Compile and run TPC-H Q13 engine
# Usage:
#   ./run.sh --data data/sf1 --out result.csv [--benchmark]
# ----------------------------

set -e

# Default arguments
DATA_DIR=""
OUT_FILE=""
BENCHMARK=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data)
      DATA_DIR="$2"
      shift 2
      ;;
    --out)
      OUT_FILE="$2"
      shift 2
      ;;
    --benchmark)
      BENCHMARK="--benchmark"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Error: --data <data_path> is required"
  exit 1
fi

# --- Compile main.cpp ---
echo "Compiling main.cpp..."
g++ -O3 -march=native -std=c++20 main.cpp $(pkg-config --cflags --libs arrow parquet) -o main
if [[ $? -ne 0 ]]; then
  echo "Compilation failed!"
  exit 1
fi

# --- Run program ---
echo "Running main on $DATA_DIR..."
./main "$DATA_DIR" "$OUT_FILE" $BENCHMARK
if [[ $? -ne 0 ]]; then
  echo "Execution failed!"
  exit 1
fi