#!/bin/bash
# ----------------------------
# run.sh - Compile and run TPC-H Q13 engine
# Usage:
#   ./run.sh --data data/sf1 [--benchmark]
# ----------------------------

set -e

# Default arguments
DATA_DIR=""
BENCHMARK=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data)
      DATA_DIR="$2"
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

# Extract SF from data path (assumes folder name is like sf0.5, sf1, sf2, sf5)
SF=$(basename "$DATA_DIR")

# Ensure results folder exists
mkdir -p results

# Output file
OUT_FILE="results/result_${SF}.csv"

# --- Compile main.cpp ---
g++ -O3 -march=native -std=c++20 main.cpp $(pkg-config --cflags --libs arrow parquet) -o main
if [[ $? -ne 0 ]]; then
  echo "Compilation failed!"
  exit 1
fi

# --- Run program ---
./main "$DATA_DIR" "$OUT_FILE" $BENCHMARK
if [[ $? -ne 0 ]]; then
  echo "Execution failed!"
  exit 1
fi