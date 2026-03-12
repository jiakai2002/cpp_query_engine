#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <cstring>
#include <algorithm>
#include <string_view>
#include <chrono>
#include <numeric>
#include <cmath>

using namespace std;

// SF=1 sizes (adjust if SF > 1)
static const int MAX_CUSTOMERS = 150001;
static const int MAX_ORDER_COUNT = 100;

alignas(64) int counts[MAX_CUSTOMERS];
alignas(64) int custdist[MAX_ORDER_COUNT];

// Predicate to reject comments: '%special%requests%'
inline bool reject_comment(string_view s) {
    // cheap prefilter
    if (!memchr(s.data(), 's', s.size())) return false;

    size_t pos = s.find("special");
    if (pos == string_view::npos) return false;

    return s.find("requests", pos + 7) != string_view::npos;
}

// Open Parquet file using Arrow FileReader
unique_ptr<parquet::arrow::FileReader> open_parquet(const string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok())
        throw runtime_error("Failed to open file: " + path);
    shared_ptr<arrow::io::ReadableFile> infile = *infile_result;

    auto reader_result = parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        parquet::ParquetFileReader::Open(infile)
    );
    if (!reader_result.ok())
        throw runtime_error("Failed to create FileReader");

    return std::move(*reader_result);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: ./main <data_path> <output_csv> [--benchmark]\n";
        return 1;
    }

    string data_path = argv[1];
    string output_file = argv[2];
    bool benchmark_mode = (argc > 3 && string(argv[3]) == "--benchmark");

    string orders_file = data_path + "/orders.parquet";
    string customer_file = data_path + "/customer.parquet";

    const int WARMUP_RUNS = benchmark_mode ? 1 : 0;
    const int MEASURE_RUNS = benchmark_mode ? 5 : 1;

    vector<double> run_times;

    int total_runs = WARMUP_RUNS + MEASURE_RUNS;

    for (int run = 0; run < total_runs; ++run) {
        memset(counts, 0, sizeof(counts));
        memset(custdist, 0, sizeof(custdist));

        auto start_time = chrono::high_resolution_clock::now();

        // -----------------------------
        // Orders scan
        auto reader = open_parquet(orders_file);
        vector<int> cols = {1, 8}; // o_custkey, o_comment
        int num_row_groups = reader->num_row_groups();

        auto orders_start = chrono::high_resolution_clock::now();
        for (int rg = 0; rg < num_row_groups; ++rg) {
            shared_ptr<arrow::Table> table;
            PARQUET_THROW_NOT_OK(reader->ReadRowGroup(rg, cols, &table));

            auto cust_arr = static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
            auto comment_arr = static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));

            const int64_t* cust_ptr = cust_arr->raw_values();
            const int32_t* offsets = comment_arr->raw_value_offsets();
            const char* data = reinterpret_cast<const char*>(comment_arr->raw_data());
            int rows = table->num_rows();

            for (int i = 0; i < rows; ++i) {
                if (i + 8 < rows) __builtin_prefetch(&counts[cust_ptr[i + 8]]);

                int custkey = (int)cust_ptr[i];
                int start = offsets[i];
                int len = offsets[i + 1] - start;
                string_view view(data + start, len);

                counts[custkey] += !reject_comment(view);
            }
        }
        auto orders_end = chrono::high_resolution_clock::now();

        // -----------------------------
        // Customers scan
        auto cust_reader = open_parquet(customer_file);
        vector<int> cust_cols = {0}; // c_custkey
        int cust_row_groups = cust_reader->num_row_groups();

        auto cust_start = chrono::high_resolution_clock::now();
        for (int rg = 0; rg < cust_row_groups; ++rg) {
            shared_ptr<arrow::Table> table;
            PARQUET_THROW_NOT_OK(cust_reader->ReadRowGroup(rg, cust_cols, &table));

            auto c_arr = static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
            const int64_t* c_ptr = c_arr->raw_values();
            int rows = table->num_rows();

            for (int i = 0; i < rows; ++i) {
                int c_count = counts[(int)c_ptr[i]];
                if (c_count < MAX_ORDER_COUNT)
                    custdist[c_count]++;
            }
        }
        auto cust_end = chrono::high_resolution_clock::now();

        auto end_time = chrono::high_resolution_clock::now();
        double elapsed_ms = chrono::duration<double, std::milli>(end_time - start_time).count();

        if (run >= WARMUP_RUNS)
            run_times.push_back(elapsed_ms);

        if (!benchmark_mode) break; // Normal mode runs only once

        cout << "[Run " << (run - WARMUP_RUNS + 1) << "] Orders: "
             << chrono::duration<double, std::milli>(orders_end - orders_start).count()
             << " ms, Customers: "
             << chrono::duration<double, std::milli>(cust_end - cust_start).count()
             << " ms, Total: " << elapsed_ms << " ms" << endl;
    }

    // Benchmark timings
    if (benchmark_mode) {
        double avg = accumulate(run_times.begin(), run_times.end(), 0.0) / run_times.size();
        cout << "Benchmark (" << MEASURE_RUNS << " runs): " << avg << " ms" << endl;
    }

    // Emit result CSV (last run)
    vector<pair<int,int>> result;
    for (int i = 0; i < MAX_ORDER_COUNT; ++i)
        if (custdist[i] > 0) result.emplace_back(i, custdist[i]);

    sort(result.begin(), result.end(),
         [](const auto &a, const auto &b) {
             if (a.second != b.second) return a.second > b.second;
             return a.first > b.first;
         });

    ofstream out(output_file);
    out << "c_count,custdist\n";
    for (auto &r : result)
        out << r.first << "," << r.second << "\n";
    out.close();

    cout << "Query finished. Result written to " << output_file << endl;

    return 0;
}