#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <arrow/util/thread_pool.h>

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
#include <iomanip>

using namespace std;

// can hold up to SF=5
static const int MAX_CUSTOMERS = 750001;
static const int MAX_ORDER_COUNT = 100;

alignas(64) int8_t counts[MAX_CUSTOMERS]; // max orders per customer is <= 50
alignas(64) int custdist[MAX_ORDER_COUNT];

inline bool reject_comment(string_view s) {
    if (s.size() < 16) return false;
    const char* p = s.data();
    const char* end = p + s.size() - 6;
    while (p < end) {
        if (*p == 's' && memcmp(p, "special", 7) == 0) {
            const char* q = p + 7;
            const char* qend = s.data() + s.size() - 7;
            while (q < qend) {
                if (*q == 'r' && memcmp(q, "requests", 8) == 0) return true;
                q++;
            }
        }
        p++;
    }
    return false;
}

unique_ptr<parquet::arrow::FileReader> open_parquet(const string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok())
        throw runtime_error("Failed to open file: " + path);
    shared_ptr<arrow::io::ReadableFile> infile = *infile_result;

    // Create single-threaded Arrow pool
    auto pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::internal::ThreadPool> single_thread_pool;
    auto tp_result = arrow::internal::ThreadPool::Make(1);
    if (!tp_result.ok()) {
        throw std::runtime_error("Failed to create single-thread thread pool: " + tp_result.status().ToString());
    }
    single_thread_pool = *tp_result;

    // Create Parquet reader with single-thread pool
    parquet::ArrowReaderProperties props;
    props.set_use_threads(false); // disable multi-threading in Parquet reader

    auto reader_result = parquet::arrow::FileReader::Make(
        pool,
        parquet::ParquetFileReader::Open(infile),
        props
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
    vector<double> read_times;
    vector<double> loop_times;
    vector<double> cust_times;

    int total_runs = WARMUP_RUNS + MEASURE_RUNS;

    cout << fixed << setprecision(2);

    for (int run = 0; run < total_runs; ++run) {

        memset(counts, 0, sizeof(counts));
        memset(custdist, 0, sizeof(custdist));

        auto start_time = chrono::high_resolution_clock::now();

        // -----------------------------
        // Orders scan
        auto reader = open_parquet(orders_file);
        vector<int> cols = {1, 8}; // o_custkey, o_comment
        int num_row_groups = reader->num_row_groups();

        double read_ms = 0;
        double loop_ms = 0;

        for (int rg = 0; rg < num_row_groups; ++rg) {

            auto read_start = chrono::high_resolution_clock::now();

            shared_ptr<arrow::Table> table;
            PARQUET_THROW_NOT_OK(reader->ReadRowGroup(rg, cols, &table));

            auto cust_arr = static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
            auto comment_arr = static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));

            const int64_t* cust_ptr = cust_arr->raw_values();
            const int32_t* offsets = comment_arr->raw_value_offsets();
            const char* data = reinterpret_cast<const char*>(comment_arr->raw_data());
            int rows = table->num_rows();

            auto read_end = chrono::high_resolution_clock::now();
            read_ms += chrono::duration<double, milli>(read_end - read_start).count();

            auto loop_start = chrono::high_resolution_clock::now();

            for (int i = 0; i < rows; ++i) {
                if (i + 8 < rows) __builtin_prefetch(&counts[cust_ptr[i + 8]]);
                int custkey = (int)cust_ptr[i];
                int start = offsets[i];
                int len = offsets[i + 1] - start;
                string_view view(data + start, len);
                counts[custkey] += !reject_comment(view);
            }

            auto loop_end = chrono::high_resolution_clock::now();
            loop_ms += chrono::duration<double, milli>(loop_end - loop_start).count();
        }

        auto orders_end = chrono::high_resolution_clock::now();

        // -----------------------------
        // Customers scan
        auto cust_reader = open_parquet(customer_file);
        vector<int> cust_cols = {0};
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
                if (c_count < MAX_ORDER_COUNT) custdist[c_count]++;
            }
        }

        auto cust_end = chrono::high_resolution_clock::now();
        double cust_ms = chrono::duration<double, milli>(cust_end - cust_start).count();

        auto end_time = chrono::high_resolution_clock::now();
        double elapsed_ms = chrono::duration<double, milli>(end_time - start_time).count();

        if (run >= WARMUP_RUNS) {
            run_times.push_back(elapsed_ms);
            read_times.push_back(read_ms);
            loop_times.push_back(loop_ms);
            cust_times.push_back(cust_ms);
        }

        if (!benchmark_mode) {
            // --- NORMAL MODE: print small table ---
            cout << "\nc_count | custdist\n";
            cout << "-------------------\n";
            for (int i = 0; i < MAX_ORDER_COUNT; ++i) {
                if (custdist[i] > 0) {
                    cout << " " << setw(6) << i << " | " << setw(8) << custdist[i] << "\n";
                }
            }
            cout << "\nTotal time: " << elapsed_ms << " ms\n" << endl;

            // Write CSV for normal mode
            ofstream out(output_file);
            out << "c_count,custdist\n";
            for (int i = 0; i < MAX_ORDER_COUNT; ++i)
                if (custdist[i] > 0) out << i << "," << custdist[i] << "\n";
            out.close();

            cout << "Query finished. Result written to " << output_file << endl;
            break; // only run once in normal mode
        }

        // --- BENCHMARK MODE ---
        if (benchmark_mode) {
            cout << "[" << (run - WARMUP_RUNS + 1) << "] time: " << elapsed_ms
                 << " ms - orders: " << (read_ms + loop_ms)
                 << " ms (read: " << read_ms
                 << " ms, loop: " << loop_ms
                 << " ms), customer: " << cust_ms << " ms" << endl;
        }
    }

    // Average benchmark timings
    if (benchmark_mode) {
        double avg_total = accumulate(run_times.begin(), run_times.end(), 0.0) / run_times.size();
        double avg_read = accumulate(read_times.begin(), read_times.end(), 0.0) / read_times.size();
        double avg_loop = accumulate(loop_times.begin(), loop_times.end(), 0.0) / loop_times.size();
        double avg_cust = accumulate(cust_times.begin(), cust_times.end(), 0.0) / cust_times.size();

        cout << "\naverage time: " << avg_total << " ms"
             << "\norders: " << (avg_read + avg_loop)
             << " ms (read: " << avg_read
             << " ms, loop: " << avg_loop
             << " ms)\ncustomer: " << avg_cust << " ms\n" << endl;
    }

    return 0;
}