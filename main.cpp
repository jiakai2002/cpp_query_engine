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

using namespace std;

// maximums for SF=1
static const int MAX_CUSTOMERS = 200000;
static const int MAX_ORDER_COUNT = 100;

alignas(64) int counts[MAX_CUSTOMERS];
alignas(64) int custdist[MAX_ORDER_COUNT];

// Two-stage substring search for o_comment
inline bool reject_comment(string_view s) {
    size_t pos = s.find("special");
    if (pos == string_view::npos) return false;
    return s.find("requests", pos) != string_view::npos;
}

// Helper to open Parquet file with modern Arrow Result API
unique_ptr<parquet::arrow::FileReader> open_parquet(const string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok()) {
        throw runtime_error("Failed to open file: " + path + " : " + infile_result.status().ToString());
    }
    shared_ptr<arrow::io::ReadableFile> infile = *infile_result;

    auto reader_result = parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        parquet::ParquetFileReader::Open(infile)
    );

    if (!reader_result.ok()) {
        throw runtime_error("Failed to create FileReader: " + reader_result.status().ToString());
    }

    // Return the unique_ptr directly
    return std::move(*reader_result);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: ./main <data_path> <output_csv>\n";
        return 1;
    }

    string data_path = argv[1];
    string output_file = argv[2];
    string orders_file = data_path + "/orders.parquet";
    string customer_file = data_path + "/customer.parquet";

    memset(counts, 0, sizeof(counts));
    memset(custdist, 0, sizeof(custdist));

    // --- Read orders.parquet ---
    auto reader = open_parquet(orders_file);

    // Only read needed columns: o_custkey (1), o_comment (8)
    vector<int> cols = {1, 8};
    int num_row_groups = reader->num_row_groups();

    for (int rg = 0; rg < num_row_groups; ++rg) {
        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadRowGroup(rg, cols, &table));

        auto cust_arr = static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
        auto comment_arr = static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));

        int rows = table->num_rows();
        for (int i = 0; i < rows; ++i) {
            auto view = comment_arr->GetView(i);
            if (!reject_comment(view)) {
                int custkey = static_cast<int>(cust_arr->Value(i));
                counts[custkey]++;
            }
        }
    }

    // --- Read customer.parquet ---
    auto cust_reader = open_parquet(customer_file);

    auto cust_cols = vector<int>{0}; // c_custkey only
    int cust_row_groups = cust_reader->num_row_groups();
    for (int rg = 0; rg < cust_row_groups; ++rg) {
        shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(cust_reader->ReadRowGroup(rg, cust_cols, &table));

        auto c_arr = static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
        int rows = table->num_rows();
        for (int i = 0; i < rows; ++i) {
            int c_count = counts[static_cast<int>(c_arr->Value(i))];
            if (c_count < MAX_ORDER_COUNT)
                custdist[c_count]++;
        }
    }

    // --- Emit results ---
    vector<pair<int,int>> result;
    for (int i = 0; i < MAX_ORDER_COUNT; ++i) {
        if (custdist[i] > 0)
            result.emplace_back(i, custdist[i]);
    }

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