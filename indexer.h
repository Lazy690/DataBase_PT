
#pragma once 

#include <iostream>
#include <fstream>
#include <variant>
#include <string>      
#include <cstdint>
#include <vector>
#include <filesystem>
using namespace std;
namespace fs = std::filesystem;

class Indexer {

    private:
        
        fs::path id_index_path;
        vector<fs::path> secondary_index_paths;

        bool write_offset_pointers(fstream& file, const uint32_t& offset_db);
        bool write_overflow_pointer(fstream& file, const uint32_t& offset_db);

        bool append_new_key_str(fstream& file, const string& item, const uint32_t& offset_db);
        bool append_new_key_int(fstream& file, const int32_t& item, const uint32_t& offset_db);
        bool append_new_key_double(fstream& file, const double& item, const uint32_t& offset_db);

    public:

        Indexer();

        bool insert_into_BST_str(fstream& file, const string& item, const int32_t& offset_db);
        bool insert_into_BST_int(fstream& file, const int32_t& item, const int32_t& offset_db);
        bool insert_into_BST_double(fstream& file, const double& item, const int32_t& offset_db);

        bool fetch_from_BST_str(fstream& file, const string& item, vector<uint32_t>& result);
        bool fetch_from_BST_int(fstream& file, const int32_t& item, vector<uint32_t>& result);
        bool fetch_from_BST_double(fstream& file, const double& item, vector<uint32_t>& result);



        bool test(fstream& file, int32_t& item, uint32_t& offset_db);
        void test_storage_str();
};     