#include <iostream>
#include "indexer.h"
#include <filesystem>
using namespace std;
namespace fs = std::filesystem;

struct Header{
    uint32_t Magic;
    uint32_t Version;
    uint32_t type;
};

int main() {
    fs::remove_all("test_data");
    fs::path file = fs::path("test_data") / "test.bin"; 
    fs::create_directories("test_data");
  
    if(!fs::exists(file)) {
        ofstream file("test_data/test.bin", ios::binary);
        Header header{12345, 1, 1};
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    }
    Indexer indexer;
    indexer.test_storage_str();

    fs::remove_all("test_data");
    return 0;
}

