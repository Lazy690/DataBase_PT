
#include <iostream>
#include <algorithm>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
#include <map>
#include <unordered_map>

#include <cstddef> // for offsetof
                   //
#include "indexer.h"
using namespace std;
namespace fs = std::filesystem;

struct RecordBankHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t ID_INDEX = 0;
};
struct IndexHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    //This also has a dataType assiciated with it.
};
struct MetaDataHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
};  

RecordBankHeader DBheader{0x44415441, 2};
IndexHeader Iheader{0x44414441, 2};

enum class DataType : uint32_t {
    INT = 1, //int
    TEXT = 2, //string
    DOUBLE = 3 //double
};

struct Column {
    bool indexed = false;
    bool primaryKey = false;
    DataType type;
    std::string name;  
};

struct Row {
    vector<variant<int32_t, std::string, double>> values; 
};

vector<Column> schema = {{DataType::INT, "id"}, {DataType::TEXT, "name"}, {DataType::DOUBLE, "grade"}};

struct Table_Files {
    fstream recordBank;
    fstream primaryIdx;
    unordered_map<string, fstream> secondaryIdx;
};

bool validate_RecordBank_header(fstream& file) {
   
    RecordBankHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(RecordBankHeader));
    if(!file) return false;  

    if(header.MAGIC != DBheader.MAGIC) {
        cerr << "Invalid Magic" << endl;
        return false;
    }
    else if(header.VERSION != DBheader.VERSION) {
        cerr << "Invalid Version" << endl;
        return false;
    }
    file.seekg(0, ios::beg);
    return true;
}

bool validate_Index_header(fstream& file) {
   
    IndexHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(IndexHeader));
   
    if(header.MAGIC != Iheader.MAGIC) {
        cerr << "Invalid Magic" << endl;
        return false;
    }
    else if(header.VERSION != Iheader.VERSION) {
        cerr << "Invalid Version" << endl;
        return false;
    }
    file.seekg(0, ios::beg);
    return true;
}

//This fucntion WILL overrite
bool index_primaryIdx(fstream& file, uint32_t new_index, uint32_t offset) {
    
    struct index_entry {
        uint32_t idx = 0;
        uint32_t offset = 0;
    };

    index_entry entry{new_index, offset};

    int newEntry_index_offset = ((entry.idx - 1) * sizeof(index_entry)) + sizeof(IndexHeader);
    
    file.seekp(newEntry_index_offset, ios::beg);

    file.write(reinterpret_cast<const char*>(&entry), sizeof(index_entry));
    if(!file) {
        cerr << "Failed to write primary index when inseting" << endl;
        return false;
    }
    return true;

}

bool index_secondaryIdx(fstream& file, DataType type, variant<int32_t, string, double> value, uint32_t offset) {
    
    switch(type) {
        case DataType::INT: {
            int32_t int_to_insert = get<int32_t>(value);
            if(!insert_into_BST(file, int_to_insert, offset)) {
                cerr << "Failed to save Index record." << endl;
                return false;
            }
            break;
        }
        case DataType::TEXT: {
            string text_to_insert = get<string>(value);
            if(!insert_into_BST(file, text_to_insert, offset)) {
                cerr << "Failed to save Index record." << endl;
                return false;
            }
            break;
        }
        case DataType::DOUBLE: {
            double double_to_insert = get<double>(value);
            if(!insert_into_BST(file, double_to_insert, offset)) {
                cerr << "Failed to save Index record." << endl;
                return false;
            }
            break;
        }
    }
    return true;

}


bool append_row(fstream& file, uint32_t append_offset, const Row row) {
    
    struct row_metadata {
        const uint32_t tumbstoned = 0;
        uint32_t end_of_row_offset = 0;
    };

    row_metadata Rmeta; 

    file.seekp(append_offset + sizeof(row_metadata), ios::beg);

    for (const auto& value : row.values) {
        
        std::visit([&file](const auto& payload) {
            using T = std::decay_t<decltype(payload)>;
                if constexpr (std::is_same_v<T, int32_t>) {

                    uint32_t type = 1;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // now 4 bytes
                   
                } else if constexpr (std::is_same_v<T, std::string>) {

                    uint32_t type = 2;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));

                    uint32_t len = static_cast<uint32_t>(payload.size());
                    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    file.write(payload.data(), len);
                   
                } else if constexpr (std::is_same_v<T, double>) {

                    uint32_t type = 3;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // 4 bytes for double
   
                }
        }, value);
        if(!file) {
            cerr << "Failed to insert an element of a row when inserting" << endl;
            return false;
        }
    }
    uint32_t end_of_row = file.tellp();
    Rmeta.end_of_row_offset = end_of_row; 
    file.seekp(append_offset, ios::beg);
    file.write(reinterpret_cast<const char*>(&Rmeta), sizeof(row_metadata));
    file.seekp(0, ios::beg);

    return true;
}

struct insert_prep {
    bool success;
    uint32_t index = 0;
    uint32_t offset = 0;
};

insert_prep get_intsert_prep(fstream& file) {
     
    insert_prep prep;

    uint32_t index = 0;
    file.seekg(offsetof(RecordBankHeader, ID_INDEX), ios::beg);
    file.read(reinterpret_cast<char*>(&index), sizeof(index));
    if(!file) {
        cerr << "Failed to read index counter in the record bank header" << endl;
        prep.success = false;
        return prep;
    }

    prep.index = index + 1;

    uint32_t offset = 0;
    file.seekg(0, ios::end);
    
    prep.offset = offset;
    
    file.seekg(0, ios::beg);
    file.seekp(0, ios::end);
    prep.success = true;
    return prep;
    
}

bool incriment_index_counter(fstream& file, uint32_t incrimented_index) {
    
    file.seekp(offsetof(RecordBankHeader, ID_INDEX), ios::beg);
    file.write(reinterpret_cast<const char*>(&incrimented_index), sizeof(incrimented_index));
    if(!file) {
        return false;
    }
    return true;
}
Table_Files open_TableFiles(string tableName, const vector<Column>& schema) {

    Table_Files files;
    files.recordBank.open(fs::path(tableName) / "RecordBank.bin", ios::binary | ios::in | ios::out);
    if (!files.recordBank) {
        throw runtime_error("Failed to open RecordBank");    
    }
    if (!validate_RecordBank_header(files.recordBank)){ 
        throw runtime_error("Invalid record bank header");
    }
    
    fs::path index_path = fs::path(tableName) / "Indexes";
    files.primaryIdx.open(index_path / "primary.idx", ios::binary | ios::in | ios::out);
    if (!files.primaryIdx) {
        throw runtime_error("Failed to open Primary Index");
    }
    if(!validate_Index_header(files.primaryIdx)) {
        throw runtime_error("Invalid Primaty Index header");
    }

    for(int i = 0; i < int(schema.size()); i++) {
        
        string column_name = schema[i].name;
        
        fstream secondaryIdx(index_path / (column_name + ".idx"), ios::binary | ios::in | ios::out);
        if (!secondaryIdx) {
            throw runtime_error("Failed to open a secondary index");
        }
        if(!validate_Index_header(secondaryIdx)) {
            throw runtime_error("Invalid Secondary Index header");
        }
        files.secondaryIdx[column_name] = move(secondaryIdx);
    }

    return files;
}

bool INSERT(Table_Files& files, const Row row) {
    
    struct append_result {
        bool success;
        int index = 0;
        int offset = 0;
    };
    
    auto prep = get_intsert_prep(files.recordBank);
    
    if(!prep.success) {
        cerr << "Failed to retrive preperation data during inserting" << endl;
        return false;
    }

    if (!append_row(files.recordBank, prep.offset, row)) {
        cerr << "Failed to append row" << endl;
        return false;
    }

    if(!incriment_index_counter(files.recordBank, prep.index)) {
        cerr << "Failed to incriment index counter in the record bank header" << endl;
        return false;
    }

    if(!index_primaryIdx(files.primaryIdx, prep.index, prep.offset)) {
        cerr << "Failed to save primary index" << endl;
        return false;
    }
    for(int i = 0; i < int(schema.size()); i++) {

        auto type = schema[i].type;
        auto name = schema[i].name;
        auto value = row.values[i];
        if(!index_secondaryIdx(files.secondaryIdx[name],
                                type, value, prep.offset)) {
            cerr << "Failed to save secondary indexes" << endl;
            return false;
        }
    }
    return true;
}
int main() {
    return 0;
}
