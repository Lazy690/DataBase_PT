#include <iostream>
#include <algorithm>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
#include <map>
#include <cstddef> // for offsetof
                   //
#include "executer.h"
#include "indexer.h"
#include "classes.h"
using namespace std;
namespace fs = std::filesystem;
RecordBankHeader DBheader{0x44415441, 2};
IndexHeader Iheader{0x44414441, 2};
uint32_t hold_row_id = 0;
Row hold_for_indexing;
uint32_t RecordBank_offset_recording = 0;

enum class status_code{
    OK,
    FILE_ERROR,
    INVALID_HEADER,
    APPEND_FAILED,
    P_INDEX_FAILED,
    S_INDEX_FAILED
}

struct append_result {
    bool success;
    int index = 0;
    int offset = 0;
}

bool save_DBheader(ostream& file, const RecordBankHeader& header) {
    file.write(reinterpret_cast<const char*>(&header), sizeof(RecordBankHeader));
    return file.good();
}
bool save_Iheader(ostream& file, const IndexHeader& header, const uint32_t& type) {
    file.write(reinterpret_cast<const char*>(&header), sizeof(IndexHeader));
    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
    return file.good();
}
bool save_MDheader(ostream& file, const MetaDataHeader& header) {
    file.write(reinterpret_cast<const char*>(&header), sizeof(MetaDataHeader));
    return file.good();
}
bool validate_RecordBank_header(fstream& file) {
   
    RecordBankHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(RecordBankHeader));
   
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
bool validate_Index_header(fstream& file, IndexHeader& validate) {
   
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
//==================================================================
//====== Index operations ======
//==================================================================
bool save_ID_record(fs::path& path, int idndex, int offset,
                    int overrite_id_offset = -1) {
    fstream file(fs::path(path) / "id.idx", ios::binary | ios::in | ios::out);
    if(!validate_Index_header(file, context.header.index)) {
        cerr << "Invalid file Format." << endl;
        return false;
    }
    if(!file) {
        cout << "Missing Id index file." << endl;
        return false;
    }
    if(int(overrite_id_offset) != -1) {
        file.seekp(overrite_id_offset, ios::beg);
    }
    else {
        file.seekp(0, ios::end);
    }
   
    file.write(reinterpret_cast<const char*>(&id), sizeof(id));
   
    file.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    //int second_pos = file.tellp();
   
    return file.good();
}
bool save_value_index_record(fs::path path, const Row row, uint32_t hold_offset) {
 
    for(int i = 0; i < int(schema.size()); i++) {
        string file_name = schema[i].name + ".idx";
        fstream index_out(fs::path(path) / file_name, ios::binary | ios::in | ios::out);
        if(!validate_Index_header(index_out)) {
            cerr << "Invalid file Format." << endl;
            return false;
        }
       
        switch(schema[i].type) {
           
            case DataType::INT: {
                cout << "Inserting into index: " << get<int32_t>(row.values[i]) << endl;
                int32_t item_int = get<int32_t>(row.values[i]);
                if(!this->indexer.insert_into_BST(index_out, item_int, hold_offset)) {
                    cerr << "Failed to save Index record." << endl;
                    return false;
                }
                break;
            }
            case DataType::TEXT: {
                cout << "Inserting into index: " << get<string>(row.values[i]) << endl;
                string item_string = get<string>(row.values[i]);
                if(!this->indexer.insert_into_BST(index_out, item_string, hold_offset)) {
                    cerr << "Failed to save Index record." << endl;
                    return false;
                }
                break;
            }
            case DataType::DOUBLE: {
                cout << "Inserting into index: " << get<double>(row.values[i]) << endl;
                double item_double = get<double>(row.values[i]);
                if(!this->indexer.insert_into_BST(index_out, item_double, hold_offset)) {
                    cerr << "Failed to save Index record." << endl;
                    return false;
                }
                break;
            }
        }
    }
    return true;
}
bool fetch_ID_offset(fstream& file, const int ID, int& fetched_offset) {
    if(!validate_Index_header(file)) {
        cerr << "Invalid file Format." << endl;
        return false;
    }
    //12 is the size of the header
    int header_size = 12;
    file.seekg(header_size, ios::beg);
    //8 is because each entry is always 8 bytes wide: [value][offset in bank]
    int id_location_offset = (ID - 1) * 8;
    file.seekg(id_location_offset, ios::cur);
   
    if(file.eof()) {
        cerr << "Id out of Bounds." << endl;
        return false;
    }
    int32_t value = 0;
    file.read(reinterpret_cast<char*>(&value), sizeof(value));
    cout << "id: " << ID << endl;
    cout << "value: " << value << endl;
    if(value != ID) {
        cerr << "Id not found" << endl;
        return false;
    }
    file.read(reinterpret_cast<char*>(&fetched_offset), sizeof(fetched_offset));
   
    return file.good();
};
bool fetch_Value_offset(fstream& file, variant<int32_t, string, double>& value, vector<uint32_t>& fetched_offsets) {
    std::visit([&file, &fetched_offsets, this](const auto& key) {
        using T = std::decay_t<decltype(key)>;
            if constexpr (std::is_same_v<T, int32_t>) {
                this->indexer.fetch_from_BST(file, key, fetched_offsets);
            } else if constexpr (std::is_same_v<T, std::string>) {
                this->indexer.fetch_from_BST(file, key, fetched_offsets);
            } else if constexpr (std::is_same_v<T, double>) {
                this->indexer.fetch_from_BST(file, key, fetched_offsets);
            }
    }, value);
    return file.good();
}
//==================================================================
//====== RecordBank operations ======
//==================================================================
bool append(std::fstream& file, const Row& row) {
    append_result result;

    index = 0;
    file.seekg(offsetof(RecordBankHeader, ID_INDEX), std::ios::beg);
    file.read(reinterpret_cast<char*>(&index), sizeof(index));

    //reset read cursor:
    file.seekg(0, ios::beg);

    index += 1;
    append_result.index = index;
    //Move the write cursor to the end of the file to append:
    file.seekp(0, ios::end);
    //Capture the offset of the start of the row were going to save to use in the indexer:
    append_result.offset = file.tellp();
    //write Tumbstone bytes:
    int32_t tumbstoned = 0;
    file.write(reinterpret_cast<const char*>(&tumbstoned), sizeof(tumbstoned));
    //write an empty space to later store the offset of the end of the row:
    int32_t end_of_row_offset = -1;
    int32_t end_of_row_offset_location = file.tellp();
    file.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));
    //Write row ID:
    uint8_t id_type = 1;
    file.write(reinterpret_cast<const char*>(&id_type), sizeof(id_type));
    file.write(reinterpret_cast<const char*>(&index), sizeof(index));
    for (const auto& value : row.values) {
        std::visit([&file](const auto& payload) {
            using T = std::decay_t<decltype(payload)>;
                if constexpr (std::is_same_v<T, int32_t>) {
                    uint8_t type = 1;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // now 4 bytes
                   
                } else if constexpr (std::is_same_v<T, std::string>) {
                    uint8_t type = 2;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    uint32_t len = static_cast<uint32_t>(payload.size());
                    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    file.write(payload.data(), len);
                } else if constexpr (std::is_same_v<T, double>) {
                    uint8_t type = 3;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // 4 bytes for double
   
                }
        }, value);
    }
    //store the offset of the end of the row in the empty space we saved earlier:
    end_of_row_offset = file.tellp();
    file.seekp(end_of_row_offset_location, ios::beg);
    file.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));
   
    //Update index counter:
    file.seekg(offsetof(RecordBankHeader, ID_INDEX), std::ios::beg);
    file.write(reinterpret_cast<const char*>(&index), sizeof(index));
    file.seekp(0, ios::beg);

    if(!file) result.sucess = false;
    else result.sucess = true;
    return result;
}
bool fetch(istream& file, Row& row, int fetched_offset = -1) {
    if (fetched_offset != -1) {
        //Now move the read cursor to the offset and get that row.
        file.seekg(fetched_offset, ios::beg);
    }
    //check Tumbstone bytes:
    int32_t tumbstoned = -1;
    file.read(reinterpret_cast<char*>(&tumbstoned), sizeof(tumbstoned));
    cout << "Tumbstone: " << tumbstoned << endl;
    if(tumbstoned == 0 && tumbstoned != -1) {
        //get the offset of end of the row:
        int32_t end_of_row_offset = 0;
        file.read(reinterpret_cast<char*>(&end_of_row_offset), sizeof(end_of_row_offset));
       
        while(true) {
            if(file.tellg() == end_of_row_offset) break;
           
            uint8_t type_int;
            file.read(reinterpret_cast<char*>(&type_int), sizeof(type_int));
            if (!file) return false;
            DataType type = static_cast<DataType>(type_int);
            switch (type) {
                case DataType::INT: {
                    int32_t payload_int;
                    file.read(reinterpret_cast<char*>(&payload_int), sizeof(payload_int));
                    if (!file) return false;
                    row.values.push_back(payload_int);
                    break;
                }
                case DataType::TEXT: {
                    uint32_t len;
                    file.read(reinterpret_cast<char*>(&len), sizeof(len));
                    if (!file) return false;
                    string payload_str;
                    payload_str.resize(len);
                    file.read(payload_str.data(), len);
                    if (!file) return false;
                    row.values.push_back(payload_str);
                    break;
                }
                case DataType::DOUBLE: {
                    double payload_double;
                    file.read(reinterpret_cast<char*>(&payload_double), sizeof(payload_double));
                    if (!file) return false;
                    row.values.push_back(payload_double);
                    break;
                }
                default:
                    return false; // Unknown type
            }
        }
    }
    else if(tumbstoned == 1 && tumbstoned !=-1) {
        cout << "Row was deleted." << endl;
    }
    else {
        cerr << "Invalid Tumbstone byte." << endl;
        return false;
    }
    return true;
}
bool deleteRow(fstream& file, int fetched_offset = -1) {
    file.seekp(fetched_offset, ios::beg);
   
    //change Tumbstone bytes to true:
    int32_t tumbstoned = 1;
    file.write(reinterpret_cast<const char*>(&tumbstoned), sizeof(tumbstoned));
    file.seekg(fetched_offset, ios::beg);
    int32_t tumb = -1;
    file.read(reinterpret_cast<char*>(&tumb), sizeof(tumb));
    cout << "Tumbstone value after writting it on delete: " << tumb << endl;
    return file.good();
}
bool append_updated_row(std::fstream& file, const Row& row, int& hold_new_offset,
                          int old_index) {
    file.seekp(0, ios::end);
    hold_new_offset = file.tellp();
    //write Tumbstone bytes:
    int32_t tumbstoned = 0;
    file.write(reinterpret_cast<const char*>(&tumbstoned), sizeof(tumbstoned));
    //write an empty space to later store the offset of the end of the row:
    int32_t end_of_row_offset = -1;
    int32_t end_of_row_offset_location = file.tellp();
    file.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));
    //Write row ID:
    uint8_t id_type = 1;
    file.write(reinterpret_cast<const char*>(&id_type), sizeof(id_type));
    file.write(reinterpret_cast<const char*>(&old_index), sizeof(old_index));
    for (const auto& value : row.values) {
        std::visit([&file](const auto& payload) {
            using T = std::decay_t<decltype(payload)>;
                if constexpr (std::is_same_v<T, int32_t>) {
                    uint8_t type = 1;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // now 4 bytes
                   
                } else if constexpr (std::is_same_v<T, std::string>) {
                    uint8_t type = 2;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    uint32_t len = static_cast<uint32_t>(payload.size());
                    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    file.write(payload.data(), len);
                } else if constexpr (std::is_same_v<T, double>) {
                    uint8_t type = 3;
                    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
                    file.write(reinterpret_cast<const char*>(&payload), sizeof(payload)); // 4 bytes for double
   
                }
        }, value);
    }
    //store the offset of the end of the row in the empty space we saved earlier:
    end_of_row_offset = file.tellp();
    file.seekp(end_of_row_offset_location, ios::beg);
    file.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));
    file.seekp(0, ios::beg);
    //deleteRow(file, old_offset);
    return file.good();
}
status_code INSERT(fs::path path, const Row row) {

    
    fstream RecordBank(path, ios::binary | ios::in | ios::out);
    if (!RecordBank) {
        return status_code::FILE_ERROR;
    }

    if (!validate_RecordBank_header(RecordBank)) 
        return status_code::INVALID_HEADER;
    
    auto result = append(RecordBank, row)
    if (!resul.sucess) {
        return status_code::APPEND_FAILED;
    }
    RecordBank.close();

    if(!save_ID_record(path, result.index, result.offset)) {
        return P_INDEX_FAILED;
    }
    if(!this->save_value_index_record(row, hold_offset)) {
        return S_INDEX_FAILED;
    }
   
}
void SELECT_byID(Paths& path, int id, Row& row, bool returns_offset = false, int* returned_offset = 0) {
   
    fstream index(fs::path(path.index) / "id.idx", ios::binary | ios::in);
    if(!index) {
        cerr << "Failed to find ID index file." << endl;
        return;
    }
    int fetched_offset = 0;
    if(!fetch_ID_offset(index, id, fetched_offset)) {
        cout << "Failed to get row location from ID index" << endl;
        return;
    }
    index.close();
   
    if(returns_offset) {
        *returned_offset = fetched_offset;
    }
    fstream RecordBank(path.record_bank, ios::binary | ios::in);
    if (!RecordBank) {
        cout << "Failed to Find RecordBank file." << endl;
        return;
    }
   
    if(!this->validate_RecordBank_header(RecordBank)) {
        cout << "Header not validated." << endl;
        return;
    }
    cout << "Fected offset from fetch: " << fetched_offset << endl;
    if(!this->fetch(RecordBank, row, fetched_offset)) {
        cout << "Failed to fetch row from RecordBank" << endl;
        return;
    }
}
void SELECT_byValue(Paths& path, string& column, const variant<int32_t, string, double>& value, vector<Row>& results,
                      bool returns_offset = false, vector<uint32_t>* returned_offsets = {}) {
   
    string file_name = column + ".idx";
    fstream index(fs::path(path.index) / file_name, ios::binary | ios::in | ios::out);
    if(!index) {
        cerr << "Failed to find Value index file." << endl;
        return;
    }
    vector<uint32_t> fetched_offsets;
   
    if(!fetch_Value_offset(index, value, fetched_offsets)) {
        cerr << "Failed to fetch record bank offsets" << endl;
        return;
    }
    index.close();
    if(returns_offset) {
        *returned_offsets = fetched_offsets;
    }
    fstream RecordBank(path.record_bank, ios::binary | ios::in);
    if (!RecordBank) {
        cout << "Failed to Find RecordBank file." << endl;
        return;
    }
   
    if(!this->validate_RecordBank_header(RecordBank)) {
        cout << "Header not validated." << endl;
        return;
    }
    for (auto fetched_offset : fetched_offsets) {
       
        Row row;
        if(!this->fetch(RecordBank, row, fetched_offset)) {
            cout << "Failed to fetch row from RecordBank" << endl;
            return;
        }
        if (row.values.empty()) {
            continue;
        }
        results.push_back(row);
    }
    RecordBank.close();
    cout << "Results vector before cheking: " << results.size() << endl;
}
void DELETE_byID(Paths& path, const int id) {
    fstream index(fs::path(path.index) / "id.idx", ios::binary | ios::in);
    if(!index) {
        cerr << "Failed to find ID index file." << endl;
        return;
    }
    int fetched_offset = 0;
    if(!fetch_ID_offset(index, id, fetched_offset)) {
        cout << "Failed to get row location from ID index" << endl;
        return;
    }
    index.close();
   
    fstream RecordBank(path.record_bank, ios::binary | ios::in | ios::out);
    if (!RecordBank) {
        cout << "Failed to Find RecordBank file." << endl;
        return;
    }
   
    if(!this->validate_RecordBank_header(RecordBank)) {
        cout << "Header not validated." << endl;
        return;
    }
    cout << "Fected offset from delete: " << fetched_offset << endl;
    if(!this->deleteRow(RecordBank, fetched_offset)) {
        cout << "Failed to delete row from RecordBank" << endl;
        return;
    }
    cout << "deleted sucessfully" << endl;
}
void DELETE_byValue(Paths& path, string& column, variant<int32_t, string, double>& value) {
    string file_name = column + ".idx";
    fstream index(fs::path(path.index) / file_name, ios::binary | ios::in | ios::out);
    if(!index) {
        cerr << "Failed to find Value index file." << endl;
        return;
    }
    vector<uint32_t> fetched_offsets;
   
    if(!fetch_Value_offset(index, value, fetched_offsets)) {
        cerr << "Failed to fetch record bank offsets" << endl;
        return;
    }
    index.close();
    fstream RecordBank(path.record_bank, ios::binary | ios::in | ios::out);
    if (!RecordBank) {
        cout << "Failed to Find RecordBank file." << endl;
        return;
    }
   
    if(!this->validate_RecordBank_header(RecordBank)) {
        cout << "Header not validated." << endl;
        return;
    }
    for (auto fetched_offset : fetched_offsets) {
       
        if(!this->deleteRow(RecordBank, fetched_offset)) {
            cout << "Failed to Delete row from RecordBank" << endl;
            return;
        }
       
    }
    RecordBank.close();
}
void UPDATE_byID(Paths& path, int id, map<string, variant<int32_t, string, double>>& values_map) {
   
    Row row_to_update;
    int hold_old_offset = 0;
    int* returned_offset_from_fetch = &hold_old_offset;
    this->fetchRow_byID(path, id, row_to_update, true, returned_offset_from_fetch);
    int old_index = get<int>(row_to_update.values[0]);
   
    //Take out the index in the loaded in row so that it doesnt repeat it in the record bank
    row_to_update.values.erase(row_to_update.values.begin());
    vector<string> columns_keys;
    vector<string> schema_column_names;
    for (auto key = values_map.begin(); key != values_map.end(); key++) {
        columns_keys.push_back(key->first);
    }
    for (auto col : this->schema) {
        schema_column_names.push_back(col.name);
    }
    for ( auto column_name : columns_keys) {
        auto it = find(schema_column_names.begin(),
                        schema_column_names.end(),
                        column_name);
        if (it == schema_column_names.end()) {
            cout << "Integrety error: " << column_name << " is not a valid column" << endl;
            return;
        }
        int index_position = it - schema_column_names.begin();
       
        row_to_update.values[index_position] = values_map[column_name];
    }
    vector<Row> results;
    results.push_back(row_to_update);
    print_row_test(results);
   
    fstream RecordBank(path.record_bank, ios::binary | ios::in | ios::out);
    int hold_new_offset = 0;
    uint32_t hold_new_offset_Uint = 0;
    if(!append_updated_row(RecordBank, row_to_update, hold_new_offset, old_index)) {
        cerr << "Failed to update the file in record bank" << endl;
        return;
    }
   
    hold_new_offset_Uint = hold_new_offset;
    RecordBank.close();
    //12 is the size of the header
    int header_size = 12;
    //8 is because each entry is always 8 bytes wide: [value][offset in bank]
    int id_location_offset = ((old_index - 1) * 8) + header_size;
    if(!this->save_ID_record(old_index, hold_new_offset, id_location_offset)) {
        cerr << "Could not update the index with the new offset." << endl;
        return;
    }
    if(!this->save_value_index_record(row_to_update, hold_new_offset_Uint)) {
        cerr << "Failed to Index Item" << endl;
        return;
    }
    fstream RecordBank_delete(path.record_bank, ios::binary | ios::in | ios::out);
    if(!deleteRow(RecordBank_delete, hold_old_offset)) {
        cout << "Failed to delete row after updating." << endl;
        return;
    }
}
void UPDATE_byValue(Paths& path, string column_to_search, variant<int32_t, string, double>& value_to_search,
                        map<string, variant<int32_t, string, double>>& values_map) {
   
    vector<Row> rows_to_update;
    vector<uint32_t> hold_old_offsets = {};
    vector<uint32_t>* returned_offset_from_fetchPTR = &hold_old_offsets;
    this->fetchRow_byValue(path, column_to_search, value_to_search, rows_to_update,
                        true, returned_offset_from_fetchPTR);
   
    for (int i = 0; i < int(rows_to_update.size()); i++) {
        int old_index = get<int>(rows_to_update[i].values[0]);
       
        //Take out the index in the loaded in row so that it doesnt repeat it in the record bank
        rows_to_update[i].values.erase(rows_to_update[i].values.begin());
        vector<string> columns_keys;
        vector<string> schema_column_names;
        for (auto key = values_map.begin(); key != values_map.end(); key++) {
            columns_keys.push_back(key->first);
        }
        for (auto col : this->schema) {
            schema_column_names.push_back(col.name);
        }
        for ( auto column_name : columns_keys) {
            auto it = find(schema_column_names.begin(),
                        schema_column_names.end(),
                        column_name);
            if (it == schema_column_names.end()) {
                cout << "Integrety error: " << column_name << " is not a valid column" << endl;
                return;
            }
            int index_position = it - schema_column_names.begin();
           
            rows_to_update[i].values[index_position] = values_map[column_name];
        }
        vector<Row> results;
        results.push_back(rows_to_update[i]);
        print_row_test(results);
       
        fstream RecordBank(path.record_bank, ios::binary | ios::in | ios::out);
        int hold_new_offset = 0;
        uint32_t hold_new_offset_Uint = 0;
        if(!append_updated_row(RecordBank, rows_to_update[i], hold_new_offset, old_index)) {
            cerr << "Failed to update the file in record bank" << endl;
            return;
        }
       
        hold_new_offset_Uint = hold_new_offset;
        RecordBank.close();
        //12 is the size of the header
        int header_size = 12;
        //8 is because each entry is always 8 bytes wide: [value][offset in bank]
        int id_location_offset = ((old_index - 1) * 8) + header_size;
        if(!this->save_ID_record(old_index, hold_new_offset, id_location_offset)) {
            cerr << "Could not update the index with the new offset." << endl;
            return;
        }
        if(!this->save_value_index_record(rows_to_update[i], hold_new_offset_Uint)) {
            cerr << "Failed to Index Item" << endl;
            return;
        }
        fstream RecordBank_delete(path.record_bank, ios::binary | ios::in | ios::out);
        if(!deleteRow(RecordBank_delete, hold_old_offsets[i])) {
            cout << "Failed to delete row after updating." << endl;
            return;
        }
    }
}
void print_row_test(vector<Row>& results) {
    int count = 0;
    for (auto row : results) {
    for (auto item: row.values) {
           
        count ++;
            visit([](const auto& x) {
                using T = std::decay_t<decltype(x)>;
   
                if constexpr (std::is_same_v<T, int32_t>) {
                    //cout << "Type: int" << endl;
                    cout << "|| " << x << " ||" << endl;
                }
                else if constexpr (std::is_same_v<T, std::string>) {
                    //cout << "Type: str" << endl;
                    cout << "|| " << x << " ||" << endl;
                }
                else if constexpr (std::is_same_v<T, double>) {
                    //cout << "Type: str" << endl;
                    cout << "|| " << x << " ||" << endl;
                }
            }, item);
        }
    }
}
