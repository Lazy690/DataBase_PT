#include <iostream>
#include <cassert>
#include <algorithm>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <cstddef> 

namespace fs = std::filesystem;

struct DB_Header {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t NUM_TABLES = 0; 
};  

struct TB_Header {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t NUM_COLUMNS = 0;
};  

struct RecordBankHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t ID_INDEX = 0;
};

struct DB_Data {
    std::string name;
    std::vector<std::string> table_names;
};

enum class DataType : uint32_t {
    INTEIRO = 1, //int
    TEXTO = 2, //string
    REAL = 3 //double
};

std::fstream file("DB_metadata.bin", std::ios::binary | std::ios::in | std::ios::out);
DB_Header DBHEADER{0x44415641, 2};
TB_Header TBHEADER{0x44415441, 3};
RecordBankHeader RBHEADER{0x44415441, 3};

bool isInVector(const std::string& value, std::vector<std::string> v) {
    auto it = std::find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}

bool create_DB_metadata(fs::path DBfolder, DB_Header header, std::string dbName) {
    
    std::fstream file((fs::path(DBfolder) / "DBHeaderMeta.bin"), std::ios::binary | std::ios::out);  
    if(!file) {
        std::cerr << "Failed to create header" << std::endl;
        return false;
    }
    file.write(reinterpret_cast<const char*>(&header), sizeof(DB_Header));
    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }
    uint32_t len = dbName.size();
    std::cout << "Len when writting: " << len << std::endl;
    std::cout << "Database name when writting: " << dbName << std::endl;
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(dbName.data(), len);
    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }

    return true;
}

bool create_TB_metadata(fs::path TBfolder, TB_Header header, std::string TBName) {

    std::fstream file((fs::path(TBfolder) / "TBHeaderMeta.bin"), std::ios::binary | std::ios::out);  

    if(!file) {
        std::cerr << "Failed to create header" << std::endl;
        return false;
    }

    file.write(reinterpret_cast<const char*>(&header), sizeof(TB_Header));

    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }
    uint32_t len = TBName.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(TBName.data(), len);
    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }
    return true;
}

bool create_recordBank(fs::path TBfolder, RecordBankHeader RBheader) {
    
    std::fstream file((fs::path(TBfolder) / "RecordBank.bin"), std::ios::binary | std::ios::out);  

    if(!file) {
        std::cerr << "Failed to create Record Bank." << std::endl;
        return false;
    }

    file.write(reinterpret_cast<const char*>(&RBheader), sizeof(RecordBankHeader));

    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }
    return true;

}
bool add_table_DB_metadata(std::fstream& file, std::string table_name) {
    file.flush();
    file.seekg(offsetof(DB_Header, NUM_TABLES), std::ios::beg);

    uint32_t current_tb_count = 0;
    file.read(reinterpret_cast<char*>(&current_tb_count), sizeof(current_tb_count));
    if(!file) {
        std::cerr << "Failed to load table counter from database metadata." << std::endl;
        return false;
    }

    uint32_t incrimented_tb_count = current_tb_count;
    incrimented_tb_count++;

    file.clear();
    file.seekp(offsetof(DB_Header, NUM_TABLES), std::ios::beg);
    file.write(reinterpret_cast<const char*>(&incrimented_tb_count), sizeof(incrimented_tb_count));
    if(!file) {
        std::cerr << "Failed to incriment table counter into database metadata." << std::endl;
        return false;
    }

    file.seekp(0, std::ios::end);

    uint32_t len = table_name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(table_name.data(), len);
    if(!file) {
        std::cerr << "Failed to add new table name into database metadata." << std::endl;
        return false;
    }
    file.seekg(0, std::ios::beg);
    file.seekp(0, std::ios::beg);

    return true;
}

bool validate_Database_header(std::fstream& file, std::string dbName) {

    file.clear();
    file.seekg(0, std::ios::beg);

    DB_Header this_Header;
    std::string name_from_file;
    uint32_t name_len = 0;

    file.read(reinterpret_cast<char*>(&this_Header), sizeof(DB_Header));

    std::cout << "sizeof(DB_Header): " << sizeof(DB_Header) << std::endl;
    file.seekg(sizeof(this_Header), std::ios::beg);

    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name_from_file.resize(name_len);
    file.read(name_from_file.data(), name_len);

    if(!file) {
        std::cerr << "Failed to load database metadata" << std::endl;
        return false;
    }

    if(this_Header.MAGIC != DBHEADER.MAGIC) {
        std::cerr << "Database has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != DBHEADER.VERSION) {
        std::cerr << "Database has invalid file VERSION" << std::endl;
        return false;
    }
    /*
    if(name_from_file != dbName) {
        std::cerr << "Database name is not the same as its header file" << std::endl;
        std::cerr << "Len_from_file: " << name_len << std::endl;
        std::cerr << "name_from_file: " << name_from_file << std::endl;
        return false;
    }
    */
    return true;
}
DB_Data load_Database_tableList(std::fstream& file) {

    DB_Data data;
    file.seekg(sizeof(DB_Header), std::ios::beg);
    std::string dbName;
    uint32_t dbName_len;
    file.read(reinterpret_cast<char*>(&dbName_len), sizeof(dbName_len));
    dbName.resize(dbName_len);
    file.read(dbName.data(), dbName_len);
    std::cout << "Database name when lead function: " << dbName << std::endl; 
    if(!file) {
        throw std::runtime_error("Failed to load database name from metadata");
    }

    data.name = dbName;

    while(file.peek() != EOF) {

        std::string TableName;
        uint32_t TableName_len;
        file.read(reinterpret_cast<char*>(&TableName_len), sizeof(TableName_len));
        TableName.resize(TableName_len);
        file.read(TableName.data(), TableName_len);
        if(!file) {
            throw std::runtime_error("Failed to load a Table name from its databases metadata");
        }

        data.table_names.push_back(TableName);
        
    }

    return data;
}

struct connection_files {
    std::fstream DB_metadata;
    //Key is table name for these hash maps
    std::unordered_map<std::string, std::fstream> table_metadatas;
    std::unordered_map<std::string, std::fstream> record_banks;
};
 //This function throws error
connection_files openFiles(fs::path db_path, std::string dbName) {

    connection_files files;

    files.DB_metadata.open((fs::path(db_path) / "DBHeaderMeta.bin"), std::ios::binary | std::ios::in | std::ios::out);
    if(!files.DB_metadata) {
        throw std::runtime_error("Failed to find or open DBHeaderMeta file");
    }
    if(!validate_Database_header(files.DB_metadata, dbName)) {
        throw std::runtime_error("Invalid Header");
    }

    std::vector<std::string> table_names;
    /*
    if(!files.DB_metadata) {
        throw std::runtime_error("Failed to load number of tables from Databases metadata");
    }
    if(num_tables = 0) {
        throw std::runtime_error("Load number of tables from Databases metadata was equal to 0");
    }
    
    std::cout << "Num tables: " << num_tables << std::endl;
    */
    while(files.DB_metadata.peek() != EOF) {
        std::string table_name;
        uint32_t len = 0;

        files.DB_metadata.read(reinterpret_cast<char*>(&len), sizeof(len));
        table_name.resize(len);
        files.DB_metadata.read(table_name.data(), len);

        if(!files.DB_metadata) {
            throw std::runtime_error("Failed to load table name from Databases metadata");
        }

        std::cout << "Table name: " << table_name << std::endl;

        table_names.push_back(table_name);
    }

    for(auto name : table_names) {
        
        fs::path table_path = db_path;
        table_path /= name;

        std::fstream metadata(fs::path(table_path) / "TBHeaderMeta.bin", std::ios::binary | std::ios::in | std::ios::out);
        if(!metadata) {
            throw std::runtime_error("Failed to Open Table metadata");
        }

        std::fstream record(fs::path(table_path) / "RecordBank.bin", std::ios::binary | std::ios::in | std::ios::out);
        if(!record) {
            throw std::runtime_error("Failed to Open RecordBank");
        }

        files.table_metadatas[name] = std::move(metadata);
        files.record_banks[name] = std::move(record);
    }
    return files;
}

bool closeFiles(connection_files& files) {
    return true;
} 

bool CREATE_DATABASE(std::string input_DBname) {
    std::string dbName = input_DBname + "_DB";
    
    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;

    if(!fs::exists(db_path)) {
        fs::create_directory(db_path);
        create_DB_metadata(db_path, DBHEADER, input_DBname);
    }
    return true;
}

bool DROP_DATABASE(std::string input_DBname) {
    std::string dbName = input_DBname + "_DB";

    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;
    
    if(!fs::exists(db_path)) {
        std::cerr << "Database does not Exist" << std::endl;
        return false;
    }
    fs::remove_all(db_path);
    return true;
}

//this function throws errors
connection_files CONNECT(std::string input_DBname) {
  
    connection_files files;

    std::string dbName = input_DBname + "_DB";
    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;

    if(!fs::exists(db_path)) {
        throw std::runtime_error("Database does not exists");
    }

    try {
        files = std::move(openFiles(db_path, input_DBname));
    }
    catch (std::runtime_error e) {
        throw std::runtime_error(e.what());
    }

    return files;
}

bool validate_Table_header(std::fstream& file, std::string TName) {
    return true;
}
bool CREATE_TABLE(std::fstream& DB_metadata, std::vector<std::string>& table_list,
                  fs::path db_path, std::string input_name, bool overrites = true) {

    
    fs::path table_path = db_path;
    table_path /= input_name;
    

    if(overrites) {
        fs::create_directory(table_path);
        if(!isInVector(input_name, table_list)) {
            add_table_DB_metadata(DB_metadata, input_name);
        }
        create_TB_metadata(table_path, TBHEADER, input_name);
        create_recordBank(table_path, RBHEADER);
    } 
    else {
        if(!fs::exists(table_path)){
            fs::create_directory(table_path);
            if(!isInVector(input_name, table_list)) {
                add_table_DB_metadata(DB_metadata, input_name);
            }
            create_TB_metadata(table_path, TBHEADER, input_name);
            create_recordBank(table_path, RBHEADER);
        }
    }
    return true;
}
bool DROP_TABLE(fs::path db_path, std::string table_name) {
    return true;
}

int main() {
    std::string name = "WorkSpace";
    if(!CREATE_DATABASE("WorkSpace")) {
        std::cerr << "CREATE DATABASE command failed to execute." << std::endl;
        return 1;
    }

    connection_files files;
    DB_Data data;

    try {
        files = std::move(CONNECT(name));
        data = load_Database_tableList(files.DB_metadata);
    }
    catch (std::runtime_error e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    std::string dbName = name + "_DB";
    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;
    std::string table_name = "dudes";

    if(!CREATE_TABLE(files.DB_metadata, data.table_names, db_path, "dudes")) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!CREATE_TABLE(files.DB_metadata, data.table_names, db_path, "Schedule")) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    /*
    if(!DROP_DATABASE(name)) { 
        std::cerr << "DROP DATABASE command failed to execute." << std::endl;
        return 1;
    }
    */    
    return 0; 
}

