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
#include <variant>

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

enum class DataType : uint32_t {
    INTEIRO = 1, //int
    TEXTO = 2, //string
    REAL = 3 //double
};

struct Constraints_list {

    bool unique = false;
    bool auto_incriment = false;
    bool not_null = false;
    bool primary_key = false;
    bool foreign_key = false;
  
};

struct Column {
    DataType type;
    std::string name;
    Constraints_list constraints;  
};

struct Row {
    using entry = std::variant<int32_t, std::string, double>; 
    std::map<Column, entry> values; 
};

struct Table {
    std::string name;
    std::vector<Column> schema;
    fs::path path;
};

struct DataBase {
    std::string name;
    fs::path baseDir;
    std::vector<std::string> table_names;
    std::vector<fs::path> paths;
};


DB_Header DBHEADER{0x44415641, 2};
TB_Header TBHEADER{0x44415441, 3};
RecordBankHeader RBHEADER{0x44415441, 3};

fs::path buildPath(fs::path cwd, std::string table) {
    cwd /= table;
    return cwd;
}
bool isInVector(const std::string& value, const std::vector<std::string>& v_to_search) {
    const auto& v = v_to_search; 
    auto it = std::find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}
bool isInVector(const fs::path& value, const std::vector<fs::path>& v_to_search) {
    const auto& v = v_to_search; 
    auto it = std::find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}
bool eraseVectorEl(const std::string& value, std::vector<std::string>& v_to_delete) {
    auto& v = v_to_delete;
    v.erase(std::find(v.begin(), v.end(), value));
    if(isInVector(value, v)) return false;
    return true;
}
bool eraseVectorEl(const fs::path& value, std::vector<fs::path>& v_to_delete) {
    auto& v = v_to_delete;
    v.erase(std::find(v.begin(), v.end(), value));
    if(isInVector(value, v)) return false;
    return true;
}
void printVec(const std::vector<std::string>& v) {
    std::cout << "Elements in vector: \n"; 
    for (auto& el : v) {
        std::cout << el << '\n';
    }
}

bool create_DB_metadata(fs::path DBfolder, DB_Header header, 
                        std::string dbName, std::string MetadataName = "DBHeaderMeta.bin") {
    
    std::fstream file((fs::path(DBfolder) / MetadataName), std::ios::binary | std::ios::out);  
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
bool add_table_DB_metadata(std::fstream& file, std::string table_name, std::vector<std::string>& table_names) {
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

    table_names.push_back(table_name);

    file.seekp(offsetof(DB_Header, NUM_TABLES), std::ios::beg);
    file.write(reinterpret_cast<const char*>(&incrimented_tb_count), sizeof(incrimented_tb_count));
    if(!file) {
        std::cerr << "Failed to incriment table counter into database metadata." << std::endl;
        return false;
    }
    file.seekg(0, std::ios::beg);
    file.seekp(0, std::ios::beg);

    return true;
}

bool validate_Database_header(std::fstream& file) {

    file.clear();
    file.seekg(0, std::ios::beg);

    DB_Header this_Header;

    file.read(reinterpret_cast<char*>(&this_Header), sizeof(DB_Header));

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
    return true;
}

std::string get_name_from_DBmetadata(std::fstream& file) {
    //always assumes the name will be right after header
    std::string name;
    uint32_t name_len;

    file.seekg(sizeof(DB_Header), std::ios::beg);

    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name.resize(name_len);
    file.read(name.data(), name_len);

    //std::cout << "Database name when load function: " << name << std::endl; 

    if(!file) {
        throw std::runtime_error("Failed to load database name from metadata");
    }
    return name;
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

std::vector<std::string> getTable_names(std::fstream& file) {

    std::vector<std::string> names;

    file.seekg(sizeof(DB_Header), std::ios::beg);
    uint32_t skip_dbname_offset;
    uint32_t skip_offset_size = sizeof(skip_dbname_offset);
    file.read(reinterpret_cast<char*>(&skip_dbname_offset), skip_offset_size);
    
    file.seekg((sizeof(DB_Header) + skip_offset_size + skip_dbname_offset), std::ios::beg);

    while(file.peek() != EOF) {

        std::string TableName;
        uint32_t TableName_len;
        file.read(reinterpret_cast<char*>(&TableName_len), sizeof(TableName_len));
        TableName.resize(TableName_len);
        file.read(TableName.data(), TableName_len);
        if(!file) {
            throw std::runtime_error("Failed to load a Table name from its databases metadata");
        }

        names.push_back(TableName);
    }

    return names;

}
 //These functions throws errors
std::fstream openFile(fs::path file_path, std::string name) {
    std::fstream file(fs::path(file_path) / name, std::ios::binary | std::ios::in | std::ios::out);
    if(!file) {
        throw std::runtime_error("Failed to open or find file");
    }
    return file;
}

bool closeFile(std::fstream& file) {
    file.close();
    return true;
}


//this function throws errors
DataBase CONNECT(std::string input_DBname) {
  
    DataBase database;
    
    try{   

        std::string dbName = input_DBname + "_DB";

        fs::path db_path = "..";
        db_path /= ".."; 
        db_path /= dbName;

        if(!fs::exists(db_path)) {
            throw std::runtime_error("Database does not exists");
        }
        
        database.baseDir = db_path;
        std::fstream metafile = std::move(openFile(db_path, "DBHeaderMeta.bin"));

        if(!validate_Database_header(metafile)) {
            throw std::runtime_error("Invalid metadata file header");
        }
        std::string name_from_file = get_name_from_DBmetadata(metafile);

        if(name_from_file != input_DBname) {
            throw std::runtime_error("Inputed name does not match name on databases metadata file");
        }
        
        database.name = name_from_file;

        database.table_names = std::move(getTable_names(metafile));

        for (auto name : database.table_names) {
            database.paths.push_back(buildPath(database.baseDir, name));
        }

    }
    catch (std::runtime_error e) {
        throw std::runtime_error(e.what());
    }

    return database;
}

bool validate_Table_header(std::fstream& file, std::string TName) {
    return true;
}
bool CREATE_TABLE(DataBase& database, std::string input_name, bool overrites = true) {

    fs::path table_path = buildPath(database.baseDir, input_name);

    if(overrites) {
        fs::create_directory(table_path);
        if(!isInVector(input_name, database.table_names)) {
            std::fstream metafile(fs::path(database.baseDir) / "DBHeaderMeta.bin", std::ios::binary | std::ios::in | std::ios::out);
            add_table_DB_metadata(metafile, input_name, database.table_names);
            metafile.close();
        }
        create_TB_metadata(table_path, TBHEADER, input_name);
        create_recordBank(table_path, RBHEADER);
    } 
    else {
        if(!fs::exists(table_path)){
            fs::create_directory(table_path);
            if(!isInVector(input_name, database.table_names)) {
                std::fstream metafile(fs::path(database.baseDir) / "DBHeaderMeta.bin", std::ios::binary | std::ios::in | std::ios::out);
                add_table_DB_metadata(metafile, input_name, database.table_names);
                metafile.close();
            }
            create_TB_metadata(table_path, TBHEADER, input_name);
            create_recordBank(table_path, RBHEADER);
        }
    }
    return true;
}
bool DROP_TABLE(DataBase& database, std::string table_name) {

    if(!isInVector(table_name, database.table_names)) {
        std::cerr << "Table " << table_name << " Does NOT Exist" << std::endl;
        return false;
    }

    std::vector<std::string> table_namescp = database.table_names;
    std::vector<fs::path>    table_pathscp = database.paths;

    if(!eraseVectorEl(table_name, table_namescp)) {
        std::cerr << "Failed to erase table name from tb name list" << std::endl;
        return false;
    }

    if(!create_DB_metadata(database.baseDir, DBHEADER, database.name, "temp.bin")){
        std::cerr << "Failed to create temp.bin when rewritting DB metadata" << std::endl;
        fs::remove_all(fs::path(database.baseDir) / "temp.bin");
        return false;
    }

    std::fstream temp(fs::path(database.baseDir) / "temp.bin", std::ios::binary | std::ios::in | std::ios::out);

    if(!temp) {
        std::cout << "Failed to create temp file when dropping table" << std::endl;
        temp.close();
        fs::remove_all(fs::path(database.baseDir) / "temp.bin");
        return false;
    }


    for(const auto& name : table_namescp) {
        if(!add_table_DB_metadata(temp, name, database.table_names)){
            std::cerr << "Failed to add table name: " << name << " to temp metadata" << std::endl;
            temp.close();
            fs::remove_all(fs::path(database.baseDir) / "temp.bin");
            return false;
        }
    }
    fs::remove_all(fs::path(database.baseDir) / "DBHeaderMeta.bin");
    fs::remove_all(fs::path(database.baseDir) / table_name);
    
    auto table_path = buildPath(database.baseDir, table_name);

    fs::rename(fs::path(database.baseDir) / "temp.bin", fs::path(database.baseDir) / "DBHeaderMeta.bin");

    database.table_names = {};
    database.paths       = {};

    database.table_names = table_namescp;
    database.paths       = table_pathscp;

    return true;
}

int main() {
    std::string name = "WorkSpace";
    if(!CREATE_DATABASE("WorkSpace")) {
        std::cerr << "CREATE DATABASE command failed to execute." << std::endl;
        return 1;
    }

    DataBase database;
    
    try {
        database = std::move(CONNECT(name));
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

    if(!CREATE_TABLE(database, "dudes")) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!CREATE_TABLE(database, "Schedule")) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!DROP_TABLE(database, "dudes")) { 
        std::cerr << "DROP TABLE command failed to execute." << std::endl;
        return 1;
    }
    return 0; 
}

