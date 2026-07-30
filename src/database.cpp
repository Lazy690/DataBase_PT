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
    uint32_t ID = 0;
    uint32_t NUM_TABLES = 0; 
};

struct TB_Header {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t ID = 0;
    uint32_t NUM_COLUMNS = 0;
};

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t TABLEID   = 0;
    uint32_t PAGECOUNT = 0;
    uint32_t LatestLSN = 0;
};

enum class DataType : uint32_t {
    INTEIRO = 1, //int
    TEXTO = 2, //string
    REAL = 3 //double
};

struct Constraints_list {

    bool unique = false;
    bool auto_incriment = false;
    bool indexed = false;
    bool not_null = false;
    bool primary_key = false;
    bool foreign_key = false;
  
};

struct Column {
    DataType type;
    std::string name;
    Constraints_list constraints;  
};

struct Table {
    std::string name;
    std::vector<Column> schema;
    fs::path path;
};

struct DataBase {
    using tableID = uint32_t; 
    using tableName = std::string;

    std::string name;
    fs::path baseDir;

    std::unordered_map<tableID, tableName> tableNames;
    std::unordered_map<tableName, tableID> tableIDs;
};

DB_Header DBHEADER{0x44415641, 2};
TB_Header TBHEADER{0x44415441, 3};
RecordHeader RBHEADER{0x44415441, 5};

fs::path buildPath(fs::path cwd, std::string table) {
    cwd /= table;
    return cwd;
}

/*
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
*/
 //These functions throws errors
std::fstream openFile(fs::path file_path, std::string name) {
    std::fstream file(fs::path(file_path) / name, std::ios::binary | std::ios::in | std::ios::out);
    if(!file) {
        std::cerr << name << "\n";
        throw std::runtime_error("Failed to open or find file");
    }
    return file;
}

bool add_column_TB_metadata(std::fstream& file, const Column& column) {
    file.flush();
    file.seekg(offsetof(TB_Header, NUM_COLUMNS), std::ios::beg);

    uint32_t current_col_count = 0;
    file.read(reinterpret_cast<char*>(&current_col_count), sizeof(current_col_count));
    if(!file) {
        std::cerr << "Failed to load column counter from table metadata." << std::endl;
        return false;
    }
    uint32_t incrimented_col_count = current_col_count;
    incrimented_col_count++;

    file.seekg(0, std::ios::beg);
    file.seekp(0, std::ios::end);

    file.write(reinterpret_cast<const char*>(&column.type), sizeof(column.type));
    if(!file) {
        std::cerr << "Failed to incriment column type into table metadata." << std::endl;
        return false;
    }

    uint32_t len = column.name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(column.name.data(), len);
    if(!file) {
        std::cerr << "Failed to add new column name into table metadata." << std::endl;
        return false;
    }

    file.write(reinterpret_cast<const char*>(&column.constraints), sizeof(Constraints_list));
    if(!file) {
        std::cerr << "Failed to save column constraints into metadata." << std::endl;
        return false;
    }

    file.seekp(offsetof(TB_Header, NUM_COLUMNS), std::ios::beg);
    file.write(reinterpret_cast<const char*>(&incrimented_col_count), sizeof(incrimented_col_count));
    if(!file) {
        std::cerr << "Failed to incriment column counter into table metadata." << std::endl;
        return false;
    }
    file.seekg(0, std::ios::beg);
    file.seekp(0, std::ios::beg);

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

template<typename HeaderType>
bool flush_header(std::fstream& file, HeaderType header) {
    file.write(reinterpret_cast<const char*>(&header), sizeof(HeaderType));
    if(!file) {
        std::cerr << "Failed to write header" << std::endl;
        return false;
    }
    return true;
}
template<typename HeaderType>
bool create_metadata_file(fs::path dir, HeaderType header, std::string name, std::string filename = "") {
    
    if(filename.empty()) { 
        if constexpr (std::is_same_v<HeaderType, DB_Header>) {
            filename = "DBMetadata.bin";
        }
        else if constexpr (std::is_same_v<HeaderType, TB_Header>) {
            filename = "TBMetadata.bin";
        }
        else {
            std::cerr << "Invalid header type detected" << std::endl;
            return false;
        }
    }
    
    std::cout << filename << "\n";
    std::fstream file(fs::path(dir) / filename, std::ios::binary | std::ios::out | std::ios::trunc);

    if(!file) {
        std::cerr << "Failed to create metadata file" << std::endl;
        return false;
    }

    if(!flush_header<HeaderType>(file, header)) {
        std::cerr << "Failed to flush header into metadata file" << std::endl;
        return false;
    };

    uint32_t len = name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(name.data(), len);
    if(!file) {
        std::cerr << "Failed to save name to header file" << std::endl;
        return false;
    }


    return true;
}

template<typename HeaderType>
bool validate_file_header(std::fstream& file, HeaderType header) {

    file.clear();
    file.seekg(0, std::ios::beg);

    HeaderType this_Header;

    file.read(reinterpret_cast<char*>(&this_Header), sizeof(HeaderType));

    if(!file) {
        std::cerr << "Failed to load file header" << std::endl;
        return false;
    }

    if(this_Header.MAGIC != header.MAGIC) {
        std::cerr << "File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != header.VERSION) {
        std::cerr << "File has invalid file VERSION" << std::endl;
        return false;
    }
    return true;
}
bool create_data_binary(fs::path dir, RecordBankHeader header) {

    std::fstream file((fs::path(dir) / "data.bin"), std::ios::binary | std::ios::out);  

    if(!file) {
        std::cerr << "Failed to create Record Bank." << std::endl;
        return false;
    }

    if(!flush_header(file, header)) {
        std::cerr << "Failed to flush header into records.dat" << std::endl;
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
        if(!create_metadata_file<DB_Header>(db_path, DBHEADER, input_DBname)) {
            std::cerr << "Failed to create database metadata" << std::endl;
            return false;
        };
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
        std::fstream metafile = std::move(openFile(db_path, "DBMetadata.bin"));

        if(!validate_file_header(metafile, DBHEADER)) {
            throw std::runtime_error("Invalid metadata file header");
        }
        std::string name_from_file = get_name_from_DBmetadata(metafile);

        if(name_from_file != input_DBname) {
            throw std::runtime_error("Inputed name does not match name on databases metadata file");
        }
        
        database.name = name_from_file;

        database.table_names = std::move(getTable_names(metafile));

        for (auto name : database.table_names) {
            auto path = buildPath(database.baseDir, name);
            database.paths[name] = path;
        }

    }
    catch (std::runtime_error e) {
        throw std::runtime_error(e.what());
    }

    return database;
}

bool CREATE_TABLE(DataBase& database, std::string input_name, std::vector<Column> columns, bool overrites = true) {
    
    fs::path table_path = buildPath(database.baseDir, input_name);
    
    if((!overrites) && (fs::exists(table_path))) return true;

    fs::create_directory(table_path);

    std::fstream metafile = std::move(openFile(database.baseDir, "DBMetadata.bin"));

    if(!add_table_DB_metadata(metafile, input_name)) {
        return false;
    };

    database.table_names.push_back(input_name);

    metafile.close();
    
    if(!create_metadata_file<TB_Header>(table_path, TBHEADER, input_name)){
        return false;
    }
    if(!create_data_binary(table_path, RBHEADER)) {
        return false;
    };
    
    if (columns.empty()) return true;

    std::fstream file = std::move(openFile(table_path, "TBMetadata.bin"));

    for (auto& col : columns) {
        if(!add_column_TB_metadata(file, col)) {
            std::cerr << "Failed to flush a column into table metadata" << std::endl;
            return false;
        };
    }
    file.close();

    return true;
}
bool DROP_TABLE(DataBase& database, std::string table_name) {

    if(!isInVector(table_name, database.table_names)) {
        std::cerr << "Table " << table_name << " Does NOT Exist" << std::endl;
        return false;
    }

    auto table_namescp = database.table_names;
    auto table_pathscp = database.paths;

    if(!eraseVectorEl(table_name, table_namescp)) {
        std::cerr << "Failed to erase table name from tb name list" << std::endl;
        return false;
    }

    if(!create_metadata_file<DB_Header>(database.baseDir, DBHEADER, database.name, "temp.bin")){
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
        if(!add_table_DB_metadata(temp, name)){
            std::cerr << "Failed to add table name: " << name << " to temp metadata" << std::endl;
            temp.close();
            fs::remove_all(fs::path(database.baseDir) / "temp.bin");
            return false;
        }
    }
    fs::remove_all(fs::path(database.baseDir) / "DBMetadata.bin");
    fs::remove_all(fs::path(database.baseDir) / table_name);
    
    auto table_path = buildPath(database.baseDir, table_name);

    fs::rename(fs::path(database.baseDir) / "temp.bin", fs::path(database.baseDir) / "DBMetadata.bin");

    database.table_names = table_namescp;
    database.paths.erase(table_name);

    return true;
}

int test() {

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
    
    std::vector<Column> a = {{
                             {DataType::INTEIRO, "id", {true, true, true, true, false}}, 
                             {DataType::TEXTO, "name", {}}, 
                             {DataType::REAL, "grade", {}}
                            }};

    std::vector<Column> b = {{
                             {DataType::INTEIRO, "id", {true, true, true, true, false}},
                             {DataType::TEXTO, "Weekday", {}}, 
                             {DataType::INTEIRO, "isManditory", {}}
                            }};

    if(!CREATE_TABLE(database, "dudes", a)) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!CREATE_TABLE(database, "Schedule", b)) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!DROP_TABLE(database, "dudes")) { 
        std::cerr << "DROP TABLE command failed to execute." << std::endl;
        return 1;
    }
    return 0; 
}

