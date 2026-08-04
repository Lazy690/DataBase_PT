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
#include <unordered_set>
#include <cstddef> 
#include <variant>

#include "../classes.h"
#include "filesys.hpp"

namespace fs = std::filesystem;

DB_Header globalDBHEADERf{0x44415641, 3};
TB_Header globalTBHEADERf{0x44415441, 4};
RecordHeader globalRBHEADERf{0x44415441, 5};



fs::path buildPath(fs::path cwd, std::string table) {
    cwd /= table;
    return cwd;
}

//=======================
//   header Section 
//=======================

//Database
bool load_DataBase_header(std::fstream& file, DB_Header& header) {
    assert(file.is_open());
    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    assert(file);
    file.seekg(0, std::ios::beg);
    return true;
}
bool flush_DataBase_header(std::fstream& file, const DB_Header& header) {
    assert(file.is_open());
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    assert(file);
    file.seekp(0, std::ios::beg);
    return true;
}
bool validate_DataBase_header(const DB_Header& this_header, const DB_Header& globalHeader) {

    if(this_header.MAGIC != globalHeader.MAGIC) {
        std::cerr << "DataBase metadata File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_header.VERSION != globalHeader.VERSION) {
        std::cerr << "DataBase metadata File has invalid file VERSION" << std::endl;
        return false;
    }
    /*
    if(this_Header.ID != id) {
        std::cerr << "DataBase metadata File has invalid ID" << std::endl;
        return false;
    }
    */

    return true;
}
bool requestDataBaseHeader(std::fstream& file, DB_Header& globalHeader, DB_Header& this_header) {

    if(!load_DataBase_header(file, this_header)) {
        return false;
    };

    if(!validate_DataBase_header(this_header, globalHeader)) {
        return false;
    }
    
    return true;
}


//Table

bool load_Table_header(std::fstream& file, TB_Header& header) {
    assert(file.is_open());
    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    assert(file);
    file.seekg(0, std::ios::beg);
    return true;
}
bool flush_Table_header(std::fstream& file, const TB_Header& header) {
    assert(file.is_open());
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    assert(file);
    file.seekp(0, std::ios::beg);
    return true;
}
bool validate_Table_header(const TB_Header& this_header, const TB_Header& globalHeader, const uint32_t id) {

    if(this_header.MAGIC != globalHeader.MAGIC) {
        std::cerr << "Table metadata File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_header.VERSION != globalHeader.VERSION) {
        std::cerr << "Table metadata File has invalid file VERSION" << std::endl;
        return false;
    }
    if(this_header.ID != id) {
        std::cerr << "Table metadata File has invalid ID" << std::endl;
        return false;
    }

    return true;
}
bool requestTableHeader(std::fstream& file, TB_Header& globalHeader, TB_Header& this_header, const uint32_t id) {

    if(!load_Table_header(file, this_header)) {
        return false;
    };

    if(!validate_Table_header(this_header, globalHeader, id)) {
        return false;
    }
    
    return true;
}

//RecordBank 
bool flush_recordbank_header(std::fstream& file, RecordHeader header) {
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(RecordHeader));
    if(!file) {
        std::cerr << "Failed to write header" << std::endl;
        return false;
    }
    return true;
}

//===================================
// Column Section
//===================================

bool flush_column(std::fstream& file, const Column& column) {
    //Always appends to the end
    assert(file.is_open());
    file.seekp(0, std::ios::end);
    uint32_t type = static_cast<uint32_t>(column.type);
    file.write(reinterpret_cast<const char*>(&type), sizeof(type));
    if(!file) {
        std::cerr << "Failed to load column type\n";
        return false;
    }

    uint32_t len = column.name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(column.name.data(), len);
    if(!file) {
        std::cerr << "Failed to flush column name\n";
        return false;
    }

    file.write(reinterpret_cast<const char*>(&column.constraints), sizeof(column.constraints));
    if(!file) {
        std::cerr << "Failed to flush column constraints\n";
        return false;
    }
    file.seekg(0, std::ios::beg);
    return true;
}
bool load_column(std::fstream& file, Column& column, uint32_t& index) {

    assert(file.is_open());
    file.seekg(index, std::ios::beg);

    //std::cout << "index: " << index << "\n";
    file.read(reinterpret_cast<char*>(&column.type), sizeof(column.type));
    if(!file) {
        std::cerr << "Failed to load column type\n";
        return false;
    }
    index += sizeof(column.type);

    uint32_t len = 0;
    file.read(reinterpret_cast<char*>(&len), sizeof(len));
    assert(len!=0);
    column.name.resize(len);
    file.read(column.name.data(), len);
    if(!file) {
        std::cerr << "Failed to load column name\n";
        return false;
    }

    index += (sizeof(len) + len);

    file.read(reinterpret_cast<char*>(&column.constraints), sizeof(Constraints_list));
    if(!file) {
        std::cerr << "Failed to load column constraints into metadata.\n";
        return false;
    }

    index += (sizeof(Constraints_list));

    file.seekg(0, std::ios::beg);
    return true;
}

void printColumn(const Column& column) {
    std::cout << "Type: ";
    switch(column.type) {
        case DataType::INTEIRO:
            std::cout << "INT\n";
            break;
        case DataType::TEXTO:
            std::cout << "TEXT\n";
            break;
        case DataType::REAL:
            std::cout << "DOUBLE\n";
            break;
    }

    std::cout << "Name: " << column.name << "\n";
    std::cout << "Constraints list: \n";
    if(column.constraints.unique) {
        std::cout << "UNIQUE\n";
    }
    if(column.constraints.auto_incriment) {
        std::cout << "AUTO_INCRIMENT\n";
    }
    if(column.constraints.indexed) {
        std::cout << "INDEXED\n";
    }
    if(column.constraints.not_null) {
        std::cout << "NOT_NULL\n";
    }
    if(column.constraints.primary_key) {
        std::cout << "PRIMARY_KEY\n";
    }
    if(column.constraints.foreign_key) {
        std::cout << "FOREIGN_KEY\n";
    }
}

//===================================
// Table Section
//===================================

bool flush_table_data(std::fstream& file, Table& table) {
    assert(file.is_open());
    if(!flush_Table_header(file, table.header)) {
        return false;
    }

    file.seekp(0, std::ios::end);
    uint32_t len = table.name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(table.name.data(), len);
    if(!file) {
        std::cerr << "Failed to flush table name\n";
        return false;
    }
    file.seekp(0, std::ios::end);

    for (int i = 0; i < table.schema.size(); i++) {
        if(!flush_column(file, table.schema[i])) {
            return false;
        }
    }
    return true;
}

bool load_table_data(std::fstream& file, Table& table, TB_Header globalHeader, uint32_t id) {
    assert(file.is_open());
    uint32_t index = 0;

    if(!requestTableHeader(file, globalHeader, table.header, id)) {
        return false;
    }

    index += sizeof(table.header);
    file.seekg(sizeof(table.header), std::ios::beg);
    
    uint32_t name_len = 0;
    std::string table_name;
    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    table_name.resize(name_len);
    file.read(table_name.data(), name_len);

    table.name = table_name;
    index += (sizeof(name_len) + name_len);
    
    for(uint32_t i = 0; i < table.header.NUM_COLUMNS; i++){
        Column column;
        if(!load_column(file, column, index)) {
            return false;
        }
        table.schema[i] = column;
        table.id_lookup[column.name] = i;
        if(column.constraints.auto_incriment) {
            table.autoIncrimentedColumnIDptr = i;
        }
        if(column.constraints.not_null) {
            table.NotNullColumns.insert(i);
        }
    }
    return true;
}

void printTable(const Table& table) {
    std::cout << "--------------\n";
    std::cout << "Table id: " << table.header.ID << "\n";
    std::cout << "Name: " << table.name << "\n";
    std::cout << "Columns: \n";
    for(uint32_t i = 0; i < table.header.NUM_COLUMNS; i++) {
        auto it = table.schema.find(i);
        if(it == table.schema.end()) continue;
        printColumn(table.schema.at(i));
    }
}

//===================================
// Database Section
//===================================

bool flush_database_data(std::fstream& file, const DataBase& database) {
    assert(file.is_open());
    if(!flush_DataBase_header(file, database.header)) {
        return false;
    }

    file.seekp(0, std::ios::end);
    uint32_t len = database.name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(database.name.data(), len);
    if(!file) {
        std::cerr << "Failed to flush table name\n";
        return false;
    }
    file.seekp(0, std::ios::end);

    for(auto& [tableID, table] : database.tables) {
        file.write(reinterpret_cast<const char*>(&tableID), sizeof(tableID));
        if(!file) {
            std::cerr << "Failed to flush table ID when flushing database\n";
            return false;
        }
    }
    return true;
}

bool load_database_data(std::fstream& file, DataBase& database, DB_Header& globalDatabaseHeader, TB_Header& globalTableHeader, fs::path cwd) {
    assert(file.is_open());
    if(!requestDataBaseHeader(file, globalDatabaseHeader, database.header)) {
        return false;
    };
    database.baseDir = cwd;
    file.seekg(sizeof(DB_Header), std::ios::beg);

    uint32_t name_len = 0;
    std::string db_name;
    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    db_name.resize(name_len);
    file.read(db_name.data(), name_len);

    database.name = db_name;

    uint32_t table_ids_offset = (sizeof(database.header) + sizeof(name_len) + name_len);
    file.seekg(table_ids_offset, std::ios::beg);

    for (int i = 0; i < database.header.NUM_TABLES; i++) {
        
        uint32_t id = 0;
        file.read(reinterpret_cast<char*>(&id), sizeof(id));
        if(!file) {
            std::cerr << "Failed to load table ID when loading database data\n";
            return false;
        }
        std::cout << "ID: " << id << "\n";
        Table table;
        fs::path path = cwd;
        path /= std::to_string(id);
        table.path = path;
        std::fstream tableFile(fs::path(path) / TABLE_FILENAME, std::ios::binary | std::ios::in);
        
        if(!load_table_data(tableFile, table, globalTableHeader, id)) {
            std::cerr << "Failed to load a table\n";
            return false;
        }
        database.id_lookup[table.name] = id;
        database.tables[id] = table;
    }
    return true;
}

void insert_table(DataBase& database, Table& table) {
    database.id_lookup.insert({table.name, table.header.ID});
    database.tables.insert({table.header.ID, table});
}

void printDataBase(const DataBase& database) {
    std::cout << "=========================\n";
    std::cout << database.name << "\n";
    std::cout << "=========================\n";

    std::cout << "Database ID: " << database.header.ID << "\n"; 
    std::cout << "LatestTableID : " << database.header.LatestTableID << "\n"; 

    for (uint32_t i = 0; i < database.header.NUM_TABLES; i++) {
        auto it = database.tables.find(i);
        if(it == database.tables.end()) continue;
        printTable(database.tables.at(i));
    }
}

//===================================
// File SYSTEM Section
//===================================

bool writeNameToDatabaseFile(std::fstream& file, std::string name) {
    file.seekp(0, std::ios::end);
    uint32_t len = name.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(name.data(), len);
    if(!file) {
        std::cerr << "Failed to flush table name\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}
bool createDataBaseFile(fs::path path, std::string name, DB_Header globalHeader) {
    std::fstream file(fs::path(path) / DATABASE_FILENAME, std::ios::binary | std::ios::out | std::ios::trunc);
    if(!flush_DataBase_header(file, globalHeader)) {
        return false;
    }
    if(!writeNameToDatabaseFile(file, name)) {
      return false;
    }
    return true;
}
bool createTableFile(fs::path path, TB_Header globalHeader) {
    std::fstream file(fs::path(path) / TABLE_FILENAME, std::ios::binary | std::ios::out | std::ios::trunc);
    if(!flush_Table_header(file, globalHeader)) {
        return false;
    }
    return true;
}
bool createRecordBankFile(fs::path path, RecordHeader globalHeader) {
    std::fstream file(fs::path(path) / RECORDBANK_FILENAME, std::ios::binary | std::ios::out | std::ios::trunc);
    if(!flush_recordbank_header(file, globalHeader)) {
        return false;
    }
    return true;
}

bool create_new_table_folder(fs::path dir, const TB_Header& globalTableHeader, const RecordHeader& globalRecordHeader) {
    /*
    fs::path new_table_path = dir;
    std::string idSTR;
    idSTR += id;
    new_table_path /= idSTR;
    */
    fs::create_directory(dir);
    if(!createTableFile(dir, globalTableHeader)) {
        return false;
    }
    if(!createRecordBankFile(dir, globalRecordHeader)) {
        return false;
    }
    return true;
}
bool delete_table_folder(fs::path dir) {
    /*
    fs::path folder_path = dir;
    std::string idSTR;
    idSTR += id;
    folder_path /= idSTR;
    */
    if (!fs::exists(dir)) return true;
    fs::remove_all(dir);
    return true;
}

bool COMMIT_DATABASE_DATA(const DataBase& database, const TB_Header& globalTableHeader, const DB_Header& globalDatabaseHeader, const RecordHeader& globalRecordHeader) {
    
    std::fstream DBFile(fs::path(database.baseDir) / DATABASE_FILENAME, std::ios::binary | std::ios::out);
    if(!DBFile) {
        std::cerr << "Failed to open database metadate file\n";
        return false;
    }
    if(!flush_database_data(DBFile, database)) return false;
    
    for (auto& dropedTableid : database.droped_tables) {
        fs::path deletePath = database.baseDir;
        deletePath /= std::to_string(dropedTableid);
        if (!delete_table_folder(deletePath)) {
            return false;
        }
    }
    for (auto& createdTableid : database.created_tables) {
        auto it = database.tables.find(createdTableid);
        assert(it!=database.tables.end());

        fs::path createPath = database.tables.at(createdTableid).path;
        if (!create_new_table_folder(createPath, globalTableHeader, globalRecordHeader)) {
            return false;
        }
    }

    for (auto [tableID, table] : database.tables) {
        std::fstream TBFile(fs::path(table.path) / TABLE_FILENAME, std::ios::binary | std::ios::out);
        if(!TBFile) {
            std::cerr << "Failed to open table metadate file\n";
            return false;
        }
        if(!flush_table_data(TBFile, table)) return false;
    }

    return true;
}

//===================================
// Command Section
//===================================

bool CREATE_DATABASE(std::string input_DBname, DB_Header& globalHeader) {
    std::string dbName = input_DBname + "_DB";
    
    fs::path db_path = BASE_DIRECTORY;
    db_path /= dbName;

    if(!fs::exists(db_path)) {
        fs::create_directory(db_path);
        if(!createDataBaseFile(db_path, dbName, globalHeader)) {
            std::cerr << "Failed to create database metadata" << std::endl;
            return false;
        };
    }

    return true;
}

bool DROP_DATABASE(std::string input_DBname) {
    std::string dbName = input_DBname + "_DB";
    
    fs::path db_path = BASE_DIRECTORY;
    db_path /= dbName;
    
    if(!fs::exists(db_path)) {
        std::cerr << "Database does not Exist" << std::endl;
        return false;
    }
    fs::remove_all(db_path);

    return true;
}
CONNECTION_STATUS CONNECT(std::string input_DBname, DataBase& database, DB_Header& globalDatabaseHeader, TB_Header& globalTableHeader) {

    std::string dbName = input_DBname + "_DB";
    fs::path db_path = BASE_DIRECTORY;
    db_path /= dbName;

    if(!fs::exists(db_path)) {
        return CONNECTION_STATUS::NOT_EXISTS;
    }
    
    std::fstream metafile(fs::path(db_path) / DATABASE_FILENAME, std::ios::binary | std::ios::in);
    if (!metafile) {
        std::cerr << "Failed to open metadata file";
        return CONNECTION_STATUS::FAILED;
    }

    if(!load_database_data(metafile, database, globalDatabaseHeader, globalTableHeader, db_path)) {
        return CONNECTION_STATUS::FAILED;
    }

    database.connected = true;

    return CONNECTION_STATUS::CONNECTED;
}

bool CREATE_TABLE(DataBase& database, const TB_Header& globalHeader, std::string table_name, std::vector<Column> columns, bool overrites) {
    
    Table table;
    table.header.MAGIC   = globalHeader.MAGIC;
    table.header.VERSION = globalHeader.VERSION;


    auto it = database.id_lookup.find(table_name);

    if (!overrites) {
        if (it != database.id_lookup.end()) return true;
        uint32_t new_table_ID = database.header.LatestTableID;
        database.header.LatestTableID++;
        table.header.ID = new_table_ID;
        database.header.NUM_TABLES++;
    }
    else {
        if (it != database.id_lookup.end()) {
            table.header.ID = database.id_lookup.at(table_name);
        }
        else {
            uint32_t new_table_ID = database.header.LatestTableID;
            database.header.LatestTableID++;
            table.header.ID = new_table_ID;
            database.header.NUM_TABLES++;
        }
    }

    table.name = table_name;
    uint32_t columnCount = 0;

    for (const auto& column : columns) {
        table.schema[columnCount]    = column;
        table.id_lookup[column.name] = columnCount;
        if(column.constraints.auto_incriment) {
            table.autoIncrimentedColumnIDptr = columnCount;
        }
        if(column.constraints.not_null) {
            table.NotNullColumns.insert(columnCount);
        }
        columnCount++;
    }

    table.header.NUM_COLUMNS = columnCount;
    table.path = fs::path(database.baseDir) / std::to_string(table.header.ID);

    insert_table(database, table);
    database.created_tables.insert(table.header.ID);
    database.droped_tables.erase(table.header.ID);

    return true;
}
bool DROP_TABLE(DataBase& database, std::string table_name) {

    auto it = database.id_lookup.find(table_name);
    if(it == database.id_lookup.end()) {
        std::cerr << "Table: " << table_name << " Does not Exist\n";
        return true;
    }

    uint32_t table_id = database.id_lookup.at(table_name);
    database.id_lookup.erase(table_name);
    database.tables.erase(table_id);

    database.droped_tables.insert(table_id);
    database.created_tables.erase(table_id);

    database.header.NUM_TABLES--;
    return true;
}

int test_fs() {
    std::string name = "WorkSpace";

    /*
    if(!CREATE_DATABASE("WorkSpace", globalDBHEADER)) {
        std::cerr << "CREATE DATABASE command failed to execute." << std::endl;
        return 1;
    }
    */

    DataBase database;
    
    
    CONNECTION_STATUS status_code = CONNECT(name, database, globalDBHEADERf, globalTBHEADERf);
    if(status_code != CONNECTION_STATUS::CONNECTED) {
        std::cerr << "Failed to connect to db\n";
        return 1;
    }

    printDataBase(database);
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

    if(!CREATE_TABLE(database, globalTBHEADERf, "dudes", a, true)) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    if(!CREATE_TABLE(database, globalTBHEADERf, "Schedule", b, true)) {
        std::cerr << "CREATE TABLE command failed to execute." << std::endl;
        return 1;
    }
    /*
    if(!DROP_TABLE(database, "dudes")) { 
        std::cerr << "DROP TABLE command failed to execute." << std::endl;
        return 1;
    }
    */

    if(!COMMIT_DATABASE_DATA(database, globalTBHEADERf, globalDBHEADERf, globalRBHEADERf)) {
        std::cerr << "Failed to commit changes\n";
        return 1;
    }

    std::cout << "Worked\n";
    return 0; 
}


