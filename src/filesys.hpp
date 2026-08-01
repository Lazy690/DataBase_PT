#include <string>
#include <vector>
#include <cstddef> 
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#pragma once

namespace fs = std::filesystem;

struct DB_Header {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t ID = 0;
    uint32_t LatestTableID = 0;
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

struct Table {
    using column_index = uint32_t;
    TB_Header header;
    std::string name;
    std::unordered_map<column_index, Column> schema;
    fs::path path;
};

struct DataBase {
    using tableID = uint32_t; 
    using tableName = std::string;

    DB_Header header;
    std::string name;
    fs::path baseDir;

    std::unordered_map<tableName, tableID> id_lookup;
    std::unordered_map<tableID, Table> tables;

    std::unordered_set<tableID> created_tables;
    std::unordered_set<tableID> droped_tables;

};


bool COMMIT_DATABASE_DATA(const DataBase& database, const TB_Header& globalTableHeader, const DB_Header& globalDatabaseHeader, const RecordHeader& globalRecordHeader);
bool CREATE_DATABASE(std::string input_DBname, DB_Header& globalHeader);
bool DROP_DATABASE(std::string input_DBname);

enum class CONNECTION_STATUS {
    CONNECTED,
    NOT_EXTSTS,
    FAILED
};

CONNECTION_STATUS CONNECT(std::string input_DBname, DataBase& database, DB_Header& globalDatabaseHeader, TB_Header& globalTableHeader);
bool CREATE_TABLE(DataBase& database, const TB_Header& globalHeader, std::string table_name, std::vector<Column> columns, bool overrites = true);
bool DROP_TABLE(DataBase& database, std::string table_name);
