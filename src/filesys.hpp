#include <string>
#include <optional>
#include <vector>
#include <cstddef> 
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#pragma once

#include "../classes.h"

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
    int32_t LatestAutoIncriment = 0;
};

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t TABLEID   = 0;
    uint32_t PAGECOUNT = 0;
    uint32_t LatestLSN = 0;
};

struct IndexHeader {
    uint32_t MAGIC        = 0;
    uint32_t VERSION      = 0;
    uint32_t ColumnID     = 0;
    uint32_t TableId      = 0;
    uint32_t PageCount    = 0;
    uint32_t LatestPageID = 0;
    DataType Type;
};

struct Table {
    using column_index = uint32_t;
    using column_name  = std::string;
    TB_Header header;
    std::string name;


    std::unordered_map<column_name, column_index> id_lookup;
    std::unordered_map<column_index, Column> schema;

    std::optional<uint32_t> autoIncrimentedColumnIDptr;
    std::unordered_set<column_index> NotNullColumns;
    fs::path path;
};

struct DataBase {

    bool connected = false;

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
    NOT_EXISTS,
    FAILED
};

const std::string DATABASE_FILENAME    = "database.mt";
const std::string TABLE_FILENAME       = "table.mt";
const std::string RECORDBANK_FILENAME  = "data.bin";
const std::string INDEX_FILENAME       = "index.idx";
const std::string LOGGER_FILENAME      = "logger.bin";
const std::string BEFOREIMAGE_FILENAME = "beforeImage.bin";

const std::string BACKUPFOLDER_NAME = "backup";
const std::string INDEXFOLDER_NAME  = "index";

const fs::path BASE_DIRECTORY = fs::path("..");

CONNECTION_STATUS CONNECT(std::string input_DBname, DataBase& database, DB_Header& globalDatabaseHeader, TB_Header& globalTableHeader);
bool CREATE_TABLE(DataBase& database, const TB_Header& globalHeader, RecordHeader& globalRBHEADER, std::string table_name, std::vector<Column> columns, bool overrites);
bool DROP_TABLE(DataBase& database, std::string table_name);
void printDataBase(const DataBase& database);

bool CREATE_INDEX(DataBase& database, const IndexHeader& globalHeader, uint32_t columnID, uint32_t tableID);
