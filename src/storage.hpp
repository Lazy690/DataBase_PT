#include <fstream>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>
#include <list>
#include <filesystem>
#include <span>
#include <cstring>

#include "../classes.h"
#include "filesys.hpp"
#pragma once 

const int KILOBYTE = 1024;
constexpr int PAGE_SIZE = KILOBYTE; 
const int MAXPAGES = 100;

//template declaractions

template<typename T>
std::optional<T> 
read_bytes(std::span<const char> buff, std::size_t& index, std::optional<uint32_t> str_len = std::nullopt) {

    if constexpr (std::is_same_v<T, std::string>) {
        if(str_len == std::nullopt) return std::nullopt;
        if(index + *str_len > buff.size()) return std::nullopt;
        T value;
        value.resize(*str_len);
        std::memcpy(value.data(), buff.data() + index, *str_len);
        index += *str_len;
        return value;
    } else {
        if(index + sizeof(T) > buff.size()) {
            std::cout << index + sizeof(T) << "/" << buff.size() << "\n";
            return std::nullopt;
        }
        T value;
        std::memcpy(&value, buff.data() + index, sizeof(T));
        index += sizeof(T);
        return value;
    }
}

enum class Conditional {
    EQUAL,
    GREATER,
    LESSER,
    GREATERorEQUAL,
    LESSERorEQUAL,
    NotEQUAL
};

// Row Declarations

struct Entry {
    DataType type;
    std::variant<int32_t, std::string, double> value;
};

struct Row { 
    bool tumpstoned = false;
    uint32_t sizeOfRow = 0;
    std::vector<Entry> values = {};
    
    void add_entry(DataType t, std::variant<int32_t, std::string, double> v) {
        if      (t == DataType::INT) sizeOfRow += sizeof(int32_t);
        else if (t == DataType::STRING)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(v).size());
        else if (t == DataType::DOUBLE)    sizeOfRow += sizeof(double);

        sizeOfRow += sizeof(uint32_t);
        values.push_back({t, v});
    };
    void add_entry(Entry& entry) {
        if      (entry.type == DataType::INT) sizeOfRow += sizeof(int32_t);
        else if (entry.type == DataType::STRING)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(entry.value).size());
        else if (entry.type == DataType::DOUBLE)    sizeOfRow += sizeof(double);

        sizeOfRow += sizeof(uint32_t);
        values.push_back(entry);
    };
        
    std::vector<Entry> getValues() {
        return this->values;
    }

    size_t size() {
        return (sizeof(tumpstoned) + sizeof(sizeOfRow) + sizeOfRow);
    }

    void RecalculateSize() {
        sizeOfRow = 0;
        for (auto& entry : values) {
            if      (entry.type == DataType::INT) sizeOfRow += sizeof(int32_t);
            else if (entry.type == DataType::STRING)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(entry.value).size());
            else if (entry.type == DataType::DOUBLE)    sizeOfRow += sizeof(double);
            sizeOfRow += sizeof(uint32_t);
        }

    }

};

// Page Declerations

struct PageHeader {
    uint32_t id        = 0;
    uint32_t freespace = 0;
    uint32_t NumRows   = 0;
    uint32_t LSN       = 0;
};

struct Page {
    PageHeader header;
    bool newAllocated = true;
    bool dirty = false;
    std::vector<char> buffer;
    Page() : buffer(PAGE_SIZE) {}
};

struct PageKey {
    uint32_t tableID = 0;
    uint32_t pageID = 0;
    bool operator==(const PageKey& other) const noexcept {
        if(tableID == other.tableID && pageID == other.pageID) return true;
        return false;
    }
};

struct PageKeyHash {
    size_t operator() (const PageKey& key) const noexcept {
        size_t hash1 = std::hash<uint32_t>{} (key.tableID);
        size_t hash2 = std::hash<uint32_t>{} (key.pageID);
        const auto distribution = 0x9e3779b97f4a7c15ULL;
        return hash1 ^ (hash2 + distribution + (hash1<<6) + (hash1>>2));
    }
};

struct Pager {
    std::unordered_map<uint32_t, RecordHeader> tableMetadata;
    std::unordered_map<PageKey, Page, PageKeyHash> pages;
    
    std::unordered_map<PageKey, std::list<PageKey>::iterator, PageKeyHash> PFIterators;
    std::list<PageKey> PageFrequency;

    Pager() {
        pages.reserve(MAXPAGES);
    }
};

// transaction Declerations

struct TransactionHeader {
    uint32_t id = 0;
    uint32_t NumLogs = 0;
    uint32_t TxSIZE = 0;
    bool commited = false;

    //use size() instead of sizeof() to avoid padding issues when seeking
    uint32_t size() {return (sizeof(uint32_t) * 3) + sizeof(bool);}
};

struct Transaction {
    TransactionHeader header;
    bool start = false;
    std::vector<char> buffer;
};

// Before Image Declarations

struct BeforeImageHeader {
    uint32_t MAGIC           = 0;
    uint32_t VERSION         = 0;
    uint32_t PAGECOUNT       = 0;
    const uint32_t PADDING   = 0;
};

struct BeforeImage {
    Page page;
};

// Log Declarations

enum class LogCMD : int32_t {
    INSERT     = 2,
    DELETE     = 3,
};

struct Log {
    PageKey ID;
    uint32_t LSN = 0;
    LogCMD command;
    PageHeader pageHeader;
    uint32_t offset = 0;
    Row row;

    size_t size() {
        return sizeof(PageKey) + sizeof(LSN) + sizeof(command) + sizeof(PageHeader) + sizeof(offset) + row.size();
    }
};

struct LoggerHeader {
    uint32_t MAGIC                  = 0;
    uint32_t VERSION                = 0;
    uint32_t TxCOUNT                = 0;
    uint32_t LatestCheckpointOffset = 0;
    uint32_t LatestLSN              = 0;
};
struct Logger {
 
    LoggerHeader header;
    BeforeImageHeader beforeImageHeader;

    std::unordered_set<PageKey, PageKeyHash> flushed_beforeImages;

    std::fstream* LoggerFile;
    std::fstream* BeforeImageLogFile;

    Transaction transaction;

};

//File Manager

struct FileManager {

    using tableID = uint32_t;
    std::unordered_map<tableID, std::fstream> files;
    std::fstream LoggerFile;
    std::fstream BeforeImageLogFile;

};

//Queries

struct QueryParams {
    size_t column_index; 
    const Conditional conditional; 
    const std::variant<int32_t, std::string, double> value;
};

//Compare Functions
template<typename T>
bool compare(T RowValue, Conditional conditional, T value, bool negated = false) {

    switch(conditional) {
        case Conditional::EQUAL:
            if(RowValue == value){
                return  true;
            }
            else {
                return  false;
            } 
            break;
        case Conditional::GREATER:
            if(RowValue > value){
                return  true;
            }
            else {
                return  false;
            } 
            break;
        case Conditional::LESSER:
            if(RowValue < value){
                return  true;
            }
            else {
                return false;
            } 
            break;
        case Conditional::GREATERorEQUAL:
            if(RowValue >= value){
                return  true;
            }
            else {
                return  false;
            } 
            break;
        case Conditional::LESSERorEQUAL:
            if(RowValue <= value){
                return  true;
            }
            else {
                return  false;
            } 
            break;
        case Conditional::NotEQUAL:
            if(RowValue != value){
                return  true;
            }
            else {
                return  false;
            } 
            break;
    }
    return false;
};

void printRow(Row& row);

bool loadTableFile(FileManager& manager, const Table& table);
bool loadTableMetadata(FileManager& manager, Pager& pager, RecordHeader& globalRBHeader, const Table& table);
Page* requestPage(std::fstream& file, Pager& pager, PageKey ID, Logger* logger = nullptr);
Page* requestPageWithSpace(std::fstream& file, Logger& logger, Pager& pager, const uint32_t tableID, Row& row);


struct ScanResult {
    size_t pageId = 0;
    size_t offset = 0;
    Row row;
};
struct Set {
    size_t column_index = 0;
    std::variant<int32_t, std::string, double> value;
};

bool ScanUniqueness(std::fstream& file, uint32_t tableID, Pager& pager, size_t column_index, const std::variant<int32_t, std::string, double> value);
bool ScanRowsFromPage(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, uint32_t pageID, Pager& pager);

bool START(const DataBase& database, FileManager& manager, Logger& logger, LoggerHeader& globalLogHeader, BeforeImageHeader& globalImageHeader);
bool COMMIT(DataBase& database, FileManager& manager, Logger* logger, Pager& pager,
            TB_Header& globalTBHEADER, DB_Header& globalDBHEADER, RecordHeader& globalRBHEADER);

bool INSERT(std::fstream& file, uint32_t tableID, Pager& pager, Logger& logger, Row& row);
bool INSERT(std::fstream& file, uint32_t tableID, Pager& pager, Row& row);

bool DELETE(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, Pager& pager, Logger& logger);
bool DELETE(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, Pager& pager);

bool UPDATE(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, Pager& pager, Logger& logger, std::vector<Set> sets);
bool UPDATE(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, Pager& pager, std::vector<Set> sets);
