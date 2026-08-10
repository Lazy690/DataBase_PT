#include <iostream>
#include <fstream>
#include <cstdint>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cstddef> 
#include <optional>
#include <cassert>
#include <variant>
#include <vector>
#include <cstring>
#include <span>
#include <list>
#include <filesystem>

#include "classes.h"
#include "filesys.hpp"
#include "storage.hpp"
#include "../headers.hpp"



//===================================
// Type Declaration and Template Section
//===================================

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

//index Declerations

struct Node {
    PageKey id;
    Entry entry;
    uint32_t offset = 0;
};

struct BranchHeader {

};

struct Branch {

};

struct TreeHeader {

};

struct Tree {
    //B+ tree
};

struct Indexer {

};

//===================================
// File Section
//===================================

std::fstream openFile(const std::filesystem::path path, std::string fileName) {
    std::fstream file(std::filesystem::path(path) / fileName, std::ios::binary | std::ios::in | std::ios::out);
    //temporary commented assert
    //assert(file.is_open());
    return file;
}

bool loadTableFile(FileManager& manager, const Table& table) {
 
    auto it = manager.files.find(table.header.ID);
    if(it != manager.files.end()) return true; 
    std::filesystem::path filePath = table.path;
    manager.files[table.header.ID] = std::move(openFile(filePath, RECORDBANK_FILENAME));
    if(!manager.files[table.header.ID]) {
        std::cerr << "Table file not found\n";
        return false;
    }
    return true;
 
}

bool loadTableFile(FileManager& manager, const uint32_t& tableID, std::filesystem::path filePath) {
 
    auto it = manager.files.find(tableID);
    if(it != manager.files.end()) return true; 
    manager.files[tableID] = std::move(openFile(filePath, RECORDBANK_FILENAME));
    if(!manager.files[tableID]) {
        std::cerr << "Table file not found\n";
        return false;
    }
    return true;
 
}


//===================================
// RecordBank Section
//===================================

bool load_RecordBank_header(std::fstream& file, RecordHeader& header, uint32_t tableID) {
    file.seekg(0, std::ios::beg);
      
    file.read(reinterpret_cast<char*>(&header), sizeof(RecordHeader));
    if(!file) {
        std::cerr << "Failed to load file header" << std::endl;
        return false;
    }
    if(header.TABLEID != tableID) {
        std::cerr << "Table not found\n";
        return false;
    }
    return true;
}

uint32_t count_pages(std::fstream& file) {
    file.seekg(0, std::ios::end);
    uint32_t EndOfFile = file.tellg();
    
    uint32_t file_size = EndOfFile - sizeof(RecordHeader);

    uint32_t page_count = (file_size / (PAGE_SIZE + sizeof(PageHeader)));
    file.seekg(0, std::ios::beg);

    return page_count;
}

bool validate_RecordBank_header(RecordHeader& globalHeader, RecordHeader& this_Header, uint32_t numPages) {
    if(this_Header.MAGIC != globalHeader.MAGIC) {
        std::cerr << "RecordBank File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != globalHeader.VERSION) {
        std::cerr << "RecordBank File has invalid file VERSION" << std::endl;
        return false;
    }

    if (this_Header.PAGECOUNT != numPages) {
        std::cerr << "File header's page count is out of sync or corrupted" << std::endl;
        return false;
    }

    return true;
}

bool requestRecordBankHeader(std::fstream& file, RecordHeader& globalHeader, RecordHeader& this_header, uint32_t tableID) {

    if(!load_RecordBank_header(file, this_header, tableID)) {
        return false;
    };
    //std::cout << "header id : " << this_header.TABLEID <<"\n";
  
    uint32_t pageCount = count_pages(file);

    if(!validate_RecordBank_header(globalHeader, this_header, pageCount)) {
        return false;
    }
    
    return true;
}

bool loadTableMetadata(FileManager& manager, Pager& pager, RecordHeader& globalRBHeader, const Table& table) {

    uint32_t id = table.header.ID;
    auto it = pager.tableMetadata.find(id);
    if(it != pager.tableMetadata.end()) {
        return true;
    }

    if(!loadTableFile(manager, table)) return false;
    RecordHeader new_header;
    pager.tableMetadata[id] = new_header;
    if(!requestRecordBankHeader(manager.files.at(id), globalRBHeader, pager.tableMetadata.at(id), id)) return false;
    return true;

}

bool flush_metadata(std::fstream& file, RecordHeader header) {
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(RecordHeader));
    if(!file) {
        std::cerr << "Failed to write header" << std::endl;
        return false;
    }
    return true;
}

//===================================
// Row Section
//===================================


std::vector<char> serializeRow(Row& row) {

    std::vector<char> bytes;
    bytes.reserve(row.size());

    uint8_t tomb = row.tumpstoned ? 1 : 0;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&tomb), reinterpret_cast<const char*>(&tomb) + sizeof(tomb));

    uint32_t rowSize = row.sizeOfRow;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&rowSize), reinterpret_cast<const char*>(&rowSize) + sizeof(rowSize));

    for (auto& entry : row.getValues()) {
        uint32_t type_u = static_cast<uint32_t>(entry.type);
        bytes.insert(bytes.end(), reinterpret_cast<const char*>(&type_u), reinterpret_cast<const char*>(&type_u) + sizeof(type_u));
          
        if(entry.type == DataType::INT) {
            const int32_t integer = std::get<int32_t>(entry.value);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&integer), reinterpret_cast<const char*>(&integer) + sizeof(integer));
        }
        else if(entry.type == DataType::STRING) {
            const std::string& str = std::get<std::string>(entry.value);
            const uint32_t len = static_cast<uint32_t>(str.size());
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&len), reinterpret_cast<const char*>(&len) + sizeof(len));
            bytes.insert(bytes.end(), str.begin(), str.end());
        }
        else if(entry.type == DataType::DOUBLE) {
            const double dub = std::get<double>(entry.value);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&dub), reinterpret_cast<const char*>(&dub) + sizeof(dub));
        }
    }

    return bytes;
}

std::optional<Row>
deserializeRow(std::span<const char> rowBytes) {
    Row row;
    std::size_t index = 0;

    auto tomb_u = read_bytes<uint8_t>(rowBytes, index);
    if(!tomb_u) { 
        std::cerr << "index went over the buffer size\n"; 
        return std::nullopt; 
    }
    row.tumpstoned = (*tomb_u != 0);

    std::optional<uint32_t> sizeOfRow = read_bytes<uint32_t>(rowBytes, index);
    if(!sizeOfRow) {
        std::cerr << "index went over the buffer size" << std::endl;
        return std::nullopt;
    }

    std::vector<Entry> entries;
    std::size_t end = index + *sizeOfRow;
    int count = 0;
    
    while (index < end) {

        //std::cout << "deserializeRow iteration: " << ++count << std::endl;
        //std::cout << "deserializeRow position: " << index << "/" << end << std::endl;

        auto type_u = read_bytes<uint32_t>(rowBytes, index);
        if(!type_u) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
        DataType dtype = static_cast<DataType>(*type_u);

        Entry entry;
        entry.type = dtype;

        switch (entry.type) {
            case DataType::INT: {
                auto integer = read_bytes<int32_t>(rowBytes, index);
                if(!integer) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
                entry.value = *integer;
                break;
            }
            case DataType::DOUBLE: {
                auto dub = read_bytes<double>(rowBytes, index);
                if(!dub) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
                entry.value = *dub;
                break;
            }
            case DataType::STRING: {
                auto str_len = read_bytes<uint32_t>(rowBytes, index);
                if(!str_len) { std::cerr << "index went over the buffer size or string len was null\n"; return std::nullopt; }
                auto str = read_bytes<std::string>(rowBytes, index, str_len);
                if(!str) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
                entry.value = *str;
                break;
            }
        }

        //std::cout << "deserializeRow position after: " << index << "/" << end << std::endl;
        entries.push_back(std::move(entry));
    }
    
    row.sizeOfRow  = *sizeOfRow;
    row.values     = std::move(entries);
    //printRow(row);

    return row;
}

void insertRowIntoBuff(std::vector<char>& buff, Row& row, size_t offset) {
    std::vector<char> RowBytes = serializeRow(row);
    std::memcpy(buff.data() + offset, RowBytes.data(), RowBytes.size());
}
void eraseRowFromBuff(std::vector<char>& buff, Row& row, size_t offset) {
    std::vector<char> emptyBytes;
    emptyBytes.resize(row.size());
    std::memcpy(buff.data() + offset, emptyBytes.data(), emptyBytes.size());
}

void printRow(Row& row) {
    std::cout << "tumpstoned: " << row.tumpstoned << "\n";
    std::cout << "row size:   " << row.sizeOfRow  << "\n";
    std::cout << "Entries: \n";
    for (auto& entry : row.values) {
        switch (entry.type) {
            case DataType::INT:
                std::cout << "Data Type: INTEIRO" << "\n";
                std::cout << "Value: " << std::get<int32_t>(entry.value) << "\n";
                break;
            case DataType::DOUBLE:
                std::cout << "Data Type: REAL" << "\n";
                std::cout << "Value: " << std::get<double>(entry.value) << "\n";
                break;
            case DataType::STRING:
                std::cout << "Data Type: TEXTO" << "\n";
                std::cout << "Value: " << std::get<std::string>(entry.value) << "\n";
                break;
        }
    }
    std::cout << "------------------\n";
}

bool loadRow(std::ifstream& file, Row& row, std::vector<DataType> types) {
    //This function if for tests only and assumes the txt file always has the right format!!!
    //Do not use in production yet!
    std::string line;

    if(!std::getline(file, line)) {
        return false;
    };

    std::stringstream ss(line);
    std::string w;
    
    std::vector<std::string> words;
    while(ss >> w) {
        words.push_back(w);
    }
    for (int i = 0; i < types.size(); i++) {

        Entry entry;
        entry.type  = types[i];

        int32_t integer = 0;
        double  dub     = 0;
        switch(types[i]) {
            case DataType::INT:
                integer = std::stoll(words[i]);
                entry.value = integer;
                break;
            case DataType::STRING:
                entry.value = words[i];
                break;
            case DataType::DOUBLE:
                dub = std::stod(words[i]);
                entry.value = dub;
                break;
        }
        row.add_entry(entry);
    }
    return true;
}
bool saveRow(std::ofstream& file, const Row& row, const std::vector<DataType>& types) {
    //This function is for tests only and assumes the Row has the correct format!!!
    //Do not use in production yet!
    
    if (!file.is_open()) {
        return false;
    }

    std::stringstream ss;
    
    for (int i = 0; i < types.size(); i++) {
        Entry entry = row.values[i];  
        
        switch(types[i]) {
            case DataType::INT:
                ss << std::get<int32_t>(entry.value);
                break;
            case DataType::STRING:
                ss << std::get<std::string>(entry.value);
                break;
            case DataType::DOUBLE:
                ss << std::get<double>(entry.value);
                break;
        }
        
        if (i < types.size() - 1) {
            ss << " ";  // Space-separated values
        }
    }
    
    file << ss.str() << "\n";
    return file.good();
}

//===================================
// Page Section
//===================================

bool flush_transaction(std::fstream& file, Transaction& transaction, uint32_t flush_offset);
bool AppendBeforeImage(Logger& logger, BeforeImage& before);
bool flush_logger_header(std::fstream& file, LoggerHeader& header);

std::optional<Page>
load_page(std::fstream& file, const int id) {
 
    Page page;

    size_t pageOffset = sizeof(RecordHeader) + id * (sizeof(PageHeader) + PAGE_SIZE);
    file.clear();
    file.seekg(pageOffset);
    file.read(reinterpret_cast<char*>(&page.header), sizeof(PageHeader));

    if(!file) {
        std::cerr << "Failed to load page header" << std::endl;
        std::cout << "Cursor: " << file.tellg() << "\n";
        return std::nullopt;
    }
    if(page.header.id != id) {
        std::cerr << "Requested Page does not exist" << std::endl;
        return std::nullopt;
    }

    const int DataBytesOffs = pageOffset + sizeof(PageHeader);
    file.seekg(DataBytesOffs, std::ios::beg);

    file.read(page.buffer.data(), PAGE_SIZE);

    if(!file) {
        std::cerr << "Failed to load byte buffer" << std::endl;
        return std::nullopt;
    }

    page.newAllocated = false;

    return page;
}

bool flush_page(std::fstream& file, const Page& page) {

    file.clear();
    int pageOffset = sizeof(RecordHeader) + page.header.id * (sizeof(PageHeader) + PAGE_SIZE);
    file.seekp(pageOffset, std::ios::beg);

    file.write(reinterpret_cast<const char*>(&page.header), sizeof(PageHeader));
    if(!file || file.tellp() != (pageOffset + sizeof(PageHeader))) {
        std::cerr << "Failed to flush header" << std::endl;
        return false;
    }

    const int DataBytesOffs = sizeof(PageHeader) + pageOffset;
    file.seekp(DataBytesOffs, std::ios::beg);
    
    file.write(page.buffer.data(), PAGE_SIZE);

    //std::cout << "Page: " << page.header.id << " flushed successfully\n";

    return true;
}

bool updatePageFrequency(Pager& pager, PageKey ID) {
    
    auto lookup = pager.PFIterators.find(ID);
    if(lookup == pager.PFIterators.end()) {
        pager.PageFrequency.push_front(ID);

        auto it = pager.PageFrequency.begin();
        pager.PFIterators.insert({ID, it});
    }
    else {
        pager.PageFrequency.splice(pager.PageFrequency.begin(),
                                   pager.PageFrequency,
                                   pager.PFIterators.at(ID));
    }
    return true;
}

Page create_page(uint32_t id) {
    PageHeader newHeader = {id, 0, 0};
    Page newPage;
    newPage.header = newHeader;
    newPage.dirty = true;
    return newPage;
}

bool evictPage(std::fstream& file, Pager& pager, Logger* logger = nullptr) {
    PageKey key = pager.PageFrequency.back();
    ////////////////////
    ///TEMPORARY!!
    auto it = pager.pages.find(key);
    if (it == pager.pages.end()) {
        std::cerr << "Could not find page in pager when trying to evict it\n";
        return false;
    }

    Page* page = &pager.pages.at(key);

    if(page->dirty) {
        if(logger != nullptr) {
            assert(logger->LoggerFile->is_open());
            if(!flush_logger_header(*logger->LoggerFile, logger->header)) {
                std::cerr << "Failed to flush logger's header when evicting page\n";
                return false;
            }
            //std::cout << "flushing logger: " << key.pageID << " When evicting\n";
            uint32_t flush_offset = logger->header.LatestCheckpointOffset;
            if(!flush_transaction(*logger->LoggerFile, logger->transaction, flush_offset)) {
                std::cerr << "Failed to flush logger when evicting page\n";
                return false;
            }
        }
        //std::cout << "flushing page: " << key.pageID << " When evicting\n";
        if(!flush_page(file, *page)) {
            std::cerr << "Failed to flush page when evicting it\n";
            return false;
        }
    }
    
    pager.PFIterators.erase(key);
    pager.PageFrequency.pop_back();
    pager.pages.erase(key);
    //std::cout << "Evicting: " << key.pageID << "\n";
    return true;
}

void mark_dirty(Page& page) {
    page.dirty = true;
}
void mark_clean(Page& page) {
    page.dirty = false;
}
bool will_fit(const Page& page, size_t rowSize) {
    return page.header.freespace + rowSize < PAGE_SIZE;
};

Page* requestPage(std::fstream& file, Pager& pager, PageKey ID, Logger* logger = nullptr) {
    Page* page = nullptr;
    auto it = pager.pages.find(ID);
    if (it == pager.pages.end()) {
        auto loadedPage = load_page(file, ID.pageID);
        if(!loadedPage) {
            return page;
        }
        //std::cout << "Page: " << ID.pageID << " Loaded\n";
        if(pager.pages.size() == MAXPAGES) {
            evictPage(file, pager, logger);
        }

        pager.pages.insert({ID, *loadedPage});

        /*
        if(logger != nullptr) {
            if(!logger->flushed_beforeImages.contains(ID)) {
                BeforeImage before;
                if(!loadedPage->newAllocated) {
                    before.page = *loadedPage;
                    if(!AppendBeforeImage(*logger, before)) {
                        return page;
                    }
                }
                logger->flushed_beforeImages.insert(ID);
            }
        }
        */
    }
    //else std::cout << "Cache hit on page: " << ID.pageID << "\n";
    page = &pager.pages.at(ID);
    
    updatePageFrequency(pager, ID);
    return page;
}

Page* requestPageWithSpace(std::fstream& file, Logger& logger, Pager& pager, const uint32_t tableID, Row& row) {

    Page* page = nullptr;

    //Just in case (-_-)
    if (row.size() > PAGE_SIZE) {
        std::cerr << "Inserion of row will end in Page overflow\n";
        return page;
    }

    uint32_t latestID = pager.tableMetadata[tableID].PAGECOUNT;
    if (latestID == 0) {
        pager.tableMetadata[tableID].PAGECOUNT++;
        if(pager.pages.size() == MAXPAGES) {
            evictPage(file, pager, &logger);
        }
        Page newPage = create_page(latestID);
        pager.pages.insert({{tableID, latestID}, newPage});
        page = &pager.pages.at({tableID, latestID});
    }
    else {
        latestID--;
        page = requestPage(file, pager, {tableID, latestID}, &logger);
        if(!page) {
            return page;
        }

        if (!will_fit(*page, row.size())) {
            pager.tableMetadata[tableID].PAGECOUNT++;
            latestID++;
            if(pager.pages.size() == MAXPAGES) {
                evictPage(file, pager, &logger);
            }
            pager.pages.insert({{tableID, latestID}, create_page(latestID)});
            page = &pager.pages.at({tableID, latestID});
        }

    }
    return page;
}


//===================================
// Before Image Section
//===================================

bool flush_beforeImage_header(std::fstream& file, BeforeImageHeader& header) {
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if(!file) {
        std::cout << "Failed to flush beforeImage header\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool load_beforeImage_header(std::fstream& file, BeforeImageHeader& header) {
    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    if(!file) {
        std::cout << "Failed to load beforeImage header\n";
        return false;
    }
    file.seekg(0, std::ios::beg);
    return true;
}

bool validate_BeforeImage_header(BeforeImageHeader& globalHeader, BeforeImageHeader& this_Header) {
    if(this_Header.MAGIC != globalHeader.MAGIC) {
        std::cerr << "Before Image File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != globalHeader.VERSION) {
        std::cerr << "Before Image File has invalid file VERSION" << std::endl;
        return false;
    }

    return true;
}

bool requestBeforeImageHeader(std::fstream& file, BeforeImageHeader& globalHeader, BeforeImageHeader& this_header) {

    if(!load_beforeImage_header(file, this_header)) {
        return false;
    };

    if(!validate_BeforeImage_header(globalHeader, this_header)) {
        return false;
    }
    
    return true;
}

std::optional<BeforeImage>
load_beforeImage(std::fstream& file, const uint32_t PageIndex) {
 
    BeforeImage before;

    size_t pageOffset = sizeof(BeforeImageHeader) + PageIndex * ( sizeof(before.page.header) + PAGE_SIZE);

    //std::cout << "pageOffset: " << pageOffset << "\n";

    file.clear();
    file.seekg(pageOffset);

    file.read(reinterpret_cast<char*>(&before.page.header), sizeof(before.page.header));
    /*
    std::cout << "Before Image header: " << before.page.header.id << ", "
                                         << before.page.header.freespace << ", "
                                         << before.page.header.NumRows << "\n";
                                         */

    if(!file) {
        std::cerr << "Failed to load page header from BeforeImage file" << std::endl;
        return std::nullopt;
    }

    const int DataBytesOffs = pageOffset + sizeof(before.page.header);

    //std::cout << "DataBytesOffs: " << DataBytesOffs << "\n";
    file.clear();
    file.seekg(DataBytesOffs, std::ios::beg);
    //std::cout << "after seek Offs: " << file.tellg() << "\n";

    file.read(before.page.buffer.data(), PAGE_SIZE);

    if(!file) {
        std::cerr << "Failed to load byte buffer from BeforeImage file" << std::endl;
        return std::nullopt;
    }

    return before;
}

bool flush_beforeImage(std::fstream& file, BeforeImage& before) {
    
    file.seekp(sizeof(BeforeImageHeader), std::ios::beg);

    file.write(reinterpret_cast<const char*>(&before.page.header), sizeof(before.page.header));
    if(!file) {
        std::cerr << "Failed to flush header when flushing before image" << std::endl;
        return false;
    }

    //file.seekp(0, std::ios::end);
    
    file.write(before.page.buffer.data(), PAGE_SIZE);

    if(!file) {
        std::cerr << "Failed to flush page buffer when flushing before image" << std::endl;
        return false;
    }
    std::cout << "Page: " << before.page.header.id << " flushed successfully to the BeforeImage file\n";

    return true;
}

//===================================
// Transaction Section
//===================================

bool flush_transaction_header(std::fstream& file, Transaction& transaction, uint32_t flush_offset) {
    assert(transaction.start);
    
    file.seekp(flush_offset, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&transaction.header.id), sizeof(transaction.header.id));
    file.write(reinterpret_cast<const char*>(&transaction.header.NumLogs), sizeof(transaction.header.NumLogs));
    file.write(reinterpret_cast<const char*>(&transaction.header.TxSIZE), sizeof(transaction.header.TxSIZE));

    uint32_t commited = transaction.header.commited ? 1 : 0;
    file.write(reinterpret_cast<const char*>(&commited), sizeof(commited));
    if(!file) {
        std::cout << "Failed to flush logger Transaction Header\n";
        return false;
    }

    file.seekp(0, std::ios::beg);
    return true;
}

bool load_transaction_header(std::fstream& file, uint32_t checkPointOffs, uint32_t TxId ,Transaction& transaction) {
    
    file.seekg(checkPointOffs, std::ios::beg);
    file.read(reinterpret_cast<char*>(&transaction.header.id), sizeof(transaction.header.id));
    file.read(reinterpret_cast<char*>(&transaction.header.NumLogs), sizeof(transaction.header.NumLogs));
    file.read(reinterpret_cast<char*>(&transaction.header.TxSIZE), sizeof(transaction.header.TxSIZE));

    uint32_t commited = 0;
    file.read(reinterpret_cast<char*>(&commited), sizeof(commited));
    transaction.header.commited = (commited != 0);

    if(!file) {
        std::cout << "Failed to load logger Transaction Header\n";
        return false;
    }
    if(transaction.header.id != TxId) {
        std::cout << "Transaction: " << TxId << "Not found\n";
        return false;
    }

    file.seekg(0, std::ios::beg);
    return true;
}
bool flush_transaction_logs(std::fstream& file, Transaction& transaction, uint32_t flush_offset) {
    assert(transaction.start);

    file.seekp(flush_offset, std::ios::beg);
    int32_t buffSize = transaction.buffer.size();
    file.write(reinterpret_cast<const char*>(&buffSize), sizeof(buffSize));
    file.write(transaction.buffer.data(), buffSize);
    if(!file) {
        std::cout << "Failed to flush logger Log buffer\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool load_transaction_logs(std::fstream& file, uint32_t checkPointOffs, uint32_t TxId ,Transaction& transaction) {
    
    file.seekg(checkPointOffs, std::ios::beg);
    int32_t buffSize = 0;
    file.read(reinterpret_cast<char*>(&buffSize), sizeof(buffSize));
    transaction.buffer.resize(buffSize);
    file.read(transaction.buffer.data(), buffSize);

    if(!file) {
        std::cout << "Failed to load logger Log buffer\n";
        return false;
    }
    file.seekg(0, std::ios::beg);
    return true;
}

bool flush_transaction(std::fstream& file, Transaction& transaction, uint32_t flush_offset) {
    if(!flush_transaction_header(file, transaction, flush_offset)) {
        return false;
    }
    flush_offset += sizeof(transaction.header);
    if(!flush_transaction_logs(file, transaction, flush_offset)) {
        return false;
    }
    return true;
}

//===================================
// Logger Section
//===================================

Log CreateLog(const PageKey ID, const LogCMD cmd, const PageHeader Pheader, const uint32_t offset, const Row& row) {
    assert(cmd == LogCMD::INSERT || cmd == LogCMD::DELETE);
    uint32_t emptyLSN = 0;
    return Log{ID, emptyLSN, cmd, Pheader, offset, row};
}

std::vector<char> serializeLog(Log& log) {
    std::vector<char> bytes;
    bytes.reserve(log.size());
    
    PageKey ID = log.ID;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&ID), reinterpret_cast<const char*>(&ID) + sizeof(ID));

    uint32_t LSN = log.LSN;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&LSN), reinterpret_cast<const char*>(&LSN) + sizeof(LSN));

    LogCMD command = log.command;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&command), reinterpret_cast<const char*>(&command) + sizeof(command));

    PageHeader pageHeader = log.pageHeader;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&pageHeader), reinterpret_cast<const char*>(&pageHeader) + sizeof(pageHeader));

    uint32_t offset = log.offset;
    Row row = log.row;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&offset), reinterpret_cast<const char*>(&offset) + sizeof(offset));
    std::vector<char> RowBytes = serializeRow(row);
    bytes.insert(bytes.end(), RowBytes.data(), RowBytes.data() + RowBytes.size());

    return bytes;
}

std::optional<Log> deserializeLog(std::span<const char> logBytes) {
    Log log;
    size_t index = 0;

    auto ID      = read_bytes<PageKey>(logBytes, index);
    if(!ID) {
        std::cerr << "Failed to deserialize ID when deserializing log\n";
        return std::nullopt;
    }
    
    auto LSN     = read_bytes<uint32_t>(logBytes, index);
    if(!LSN) {
        std::cerr << "Failed to deserialize ID when deserializing log\n";
        return std::nullopt;
    }

    auto command = read_bytes<LogCMD>(logBytes, index);
    if(!command) {
        std::cerr << "Failed to deserialize command when deserializing log\n";
        return std::nullopt;
    }

    auto PHeader = read_bytes<PageHeader>(logBytes, index);
    if(!PHeader) {
        std::cerr << "Failed to deserialize command when deserializing log\n";
        return std::nullopt;
    }

    auto offset  = read_bytes<uint32_t>(logBytes, index);
    if(!offset) {
        std::cerr << "Failed to deserialize offset when deserializing log\n";
        return std::nullopt;
    }
    
    auto rowptr = deserializeRow({logBytes.begin() + index, logBytes.end()});
    if(!rowptr) {
        std::cerr << "Failed to deserialize row when deserializing log\n";
        return std::nullopt;
    }

    index += rowptr->size();

    log.ID = *ID;
    log.LSN = *LSN;
    log.command = *command;
    log.pageHeader = *PHeader;
    log.offset = *offset;
    log.row = *rowptr;

    return log;
}

void UpdateLSN(Logger& logger, RecordHeader& header, Page* page, Log& log) {

    if(logger.header.LatestLSN != header.LatestLSN) std::cout << "header LSN: " << header.LatestLSN << " logger LSN: " << logger.header.LatestLSN << "\n";
    assert(logger.header.LatestLSN == header.LatestLSN);
    assert(page!=nullptr);

    logger.header.LatestLSN++;
    header.LatestLSN++;

    uint32_t this_lsn = logger.header.LatestLSN;

    log.LSN   = this_lsn;
    page->header.LSN = this_lsn;

}

void AppendLog(Logger& logger, Log& log) {
    assert(logger.transaction.start && !logger.transaction.header.commited);

    std::vector<char> bytes = serializeLog(log);
    logger.transaction.buffer.insert(logger.transaction.buffer.end(), bytes.data(), bytes.data() + bytes.size());

    logger.transaction.header.TxSIZE += log.size();
}
bool AppendBeforeImage(Logger& logger, BeforeImage& before) {

    if(!flush_beforeImage(*logger.BeforeImageLogFile, before)) {
        return false;
    }

    logger.beforeImageHeader.PAGECOUNT++;
    if(!flush_beforeImage_header(*logger.BeforeImageLogFile, logger.beforeImageHeader)) {
        return false;
    }

    return true;
}

void EmptyLogger(Logger& logger) {

    logger.flushed_beforeImages.clear();
    logger.transaction.buffer.clear();
    logger.transaction.header.commited = false;
    logger.transaction.start = false;

}

bool flush_logger_state(std::fstream& file, Logger& logger, LogCMD state) {
    //assert(state == LogCMD::START || state == LogCMD::COMMIT);
    /*
    switch(state) {
        case LogCMD::START:
            assert(logger.transaction.start && !logger.transaction.header.commited);
            break;
        case LogCMD::COMMIT:
            assert(logger.transaction.start && logger.transaction.header.commited);
            break;
    }
    */
    
    file.seekp(0, std::ios::end);

    file.write(reinterpret_cast<const char*>(&state), sizeof(state));
    if(!file) {
        std::cout << "failed to flush status command to logger\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool load_logger_state(std::fstream& file, uint32_t checkPointOffs, LogCMD& state) {
    
    file.seekp(checkPointOffs, std::ios::beg);

    file.read(reinterpret_cast<char*>(&state), sizeof(state));
    if(!file) {
        std::cout << "failed to Load status command from logger file\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool flush_logger_checkpoint(std::fstream& file, LoggerHeader& header) {

    //const LogCMD checkPoint = LogCMD::CHECKPOINT;
    file.seekp(0, std::ios::end);
    
    //file.write(reinterpret_cast<const char*>(&checkPoint), sizeof(LogCMD));
    //Temporary:
    //std::string s = "CHECKPOINT";
    //file.write(s.data(), s.size());

    if(!file) {
        std::cout << "Failed to flush checkPoint to logger\n";
        return false;
    }

    header.LatestCheckpointOffset = file.tellp();
    file.seekp(0, std::ios::beg);
    return true;
}

void get_logger_checkpoint(std::fstream& file, LoggerHeader& header) {

    file.seekg(0, std::ios::end);
    assert(file.tellg() != -1);
    header.LatestCheckpointOffset = file.tellg();
    file.seekg(0, std::ios::beg);

}

bool flush_logger_header(std::fstream& file, LoggerHeader& header) {
    file.seekp(0, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&header), sizeof(LoggerHeader));
    if(!file) {
        std::cout << "Failed to flush logger Log buffer\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool load_logger_header(std::fstream& file, LoggerHeader& header) {
    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(&header), sizeof(LoggerHeader));
    if(!file) {
        std::cout << "Failed to load loggers header\n";
        return false;
    }
    file.seekg(0, std::ios::beg);
    return true;
}

bool validate_logger_header(LoggerHeader& globalHeader, LoggerHeader& this_Header) {
    if(this_Header.MAGIC != globalHeader.MAGIC) {
        std::cerr << "Logger File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != globalHeader.VERSION) {
        std::cerr << "Logger File has invalid file VERSION" << std::endl;
        return false;
    }

    return true;
}

bool requestLoggerHeader(std::fstream& file, LoggerHeader& globalHeader, LoggerHeader& this_header) {

    if(!load_logger_header(file, this_header)) {
        return false;
    };

    if(!validate_logger_header(globalHeader, this_header)) {
        return false;
    }
    
    return true;
}

//temporary
void ShrinkLoggerFile(LoggerHeader& header) {
    assert(std::filesystem::exists(std::filesystem::path("logger.bin")));
    size_t new_file_size = sizeof(LoggerHeader);
    std::filesystem::resize_file("logger.bin", new_file_size);
    header.LatestCheckpointOffset = 0;
}

void printLog(Log& log) {

    std::cout << "Log: \n";
    std::cout << "Table Id: " << log.ID.tableID 
              << " Page Id: "    << log.ID.pageID << "\n";

    std::cout << "LSN: " << log.LSN;

    std::cout << "Command: ";
    switch(log.command) {
        case LogCMD::INSERT:
            std::cout << "INSERT\n";
            break;
        case LogCMD::DELETE:
            std::cout << "DELETE\n";
            break;
    }

    std::cout << "Offset: " << log.offset << "\n";

    std::cout << "Row: \n";
    printRow(log.row);
}

//===================================
// Recovery Section
//===================================

bool REDO(const DataBase& database, FileManager& manager, uint32_t NumLogs, const std::vector<char>& buff) {
    //std::cout << "REDO\n";

    using LatestLSNCount = uint32_t;
    using tableID        = uint32_t;
    Pager pager;

    std::unordered_map<tableID, LatestLSNCount> tableLSNs;
    
    auto cursor = buff.begin();

    for(size_t i = 0; i < NumLogs; i++) {
        std::span bytes = {cursor, buff.end()};

        if(bytes.size() == 0) break;
        assert(bytes.size()>0);
 
        auto logPtr = deserializeLog(bytes);
        if(!logPtr) {
            std::cerr << "Failed to deserialize Log when REDO ing\n";
            return false;
        }
        cursor += logPtr->size();
        Log log = *logPtr;

        auto it = database.tables.find(log.ID.tableID);
        if(it == database.tables.end()) {
            std::cerr << "Table not found when REDO ing\n";
            return false;
        }
        if(!loadTableFile(manager, it->second)){
            std::cerr << "Failed to load Table file when REDO ing\n";
            return false;
        }
        Page* page = requestPage(manager.files.at(log.ID.tableID), pager, log.ID);
        if(!page) {
            //std::cerr << "Repairing page\n";
            Page newPage = create_page(log.ID.pageID);
            pager.pages.insert({log.ID, newPage});
            page = &pager.pages.at(log.ID);
        }

        //std::cout << "log LSN: " << log.LSN << " page LSN: " << page->header.LSN << "\n";
        if (log.LSN <= page->header.LSN) {
            //std::cout << "Ignoring page\n";
            continue;
        }
        /*
        else {
            std::cout << "Redoing changes to page page\n";
        }
        */

        page->header = log.pageHeader;
        insertRowIntoBuff(page->buffer, log.row, log.offset);
        page->header.LSN = log.LSN;

        tableLSNs[log.ID.tableID] = log.LSN;
    }
    for (auto& [key, page] : pager.pages) {
        auto it = database.tables.find(key.tableID);
        if(it == database.tables.end()) {
            std::cerr << "Table not found when REDO ing\n";
            return false;
        }
        if(!loadTableFile(manager, it->second)){
            std::cerr << "Failed to load Table file when REDO ing\n";
            return false;
        }

        if(!flush_page(manager.files.at(key.tableID), page)) {
            std::cerr << "failed to flush page: " << page.header.id << "\n";
            return false;
        };
    }

    //Repair Files metadata
    for (const auto& [tableID, latestLSN] : tableLSNs) {
        RecordHeader this_header;

        auto it = database.tables.find(tableID);
        if(it == database.tables.end()) {
            std::cerr << "Table not found when REDO ing\n";
            return false;
        }
        if(!loadTableFile(manager, it->second)){
            std::cerr << "Failed to load Table file when REDO ing\n";
            return false;
        }

        if(!load_RecordBank_header(manager.files.at(tableID), this_header, tableID)) {
            std::cout << "Failed to load metadata while Repairing header\n";
            return false;
        };
        //temporary
        this_header.PAGECOUNT = count_pages(manager.files.at(tableID));

        this_header.LatestLSN = latestLSN;
        if(!flush_metadata(manager.files.at(tableID), this_header)) {
            std::cout << "Failed to flush repaired metadata while Repairing header\n";
            return false;
        }
    }

    return true;
}

bool UNDO(const DataBase& database) {
    
    std::filesystem::path backup_path = BASE_DIRECTORY;
    backup_path /= BACKUPFOLDER_NAME;
    backup_path /= database.name;

    if(!fs::exists(backup_path)) {
        std::cerr << "Cannot recover database as the recovery folder must have been deleted\n";
        std::cerr << "Database data may be heavily corrupted\n";
        return false;
    }

    fs::path new_folder_path = BASE_DIRECTORY;
    new_folder_path /= database.name;

    fs::remove_all(database.baseDir);
    fs::copy(backup_path, new_folder_path, fs::copy_options::recursive);
    fs::remove_all(backup_path);

    return true;
}

bool SYNC(const DataBase& database, FileManager& manager, LoggerHeader& globalLogHeader, BeforeImageHeader& globalImageHeader) {

    std::fstream LogFile(std::filesystem::path(BASE_DIRECTORY) / LOGGER_FILENAME, std::ios::binary | std::ios::in | std::ios::out);
    std::fstream BeforeImageLogFile(std::filesystem::path(BASE_DIRECTORY) / BEFOREIMAGE_FILENAME, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);

    if (!LogFile) {
        return true;
    }

    Logger logger;
    if(!requestLoggerHeader(LogFile, globalLogHeader, logger.header)) {
        return false;
    }

    if(logger.header.TxCOUNT == 0) {
        std::cout << "No logs to sync\n";
        return true;
    }

    uint32_t checkPointOffs = logger.header.LatestCheckpointOffset;

    if(checkPointOffs == 0) checkPointOffs += sizeof(LoggerHeader);

    /*
    LogCMD statusComand;
    if(!load_logger_state(LogFile, checkPointOffs, statusComand)){
        return false;
    }
    assert(statusComand == LogCMD::START);
    */

    //checkPointOffs += sizeof(LogCMD);
    
    assert(logger.header.TxCOUNT > 0);

    uint32_t TxId = logger.header.TxCOUNT - 1;

    if(!load_transaction_header(LogFile, checkPointOffs, TxId, logger.transaction)) {
        return false;
    }

    if(logger.transaction.header.commited) {

        checkPointOffs += sizeof(logger.transaction.header);

        if(!load_transaction_logs(LogFile, checkPointOffs, TxId, logger.transaction)) {
            return false;
        }

        assert(logger.transaction.buffer.size() > 0);
        std::cout << "buff size: " << logger.transaction.buffer.size() << "\n";

        if(!REDO(database, manager, logger.transaction.header.NumLogs, logger.transaction.buffer)) {
            return false;
        };
    }
    else {
        if(!UNDO(database)) {
            return false;
        };
    }

    get_logger_checkpoint(LogFile, logger.header);
    //std::cout << "syncing checkpoint: " << logger.header.LatestCheckpointOffset << "\n";

    if(!flush_logger_header(LogFile, logger.header)) {
        return false;
    }

    return true;
}

//===================================
// Scan Section
//===================================

struct Set {
    size_t column_index = 0;
    std::variant<int32_t, std::string, double> value;
};

bool ScanAllRows(std::vector<ScanResult>& results, const Page& page) {

    auto cursor = page.buffer.begin();

    for (int i = 0; i < page.header.NumRows; i++) {
        std::span bytes = {cursor, page.buffer.end()};

        if(bytes.size() == 0) break;
        assert(bytes.size()>0);

        std::optional<Row> rowPtr = deserializeRow(bytes);

        if(!rowPtr) {
            std::cerr << "Failed to deserializeRow a row at page: " << page.header.id <<std::endl;
            return false;
        }

        Row row = *rowPtr;

        if(row.tumpstoned) {
            int tumpstoneByteSize = sizeof(row.tumpstoned);
            int sizeOfRowByteSize = sizeof(row.sizeOfRow);
            int totalSkipSize = tumpstoneByteSize + sizeOfRowByteSize + row.sizeOfRow;
            cursor += totalSkipSize;
            continue;
        }

        size_t rowOffset  = std::distance(page.buffer.begin(), cursor);
        size_t ID         = page.header.id;

        results.push_back({ID, rowOffset, row});

        int tumpstoneByteSize = sizeof(row.tumpstoned);
        int sizeOfRowByteSize = sizeof(row.sizeOfRow);
        int totalSkipSize = tumpstoneByteSize + sizeOfRowByteSize + row.sizeOfRow;
        cursor += totalSkipSize;
    }
    return true;
}

bool ScanRowsFromPage(std::fstream& file, std::vector<ScanResult>& results, uint32_t tableID, uint32_t pageID,Logger& logger, Pager& pager) {
    assert(file.is_open());
    Page* page = requestPage(file, pager, {tableID, pageID}, &logger);
    if(!page) {
        std::cerr << "Page not found\n";
        return false;
    }
    if(!ScanAllRows(results, *page)) {
        return false;
    }
    return true;
}

bool ScanPage(std::vector<ScanResult>& results, const Page& page, QueryParams params) {
 
    auto cursor = page.buffer.begin();

    for (int i = 0; i < page.header.NumRows; i++) {
        std::span bytes = {cursor, page.buffer.end()};

        if(bytes.size() == 0) break;
        assert(bytes.size()>0);

        std::optional<Row> rowPtr = deserializeRow(bytes);

        if(!rowPtr) {
            std::cerr << "Failed to deserializeRow a row at page: " << page.header.id <<std::endl;
            return false;
        }

        Row row = *rowPtr;

        if(row.tumpstoned) {
            int tumpstoneByteSize = sizeof(row.tumpstoned);
            int sizeOfRowByteSize = sizeof(row.sizeOfRow);
            int totalSkipSize = tumpstoneByteSize + sizeOfRowByteSize + row.sizeOfRow;
            cursor += totalSkipSize;
            continue;
        }

        size_t rowOffset  = std::distance(page.buffer.begin(), cursor);
        size_t ID         = page.header.id;

        std::visit([&params, &results, &row, rowOffset, ID](const auto& x) {
            using T = std::decay_t<decltype(x)>;
              
            if constexpr (std::is_same_v<T, int32_t>) {
                if(compare(x, params.conditional, std::get<int32_t>(params.value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                if(compare(x, params.conditional, std::get<std::string>(params.value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
            else if constexpr (std::is_same_v<T, double>) {
                if(compare(x, params.conditional, std::get<double>(params.value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
        }, row.values[params.column_index].value);

        int tumpstoneByteSize = sizeof(row.tumpstoned);
        int sizeOfRowByteSize = sizeof(row.sizeOfRow);
        int totalSkipSize = tumpstoneByteSize + sizeOfRowByteSize + row.sizeOfRow;
        cursor += totalSkipSize;
    }
    return true;
}
std::optional<std::vector<ScanResult>>
ScanTable(std::fstream& file, uint32_t tableID, Logger& logger, Pager& pager, QueryParams* params = nullptr) {
    
    std::vector<ScanResult> results;

    if(pager.tableMetadata[tableID].PAGECOUNT <= 0) { std::cout << "Table has no pages\n";
        return results;
    } 

    for (uint32_t id = 0; id < pager.tableMetadata[tableID].PAGECOUNT; id++) {
        
        Page* page = requestPage(file, pager, {tableID, id}, &logger);

        std::vector<ScanResult> result;       
        if(!params) {
            if(!ScanAllRows(result, *page)) {
                std::cerr << "Failed to scan table\n";
                return std::nullopt;
            }
        }
        else {
            if(!ScanPage(result, *page, *params)) {
                std::cerr << "Failed to scan table\n";
                return std::nullopt;
            }
        }
        results.insert(results.end(), result.begin(), result.end());
    }

    return results;

}



bool ScanUniqueness(std::fstream& file, uint32_t tableID, Pager& pager, size_t column_index, const std::variant<int32_t, std::string, double> value) {
    assert(file.is_open());
    if(pager.tableMetadata[tableID].PAGECOUNT <= 0) {
        return true;
    } 
    for (uint32_t id = 0; id < pager.tableMetadata[tableID].PAGECOUNT; id++) {
        
        Page* page = requestPage(file, pager, {tableID, id});
        if(!page) {
            std::cerr << "Failed to load page\n";
            return false;
        }

        std::vector<ScanResult> result;       
        QueryParams params = {column_index, Conditional::EQUAL, value};
        if(!ScanPage(result, *page, params)) {
            std::cerr << "Failed to scan table\n";
            return false;
        }
        if(result.size() > 0) {
            return false;
        }
    }
    return true;
}

//===================================
// Index Section
//===================================

Node CreateNode(PageKey id, Entry entry, uint32_t offset) {
    assert(offset != 0);
    assert(offset > PAGE_SIZE);
 
    return {id, entry, offset};
}

std::vector<char> serializeNode(Node& node) {
    std::vector<char> v;
    return v;

}
std::vector<char> serializeBranch(Branch& branch) {
    std::vector<char> v;
    return v;
  
}

std::optional<Node>
deserializeNode(std::span<const char> nodeBytes) {
    return std::nullopt;

}

std::optional<Node>
deserializeBranch(std::span<const char> branchBytes) {
    return std::nullopt;

}

bool InsertIntoTree(Tree& tree, Node& node) {
    return true;

}

bool IndexTable(Tree& tree, Pager& pager, uint32_t tableID, uint32_t column_index) {
 
    if(pager.tableMetadata[tableID].PAGECOUNT <= 0) {
        std::cout << "Table has no pages\n";
        return true;
    } 
    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////

    for (uint32_t pageid = 0; pageid < pager.tableMetadata[tableID].PAGECOUNT; pageid++) {
 
        Page* page = requestPage(file, pager, {tableID, pageid});

        std::vector<ScanResult> results;
        if(!ScanAllRows(results, *page)) {
            std::cerr << "Failed to scan table\n";
            return false;
        }
 
        std::vector<Node> nodes;
        for(auto result : results) {

            PageKey ID      = {tableID, pageid};
            Entry entry     = result.row.values[column_index];
            uint32_t offset = result.offset;

            Node node = CreateNode(ID, entry, offset);

            if(!InsertIntoTree(tree, node)) {
                std::cerr << "Failed to insert into B+ tree\n";
                return false;
            }
        }
    }
    return true;
}

bool flsuh_branch_header() {
    return true;

}

bool load_branch_header() {
    return true;

}

bool flush_branch() {

    return true;
}

bool load_branch() {

    return true;
} 

bool flush_tree_header() {

    return true;
}

bool load_tree_header() {

    return true;
}

bool validate_tree_header () {

    return true;
}

bool requestTreeHeader() {

    return true;
}

bool flush_tree() {

    return true;
}

bool load_tree() {

    return true;
}

//===================================
// Command Section
//===================================

bool START(const DataBase& database, FileManager& manager, Logger& logger, LoggerHeader& globalLogHeader, BeforeImageHeader& globalImageHeader) {

    manager.LoggerFile.close();
    manager.BeforeImageLogFile.close();
    logger.LoggerFile = nullptr;
    logger.BeforeImageLogFile = nullptr;
    logger.transaction = {};

    if(!SYNC(database, manager, globalLogHeader, globalImageHeader)) {
        return false;
    }

    logger.transaction.start = true;

    std::fstream LoggerFile(std::filesystem::path(BASE_DIRECTORY) / LOGGER_FILENAME, std::ios::binary | std::ios::in | std::ios::out);
    if(!LoggerFile) {
        LoggerFile.close();
        LoggerFile.open(std::filesystem::path(BASE_DIRECTORY) / LOGGER_FILENAME, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
        if(!flush_logger_header(LoggerFile, globalLogHeader)) {
            std::cerr << "Failed to create logger file\n";
            return false;
        }
    }

    if(!requestLoggerHeader(LoggerFile, globalLogHeader, logger.header)) {
        return false;
    }

    logger.header.TxCOUNT++;
    assert(logger.header.TxCOUNT > 0);
    logger.transaction.header.id = logger.header.TxCOUNT - 1;

    logger.transaction.start = true;

    if(logger.header.LatestCheckpointOffset == 0) logger.header.LatestCheckpointOffset += sizeof(LoggerHeader);

    get_logger_checkpoint(LoggerFile, logger.header);
    //std::cout << "starting checkpoint: " << logger.header.LatestCheckpointOffset << "\n";

    if(!flush_transaction_header(LoggerFile, logger.transaction, logger.header.LatestCheckpointOffset)) {
        return false;
    }

    logger.header.MAGIC   = globalLogHeader.MAGIC;
    logger.header.VERSION = globalLogHeader.VERSION;

    if(!flush_logger_header(LoggerFile, logger.header)) {
        return false;
    }
  
    std::filesystem::path backup_path = BASE_DIRECTORY;
    backup_path /= BACKUPFOLDER_NAME;
    fs::create_directory(fs::path(backup_path) / database.name);
    backup_path /= database.name;

    std::string dbName = database.name;
    fs::path db_path = BASE_DIRECTORY;
    db_path /= dbName;
    std::filesystem::copy(db_path, backup_path, fs::copy_options::recursive);

    manager.LoggerFile = std::move(LoggerFile);
    logger.LoggerFile = &manager.LoggerFile;

    return true;
}

bool COMMIT(DataBase& database, FileManager& manager, Logger& logger, Pager& pager,
            TB_Header& globalTBHEADER, DB_Header& globalDBHEADER, RecordHeader& globalRBHEADER) {

    assert(logger.transaction.start);
    assert(!logger.transaction.header.commited);
    
    //Simulated crash
    //return false;

    if(!COMMIT_DATABASE_DATA(database, globalTBHEADER, globalDBHEADER, globalRBHEADER)){
        return false;
    }

    if(!flush_logger_header(*logger.LoggerFile, logger.header)) {
        std::cerr << "Failed to flush logger's header\n";
        return false;
    }

    logger.transaction.header.commited = true;
    uint32_t flush_offset = logger.header.LatestCheckpointOffset;
    if(!flush_transaction(*logger.LoggerFile, logger.transaction, flush_offset)) {
        return false;
    }

    /*
    if(!flush_logger_state(logger.LoggerFile, logger, LogCMD::COMMIT)) {
        std::cerr << "Failed to flush logger's state\n";
        return false;
    }
    */
    int count = 0;
    for (auto& [key, page] : pager.pages) {
        //if (count == 2) return false;
        count++;
        if(!page.dirty) continue;
        
        std::filesystem::path filePath = std::filesystem::path(BASE_DIRECTORY);
        filePath /= std::to_string(key.tableID);
        if(!loadTableFile(manager, key.tableID, filePath)) {
            return false;
        }
        if(!flush_page(manager.files.at(key.tableID), page)) {
            std::cerr << "failed to flush page: " << page.header.id << "\n";
            return false;
        };
        mark_clean(page);
    }

    for(auto& [id, file] : manager.files) {
        flush_metadata(file, pager.tableMetadata[id]);
    }
    EmptyLogger(logger);

    std::filesystem::path backup_path = BASE_DIRECTORY;
    backup_path /= BACKUPFOLDER_NAME;
    backup_path /= database.name;

    std::filesystem::remove_all(backup_path);

    logger.transaction = {};

    return true;
}
 
bool SELECT(std::fstream& file, std::vector<Row>& resultSet, uint32_t tableID, Logger& logger, Pager& pager, QueryParams* params) {
    
    auto resultsPtr = ScanTable(file, tableID, logger, pager, params);
    if (!resultsPtr) {
        return false;
    }

    std::vector<ScanResult> results = *resultsPtr;
    resultSet.reserve(results.size());
    for (int i = 0; i < results.size(); i++) {
        resultSet.push_back(results[i].row);
    }
    return true;
}

bool DELETE(std::fstream& file, uint32_t tableID, Pager& pager, Logger& logger, QueryParams* params) {

    auto resultsPtr = ScanTable(file, tableID, logger, pager, params);
    if(!resultsPtr) {
        return false;
    }

    std::vector<ScanResult> results = *resultsPtr;
    for (auto& result : results) {
        uint32_t ID = result.pageId;
        Page* page = requestPage(file, pager, {tableID, ID}, &logger);
        if (!page) {
            return false;
        }

        result.row.tumpstoned = 1;

        const LogCMD command = LogCMD::DELETE;
        Log log = CreateLog({tableID, page->header.id}, command, page->header, result.offset, result.row);
        size_t beforeSize = logger.transaction.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], page, log);
        AppendLog(logger, log);
        assert(logger.transaction.buffer.size() == (beforeSize + log.size()));
        logger.transaction.header.NumLogs++;

        insertRowIntoBuff(page->buffer, result.row, result.offset);
        mark_dirty(*page);
    }

    return true;
}

bool UPDATE(std::fstream& file, uint32_t tableID, Pager& pager, Logger& logger, std::vector<Set> sets, QueryParams* params) {

  
    auto resultsPtr = ScanTable(file, tableID, logger, pager, params);
    if(!resultsPtr) {
        return false;
    }
    std::vector<ScanResult> results = *resultsPtr;

    for (auto& result : results) {
        uint32_t ID = result.pageId;
        Page* page = requestPage(file, pager, {tableID, ID}, &logger);
        if (!page) {
            return false;
        }
        result.row.tumpstoned = 1;

        const LogCMD DeleteCommand = LogCMD::DELETE;
        Log DeleteLog = CreateLog({tableID, page->header.id}, DeleteCommand,  page->header, result.offset, result.row);
        size_t beforeSize = logger.transaction.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], page, DeleteLog);
        AppendLog(logger, DeleteLog);
        assert(logger.transaction.buffer.size() == (beforeSize + DeleteLog.size()));
        logger.transaction.header.NumLogs++;

        insertRowIntoBuff(page->buffer, result.row, result.offset);
        mark_dirty(*page);
        result.row.tumpstoned = 0;
        
        Row& UpdatedRow = result.row;
        for (auto set : sets) {
            UpdatedRow.values[set.column_index].value = set.value;
        }
        UpdatedRow.RecalculateSize();

        Page* PageWithSpace = requestPageWithSpace(file, logger, pager, tableID, UpdatedRow);
        if(!PageWithSpace) {
            return false;
        }

        size_t insert_position = PageWithSpace->header.freespace;

        PageWithSpace->header.freespace += UpdatedRow.size();
        PageWithSpace->header.NumRows++;

        const LogCMD InsertCommand = LogCMD::INSERT;
        Log InsertLog = CreateLog({tableID, PageWithSpace->header.id}, InsertCommand, 
                                   PageWithSpace->header, insert_position, UpdatedRow);
        beforeSize = logger.transaction.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], PageWithSpace, InsertLog);
        AppendLog(logger, InsertLog);
        assert(logger.transaction.buffer.size() == (beforeSize + InsertLog.size()));
        logger.transaction.header.NumLogs++;

        insertRowIntoBuff(PageWithSpace->buffer, UpdatedRow, insert_position);
        mark_dirty(*PageWithSpace);
    }
    
    return true;
}

bool INSERT(std::fstream& file, uint32_t tableID, Pager& pager, Logger& logger, Row& row) {
 
    Page* page = requestPageWithSpace(file, logger, pager, tableID, row);
    if (!page) {
        return false;
    }

    size_t insert_position = page->header.freespace;

    page->header.freespace += row.size();
    page->header.NumRows += 1;

    const LogCMD command = LogCMD::INSERT;
    Log log = CreateLog({tableID, page->header.id}, command, page->header, insert_position, row);
    size_t beforeSize = logger.transaction.buffer.size();
    UpdateLSN(logger, pager.tableMetadata[tableID], page, log);
    AppendLog(logger, log);
    assert(logger.transaction.buffer.size() == (beforeSize + log.size()));
    logger.transaction.header.NumLogs++;

    insertRowIntoBuff(page->buffer, row, insert_position);

    mark_dirty(*page);
 
    return true;
}

bool CREATE_INDEX() {
    return true;

}

/*
int testReco() {

    RecordHeader globalRBHeader{0x44415441, 5};
    LoggerHeader globalLogHeader{0x44518449, 4};
    BeforeImageHeader globalImageHeader{0x75314648, 1};
    globalLogHeader.LatestLSN = 7;
    globalRBHeader.TABLEID = 1;
    std::fstream file2("logger.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_logger_header(file2, globalLogHeader)) {
        std::cout << "Failed to flush loggers header at beggining of file\n";
        return 1;
    }
    file2.close();
    std::fstream file1("data.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_metadata(file1, globalRBHeader)) {
        std::cout << "Failed to flush RB header at beggining of file\n";
        return 1;
    }
 
    file1.close();

    if(!SYNC(globalLogHeader, globalImageHeader)) {
        std::cerr << "Failed to SYNC DB\n";
        return 1;
    }

    RecordHeader RH;
    std::fstream file4("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    if (!requestRecordBankHeader(file4, globalRBHeader, RH, 1)) {
        std::cout << "Failed to load RecordHeader\n";
        return 1;
    }
    Pager pager;
    Logger logger;
    pager.tableMetadata[1] = RH;
    file4.close();

    if(!START(logger, globalLogHeader, globalImageHeader)){
        std::cout << "Failed to start Transaction\n";
        return 1;
    };

    std::ifstream rowFile("rows3.txt");
    if(!rowFile) {
        std::cout << "File not found\n";
        return 1;
    }
    int count = 0;
    int size = 0;
    while(true) {
        //std::cout << "------------------------\n";
        //std::cout << "iteration: " << ++count << "\n";
        Row lrow; 
        //std::cout << "its this function\n";
        if(!loadRow(rowFile, lrow, {{DataType::INTEIRO, DataType::TEXTO, DataType::REAL}})){
            std::cout << "end of file\n";
            break;
        };
        //std::cout << "never mind\n";
        size += lrow.size();

        //printRow(lrow);
        //std::cout << "Inserting: \n"; 
        if (!INSERT(1, pager, logger, lrow)) {
            std::cout << "Inserion Failed\n";
            return 1;
        }
        //std::cout << "Buff size: " << size << "/" << PAGE_SIZE << "\n";
        
        //std::cout << "Loaded pages: " << pager.pages.size() << "\n";

    }
    if (!UPDATE(1, pager, logger, {{0, 69}, {1, "femboyYohan"}}, 1, Conditional::EQUAL, "Yohan")) {
        std::cout << "Update Failed\n";
        return 1;
    }
    if (!UPDATE(1, pager, logger, {{0, 67}, {1, "Foxu"}}, 1, Conditional::EQUAL, "Kirsche")) {
        std::cout << "Update Failed\n";
        return 1;
    }
    if (!DELETE(1, pager, logger, 1, Conditional::EQUAL, "E;R")) {
        std::cout << "DELETE Failed\n";
        return 1;
    }
    if (!UPDATE(1, pager, logger, {{0, 67}, {1, "Foxu"}}, 1, Conditional::EQUAL, "Kirsche")) {
        std::cout << "Update Failed\n";
        return 1;
    }

    std::ofstream rowFileout("rows4.txt");
    if(!rowFileout) {
        std::cout << "File not found\n";
        return 1;
    }
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    int numpage = pager.tableMetadata[1].PAGECOUNT;
    for(uint32_t i = 0; i < numpage; i++) {
        Page* page = requestPage(file, pager, {1, i});
        std::vector<ScanResult> results;
        if(!ScanAllRows(results, *page)) {
            std::cout << "Failed to scan all rows\n";
            return false;
        }

        for(auto& re : results) {
            if(!saveRow(rowFileout, re.row, {{DataType::INTEIRO, DataType::TEXTO, DataType::REAL}})){
                std::cout << "end of file\n";
                break;
            }
        }
    }

    COMMIT(file, logger, pager);
    file.close();

    std::cout << "Compiles!\n";
    return 0;
}
*/
//////////////////////////////////////
/* TODO:
 * 1. Inegrate and TEST requestPageWithSpace() func into INSERT func
 * 2. Make and test Iviction policies in PAGER
 * 3. Make auto incriment on insertions for cols with Primary Key
 * 4. Make SURE that if the row has a Primary Key that UPDATE will not auto incriment when inserting
 */
/*
int main() {

    RecordHeader globalRBHeader{0x44415441, 5};
    LoggerHeader globalLogHeader{0x44518449, 4};
    BeforeImageHeader globalImageHeader{0x75314648, 1};
    //globalLogHeader.LatestLSN = 7;
    globalRBHeader.TABLEID = 1;
    std::fstream file2("logger.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_logger_header(file2, globalLogHeader)) {
        std::cout << "Failed to flush loggers header at beggining of file\n";
        return 1;
    }
    file2.close();
    std::fstream file1("data.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_metadata(file1, globalRBHeader)) {
        std::cout << "Failed to flush RB header at beggining of file\n";
        return 1;
    }
 
    file1.close();


    RecordHeader RH;
    std::fstream file4("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    if (!requestRecordBankHeader(file4, globalRBHeader, RH, 1)) {
        std::cout << "Failed to load RecordHeader\n";
        return 1;
    }
    Pager pager;
    Logger logger;
    pager.tableMetadata[1] = RH;

    std::cout << "Starting transaction 1 \n";
    if(!SYNC(globalLogHeader, globalImageHeader)) {
        std::cerr << "Failed to SYNC DB\n";
        return 1;
    }
    if(!START(logger, globalLogHeader, globalImageHeader)){
        std::cout << "Failed to start Transaction\n";
        return 1;
    };
    std::ifstream rowFile1("db_test_1.txt");
    if(!rowFile1) {
        std::cout << "File not found\n";
        return 1;
    }
    while(true) {
        Row lrow; 
        if(!loadRow(rowFile1, lrow, {{DataType::INTEIRO, DataType::TEXTO, DataType::REAL}})){
            std::cout << "end of file\n";
            break;
        };

        if (!INSERT(1, pager, logger, lrow)) {
            std::cout << "Inserion Failed\n";
            return 1;
        }
    }
    COMMIT(file4, logger, pager);

    std::cout << "transaction 1 Commited\n";




    std::cout << "Starting transaction 2 \n";
    if(!SYNC(globalLogHeader, globalImageHeader)) {
        std::cerr << "Failed to SYNC DB\n";
        return 1;
    }
    if(!START(logger, globalLogHeader, globalImageHeader)){
        std::cout << "Failed to start Transaction\n";
        return 1;
    };
    std::ifstream rowFile2("db_test_2.txt");
    if(!rowFile2) {
        std::cout << "File not found\n";
        return 1;
    }
    while(true) {
        Row lrow; 
        if(!loadRow(rowFile2, lrow, {{DataType::INTEIRO, DataType::TEXTO, DataType::REAL}})){
            std::cout << "end of file\n";
            break;
        };

        if (!INSERT(1, pager, logger, lrow)) {
            std::cout << "Inserion Failed\n";
            return 1;
        }
    }
    COMMIT(file4, logger, pager);

    std::cout << "transaction 2 Commited\n";






    std::cout << "Starting transaction 3 \n";
    if(!SYNC(globalLogHeader, globalImageHeader)) {
        std::cerr << "Failed to SYNC DB\n";
        return 1;
    }
    if(!START(logger, globalLogHeader, globalImageHeader)){
        std::cout << "Failed to start Transaction\n";
        return 1;
    };
    std::ifstream rowFile3("db_test_3.txt");
    if(!rowFile3) {
        std::cout << "File not found\n";
        return 1;
    }
    while(true) {
        Row lrow; 
        if(!loadRow(rowFile3, lrow, {{DataType::INTEIRO, DataType::TEXTO, DataType::REAL}})){
            std::cout << "end of file\n";
            break;
        };

        if (!INSERT(1, pager, logger, lrow)) {
            std::cout << "Inserion Failed\n";
            return 1;
        }
    }
    COMMIT(file4, logger, pager);

    std::cout << "transaction 3 Commited\n";

    std::vector<Row> result;
    if(!SELECT(result, 1, logger, pager, 0, Conditional::EQUAL, 69)){
        std::cout << "SELECT failed\n";
    }
    if(!SELECT(result, 1, logger, pager, 1, Conditional::EQUAL, "Kirsche")){
        std::cout << "SELECT failed\n";
    }
    if(!SELECT(result, 1, logger, pager, 2, Conditional::EQUAL, 6.7)){
        std::cout << "SELECT failed\n";
    }
    for(auto row : result) {
        printRow(row);
    }

    std::cout << "Compiles!\n";
}
*/
