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

const int KILOBYTE = 1024;
constexpr int PAGE_SIZE = KILOBYTE; 
const int MAXPAGES = 100;


enum class DataType : uint32_t {
    INTEIRO = 1, //int
    TEXTO = 2, //string
    REAL = 3 //double
};

enum class Conditional {
    EQUAL,
    GREATER,
    LESSER,
    GREATERorEQUAL,
    LESSERorEQUAL
};


struct Entry {
    DataType type;
    std::variant<int32_t, std::string, double> value;
};
struct Row { 
    bool tumpstoned = false;
    uint32_t sizeOfRow = 0;
    std::vector<Entry> values = {};
    
    void add_entry(DataType t, std::variant<int32_t, std::string, double> v) {
        if      (t == DataType::INTEIRO) sizeOfRow += sizeof(int32_t);
        else if (t == DataType::TEXTO)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(v).size());
        else if (t == DataType::REAL)    sizeOfRow += sizeof(double);

        sizeOfRow += sizeof(uint32_t);
        values.push_back({t, v});
    };
    void add_entry(Entry& entry) {
        if      (entry.type == DataType::INTEIRO) sizeOfRow += sizeof(int32_t);
        else if (entry.type == DataType::TEXTO)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(entry.value).size());
        else if (entry.type == DataType::REAL)    sizeOfRow += sizeof(double);

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
            if      (entry.type == DataType::INTEIRO) sizeOfRow += sizeof(int32_t);
            else if (entry.type == DataType::TEXTO)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(entry.value).size());
            else if (entry.type == DataType::REAL)    sizeOfRow += sizeof(double);
            sizeOfRow += sizeof(uint32_t);
        }

    }

};

void printRow(Row& row);

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t TABLEID   = 0;
    uint32_t PAGECOUNT = 0;
    uint32_t LatestLSN = 0;
};

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

enum class LogCMD : int32_t {
    START      = 1,
    INSERT     = 2,
    DELETE     = 3,
    COMMIT     = 4,
    CHECKPOINT = 5,
    NULLROW    = 6,
    ENDOFLOG   = 7
};

struct Log {
    PageKey ID;
    uint32_t LSN = 0;
    LogCMD command;
    PageHeader pageHeader;
    uint32_t offset = 0;
    Row row;
    const LogCMD endOfLog = LogCMD::ENDOFLOG;

    size_t size() {
        return sizeof(PageKey) + sizeof(LSN) + sizeof(command) + sizeof(PageHeader) + sizeof(offset) + row.size() + sizeof(endOfLog);
    }
};

struct LoggerHeader {
    uint32_t MAGIC                  = 0;
    uint32_t VERSION                = 0;
    uint32_t TxCOUNT                = 0;
    uint32_t LatestCheckpointOffset = 0;
    uint32_t LatestLSN              = 0;
};

struct BeforeImageHeader {
    uint32_t MAGIC           = 0;
    uint32_t VERSION         = 0;
    uint32_t PAGECOUNT       = 0;
    const uint32_t PADDING   = 0;
};

struct BeforeImage {
    Page page;
};

struct TransactionHeader {
    uint32_t id = 0;
    uint32_t NumLogs = 0;
    uint32_t TxSIZE = 0;
    bool commited = false;
};

struct Logger {
    
    LoggerHeader header;
    BeforeImageHeader beforeImageHeader;
    TransactionHeader Txheader;

    bool start = false;

    std::unordered_set<PageKey, PageKeyHash> flushed_beforeImages;
    std::fstream LoggerFile;
    std::fstream BeforeImageLogFile;

    std::vector<char> buffer;
};

void mark_clean(Page& page);
void mark_dirty(Page& page);
bool flush_page(std::fstream& file, const Page& page);
bool flush_metadata(std::fstream& file, RecordHeader header);
std::vector<char> serializeRow(Row& row);

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
        if(index + sizeof(T) > buff.size()) return std::nullopt; 
        T value;
        std::memcpy(&value, buff.data() + index, sizeof(T));
        index += sizeof(T);
        return value;
    }
}

Page create_page(uint32_t id);
void insertRowIntoBuff(std::vector<char>& buff, Row& row, size_t offset);
void eraseRowFromBuff(std::vector<char>& buff, Row& row, size_t offset);
std::optional<Row> deserializeRow(std::span<const char> rowBytes);
uint32_t count_pages(std::fstream& file);
bool load_RecordBank_header(std::fstream& file, RecordHeader& header, uint32_t tableID);
bool flush_logger(std::fstream& file, Logger& logger);
bool flush_logger_header(std::fstream& file, LoggerHeader& header);
bool flush_beforeImage_header(std::fstream& file, BeforeImageHeader& header);
bool flush_beforeImage(std::fstream& file, BeforeImage& before);
std::optional<Page> load_page(std::fstream& file, const int id);
bool AppendBeforeImage(Logger& logger, BeforeImage& before);


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
            assert(logger->LoggerFile.is_open());
            if(!flush_logger_header(logger->LoggerFile, logger->header)) {
                std::cerr << "Failed to flush logger's header when evicting page\n";
                return false;
            }
            std::cout << "flushing logger: " << key.pageID << " When evicting\n";
            if(!flush_logger(logger->LoggerFile, *logger)) {
                std::cerr << "Failed to flush logger when evicting page\n";
                return false;
            }
        }
        std::cout << "flushing page: " << key.pageID << " When evicting\n";
        if(!flush_page(file, *page)) {
            std::cerr << "Failed to flush page when evicting it\n";
            return false;
        }
    }
    
    pager.PFIterators.erase(key);
    pager.PageFrequency.pop_back();
    pager.pages.erase(key);
    std::cout << "Evicting: " << key.pageID << "\n";
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

Page* find_free_page(Pager& T, size_t rowSize) {
    
    Page* page = nullptr;

    for (auto& loadedPage : T.pages) {
        if(!will_fit(loadedPage.second, rowSize)) {
            continue;
        }
        else {
            page = &loadedPage.second;
            return page;
        }
    }

    return page;
}

Page* requestPage(std::fstream& file, Pager& pager, PageKey ID, Logger* logger = nullptr) {
    Page* page = nullptr;
    auto it = pager.pages.find(ID);
    if (it == pager.pages.end()) {
        auto loadedPage = load_page(file, ID.pageID);
        if(!loadedPage) {
            return page;
        }
        std::cout << "Page: " << ID.pageID << " Loaded\n";
        if(pager.pages.size() == MAXPAGES) {
            evictPage(file, pager, logger);
        }

        pager.pages.insert({ID, *loadedPage});

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
    }
    else std::cout << "Cache hit on page: " << ID.pageID << "\n";
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

    LogCMD endOfLog = log.endOfLog;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&endOfLog), reinterpret_cast<const char*>(&endOfLog) + sizeof(endOfLog));

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

    auto endOfLog = read_bytes<LogCMD>(logBytes, index);
    if(!endOfLog) {
        std::cerr << "Failed to deserialize END OF LOG when deserializing log\n";
        return std::nullopt;
    }
    
    if(*endOfLog != LogCMD::ENDOFLOG) {
        std::cerr << "Log does not contain END OF LOG byte\n";
        return std::nullopt;
    }

    log.ID = *ID;
    log.LSN = *LSN;
    log.command = *command;
    log.pageHeader = *PHeader;
    log.offset = *offset;
    log.row = *rowptr;

    return log;
}

void UpdateLSN(Logger& logger, RecordHeader& header, Page* page, Log& log) {

    assert(logger.header.LatestLSN == header.LatestLSN);
    assert(page!=nullptr);

    logger.header.LatestLSN++;
    header.LatestLSN++;

    uint32_t this_lsn = logger.header.LatestLSN;

    log.LSN   = this_lsn;
    page->header.LSN = this_lsn;

}
void AppendLog(Logger& logger, Log& log) {
    assert(logger.start && !logger.Txheader.commited);

    std::vector<char> bytes = serializeLog(log);
    logger.buffer.insert(logger.buffer.end(), bytes.data(), bytes.data() + bytes.size());

    logger.Txheader.TxSIZE += log.size();
}
bool AppendBeforeImage(Logger& logger, BeforeImage& before) {

    if(!flush_beforeImage(logger.BeforeImageLogFile, before)) {
        return false;
    }

    logger.beforeImageHeader.PAGECOUNT++;
    if(!flush_beforeImage_header(logger.BeforeImageLogFile, logger.beforeImageHeader)) {
        return false;
    }

    return true;
}


bool flush_logger_state(std::fstream& file, Logger& logger, LogCMD state) {

    assert(state == LogCMD::START || state == LogCMD::COMMIT);

    switch(state) {
        case LogCMD::START:
            assert(logger.start && !logger.Txheader.commited);
            break;
        case LogCMD::COMMIT:
            assert(logger.start && logger.Txheader.commited);
            break;
    }
    
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

    const LogCMD checkPoint = LogCMD::CHECKPOINT;
    file.seekp(0, std::ios::end);
    
    file.write(reinterpret_cast<const char*>(&checkPoint), sizeof(LogCMD));
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

bool flush_logger(std::fstream& file, Logger& logger) {
    assert(logger.start);
    
    uint32_t flush_offset = logger.header.LatestCheckpointOffset + sizeof(LogCMD);
    file.seekp(flush_offset, std::ios::beg);
    file.write(reinterpret_cast<const char*>(&logger.Txheader.id), sizeof(logger.Txheader.id));
    file.write(reinterpret_cast<const char*>(&logger.Txheader.NumLogs), sizeof(logger.Txheader.NumLogs));
    file.write(reinterpret_cast<const char*>(&logger.Txheader.TxSIZE), sizeof(logger.Txheader.TxSIZE));

    uint8_t commited = logger.Txheader.commited ? 1 : 0;
    file.write(reinterpret_cast<const char*>(&commited), sizeof(commited));
    if(!file) {
        std::cout << "Failed to flush logger Transaction Header\n";
        return false;
    }

    int32_t buffSize = logger.buffer.size();
    file.write(reinterpret_cast<const char*>(&buffSize), sizeof(buffSize));
    file.write(logger.buffer.data(), buffSize);
    if(!file) {
        std::cout << "Failed to flush logger Log buffer\n";
        return false;
    }
    file.seekp(0, std::ios::beg);
    return true;
}

bool flush_TxHeader_commited(std::fstream& file, Logger& logger, uint32_t offset) {
    assert(logger.Txheader.commited);
    assert(logger.start);

    file.seekp(offset, std::ios::end);

    file.seekp(0, std::ios::end);
    file.write(reinterpret_cast<const char*>(&logger.Txheader.id), sizeof(logger.Txheader.id));
    file.write(reinterpret_cast<const char*>(&logger.Txheader.NumLogs), sizeof(logger.Txheader.NumLogs));
    file.write(reinterpret_cast<const char*>(&logger.Txheader.TxSIZE), sizeof(logger.Txheader.TxSIZE));

    uint8_t commited = 1;
    file.write(reinterpret_cast<const char*>(&commited), sizeof(commited));
    if(!file) {
        std::cout << "Failed to flush logger Transaction Header\n";
        return false;
    }

    file.seekp(0, std::ios::beg);
    return true;
}

bool load_logger(std::fstream& file, uint32_t checkPointOffs, uint32_t TxId ,Logger& logger) {
    
    file.seekg(checkPointOffs, std::ios::beg);
    file.read(reinterpret_cast<char*>(&logger.Txheader.id), sizeof(logger.Txheader.id));
    file.read(reinterpret_cast<char*>(&logger.Txheader.NumLogs), sizeof(logger.Txheader.NumLogs));
    file.read(reinterpret_cast<char*>(&logger.Txheader.TxSIZE), sizeof(logger.Txheader.TxSIZE));

    uint8_t commited = 0;
    file.read(reinterpret_cast<char*>(&commited), sizeof(commited));
    logger.Txheader.commited = (commited != 0);

    if(!file) {
        std::cout << "Failed to load logger Transaction Header\n";
        return false;
    }
    if(logger.Txheader.id != TxId) {
        std::cout << "Transaction: " << TxId << "Not found\n";
        return false;
    }

    int32_t buffSize = 0;
    file.read(reinterpret_cast<char*>(&buffSize), sizeof(buffSize));
    logger.buffer.resize(buffSize);
    file.read(logger.buffer.data(), buffSize);

    if(!file) {
        std::cout << "Failed to load logger Log buffer\n";
        return false;
    }
    file.seekg(0, std::ios::beg);
    return true;
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

std::optional<BeforeImage>
load_beforeImage(std::fstream& file, const uint32_t PageIndex) {
 
    BeforeImage before;

    size_t pageOffset = sizeof(BeforeImageHeader) + PageIndex * ( sizeof(before.page.header) + PAGE_SIZE);

    std::cout << "pageOffset: " << pageOffset << "\n";

    file.clear();
    file.seekg(pageOffset);

    file.read(reinterpret_cast<char*>(&before.page.header), sizeof(before.page.header));
    std::cout << "Before Image header: " << before.page.header.id << ", "
                                         << before.page.header.freespace << ", "
                                         << before.page.header.NumRows << "\n";

    if(!file) {
        std::cerr << "Failed to load page header from BeforeImage file" << std::endl;
        return std::nullopt;
    }

    const int DataBytesOffs = pageOffset + sizeof(before.page.header);

    std::cout << "DataBytesOffs: " << DataBytesOffs << "\n";
    file.clear();
    file.seekg(DataBytesOffs, std::ios::beg);
    std::cout << "after seek Offs: " << file.tellg() << "\n";

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

bool REDO(std::fstream& file, uint32_t NumLogs, const std::vector<char>& buff) {
    std::cout << "REDO\n";

    using LSNCount = uint32_t;
    using tableID  = uint32_t;
    Pager pager;

    std::unordered_map<tableID, LSNCount> tableIDs;
    auto cursor = buff.begin();

    for(size_t i = 0; i < NumLogs; i++) {
        std::span bytes = {cursor, buff.end()};
 
        auto logPtr = deserializeLog(bytes);
        if(!logPtr) {
            std::cerr << "Failed to deserialize Log when REDO ing\n";
            return 1;
        }
        cursor += logPtr->size();
        Log log = *logPtr;

        Page* page = requestPage(file, pager, log.ID);
        if(!page) {
            std::cerr << "Repairing page\n";
            Page newPage = create_page(log.ID.pageID);
            pager.pages.insert({log.ID, newPage});
            page = &pager.pages.at(log.ID);
        }

        std::cout << "log LSN: " << log.LSN << " page LSN: " << page->header.LSN << "\n";
        if (log.LSN <= page->header.LSN) {
            std::cout << "Ignoring page\n";
            tableIDs[log.ID.tableID] += 1;
            continue;
        }
        else {
            std::cout << "Redoing changes to page page\n";
        }

        page->header = log.pageHeader;
        insertRowIntoBuff(page->buffer, log.row, log.offset);
        page->header.LSN = log.LSN;
        tableIDs[log.ID.tableID] += 1;
    }
    for (auto& [key, page] : pager.pages) {
        if(!flush_page(file, page)) {
            std::cerr << "failed to flush page: " << page.header.id << "\n";
            return false;
        };
    }

    //Repair Files metadata
    for (const auto& [id, lsnCount] : tableIDs) {
        RecordHeader this_header;
        if(!load_RecordBank_header(file, this_header, id)) {
            std::cout << "Failed to load metadata while Repairing header\n";
            return false;
        };
        //temporary
        this_header.PAGECOUNT = count_pages(file);

        this_header.LatestLSN += lsnCount;
        if(!flush_metadata(file, this_header)) {
            std::cout << "Failed to flush repaired metadata while Repairing header\n";
            return false;
        }
    }

    return true;
}

bool UNDO(std::fstream& Imagefile, std::fstream& DataFile, uint32_t ImageCount, uint32_t RHPAGECOUNT) {
    std::cout << "UNDO\n";
    std::vector<Page> befores;
 
    for(size_t index = 0; index < ImageCount; index++) {

        auto beforeptr = load_beforeImage(Imagefile, index);
        if (!beforeptr) {
            return false;
        }
        BeforeImage before = *beforeptr;
        befores.push_back(before.page);

    }
    uint32_t RepairedSize = sizeof(RecordHeader) + RHPAGECOUNT * (sizeof(PageHeader) + PAGE_SIZE);
    //Temporary
    std::filesystem::resize_file("data.bin", RepairedSize);

    for (auto& image : befores) {

        if(!flush_page(DataFile, image)) {
            std::cerr << "failed to flush page: " << image.header.id << "\n";
            return false;
        };
    }
    return true;
}

bool SYNC() {
    //!!!!!!Temporary!!!!!!
    std::fstream DataFile("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    std::fstream LogFile("logger.bin", std::ios::binary | std::ios::out | std::ios::in);
    std::fstream BeforeImageLogFile("beforeImage.bin", std::ios::binary | std::ios::in | std::ios::out);
    ///////////////////////
    if (!DataFile) {
        std::cerr << "Failed to open data.bin\n";
        return false;
    }
    if (!LogFile) {
        std::cerr << "Failed to open logger.bin\n";
        return false;
    }
    if (!BeforeImageLogFile) {
        std::cerr << "Failed to open beforeImage.bin\n";
        return false;
    }



    Logger logger;
    if(!load_logger_header(LogFile, logger.header)) {
        return false;
    }

    if(logger.header.TxCOUNT == 0) {
        std::cout << "No logs to sync\n";
        return true;
    }

    uint32_t checkPointOffs = logger.header.LatestCheckpointOffset;

    if(checkPointOffs == 0) checkPointOffs += sizeof(LoggerHeader);

    LogCMD statusComand;
    if(!load_logger_state(LogFile, checkPointOffs, statusComand)){
        return false;
    }
    assert(statusComand == LogCMD::START);

    checkPointOffs += sizeof(LogCMD);
    
    assert(logger.header.TxCOUNT > 0);

    uint32_t TxId = logger.header.TxCOUNT - 1;

    if(!load_logger(LogFile, checkPointOffs, TxId, logger)) {
        return false;
    }

    if(logger.Txheader.commited) {

        if(!REDO(DataFile, logger.Txheader.NumLogs, logger.buffer)) {
            return false;
        };
    }
    else {

        BeforeImageHeader beforeHeader;
        if(!load_beforeImage_header(BeforeImageLogFile, beforeHeader)) {
            return false;
        }
        RecordHeader header;
        //the 1 is temporary
        uint32_t tempID = 1;
        if(!load_RecordBank_header(DataFile, header, tempID)) {
            return false;
        }

        if(!UNDO(BeforeImageLogFile, DataFile, beforeHeader.PAGECOUNT, header.PAGECOUNT)) {
            return false;
        };

    }


    std::cout << "logger.buffer.size(): " << logger.buffer.size() << "\n";
    if(!flush_logger_checkpoint(LogFile, logger.header)){
        return false;
    };
    if(!flush_logger_header(LogFile, logger.header)) {
        return false;
    }

    return true;
}

bool START(Logger& logger) {
    logger.start = true;

    std::fstream LoggerFile("logger.bin", std::ios::binary | std::ios::in | std::ios::out);
    std::fstream BeforeImageLogFile("beforeImage.bin", std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);

    if(!load_logger_header(LoggerFile, logger.header)) {
        return false;
    }
    logger.header.TxCOUNT++;
    assert(logger.header.TxCOUNT > 0);
    logger.Txheader.id = logger.header.TxCOUNT - 1;

    if(logger.header.LatestCheckpointOffset == 0) logger.header.LatestCheckpointOffset += sizeof(LoggerHeader);
    if(!flush_logger_state(LoggerFile, logger, LogCMD::START)) {
        return false;
    }

    /*
    if(!load_beforImage_header(BeforeImageLogFile, logger.beforeImageHeader)) {
        return false;
    }
    */

    logger.LoggerFile = std::move(LoggerFile);
    logger.BeforeImageLogFile = std::move(BeforeImageLogFile);

    return true;
}

bool COMMIT(std::fstream& file, Logger& logger, Pager& pager) {
    
    assert(logger.start);
    assert(!logger.Txheader.commited);
    
    //Simulated crash
    //return false;

    //std::cout << "logger.buffer.size(): " << logger.buffer.size() << "\n";

    if(!flush_logger_header(logger.LoggerFile, logger.header)) {
        std::cerr << "Failed to flush logger's header\n";
        return false;
    }
    logger.Txheader.commited = true;
    if(!flush_logger(logger.LoggerFile, logger)) {
        std::cerr << "Failed to flush logger's buffer\n";
        return false;
    }

    /*
    uint32_t Txheader_offset = logger.buffer.size();
    if(!flush_TxHeader_commited(logger.file, logger, Txheader_offset)) {
        std::cerr << "Failed to flush commited to TransactionHeader\n";
        return false;
    }
    */
    //return false;

    if(!flush_logger_state(logger.LoggerFile, logger, LogCMD::COMMIT)) {
        std::cerr << "Failed to flush logger's state\n";
        return false;
    }
    int count = 0;
    for (auto& [key, page] : pager.pages) {
        //if (count == 2) return false;
        count++;
        if(!page.dirty) continue;

        if(!flush_page(file, page)) {
            std::cerr << "failed to flush page: " << page.header.id << "\n";
            return false;
        };
        mark_clean(page);
    }

    //Temporary
    flush_metadata(file, pager.tableMetadata[1]);
    return true;
}

void printPageKey(PageKey p) {
    std::cout << "------------------------\n";
    std::cout << p.tableID << "\n";
    std::cout << p.pageID << "\n";
}

Page create_page(uint32_t id) {
    PageHeader newHeader = {id, 0, 0};
    Page newPage;
    newPage.header = newHeader;
    newPage.dirty = true;
    return newPage;
}

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

    std::cout << "Page: " << page.header.id << " flushed successfully\n";

    return true;
}

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
        std::cerr << "File has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != globalHeader.VERSION) {
        std::cerr << "File has invalid file VERSION" << std::endl;
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
    std::cout << "header id : " << this_header.TABLEID <<"\n";
  
    uint32_t pageCount = count_pages(file);

    if(!validate_RecordBank_header(globalHeader, this_header, pageCount)) {
        return false;
    }
    
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
          
        if(entry.type == DataType::INTEIRO) {
            const int32_t integer = std::get<int32_t>(entry.value);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&integer), reinterpret_cast<const char*>(&integer) + sizeof(integer));
        }
        else if(entry.type == DataType::TEXTO) {
            const std::string& str = std::get<std::string>(entry.value);
            const uint32_t len = static_cast<uint32_t>(str.size());
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&len), reinterpret_cast<const char*>(&len) + sizeof(len));
            bytes.insert(bytes.end(), str.begin(), str.end());
        }
        else if(entry.type == DataType::REAL) {
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
            case DataType::INTEIRO: {
                auto integer = read_bytes<int32_t>(rowBytes, index);
                if(!integer) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
                entry.value = *integer;
                break;
            }
            case DataType::REAL: {
                auto dub = read_bytes<double>(rowBytes, index);
                if(!dub) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
                entry.value = *dub;
                break;
            }
            case DataType::TEXTO: {
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


template<typename T>
bool compare(T RowValue, Conditional conditional, T value) {
    if (conditional == Conditional::EQUAL) {
        if(RowValue == value){
            return true;
        }
        else {
            return false;
        } 
    }
    if (conditional == Conditional::GREATER) {
        if(RowValue > value){
            return true;
        }
        else {
            return false;
        } 
    }
    if (conditional == Conditional::LESSER) {
        if(RowValue < value){
            return true;
        }
        else {
            return false;
        } 
    }
    if (conditional == Conditional::GREATERorEQUAL) {
        if(RowValue >= value){
            return true;
        }
        else {
            return false;
        } 
    }
    if (conditional == Conditional::LESSERorEQUAL) {
        if(RowValue <= value){
            return true;
        }
        else {
            return false;
        } 
    }
    return false;
};

struct ScanResult {
    size_t pageId = 0;
    size_t offset = 0;
    Row row;
};

bool ScanAllRows(std::vector<ScanResult>& results, Page page) {

    auto cursor = page.buffer.begin();

    for (int i = 0; i < page.header.NumRows; i++) {
        std::span bytes = {cursor, page.buffer.end()};

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
bool ScanPage(std::vector<ScanResult>& results, const Page& page, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
 
    auto cursor = page.buffer.begin();

    for (int i = 0; i < page.header.NumRows; i++) {
        std::span bytes = {cursor, page.buffer.end()};

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

        std::visit([&value, conditional, &results, &row, rowOffset, ID](const auto& x) {
            using T = std::decay_t<decltype(x)>;
              
            if constexpr (std::is_same_v<T, int32_t>) {
                if(compare(x, conditional, std::get<int32_t>(value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                if(compare(x, conditional, std::get<std::string>(value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
            else if constexpr (std::is_same_v<T, double>) {
                if(compare(x, conditional, std::get<double>(value))) {
                    results.push_back({ID, rowOffset, row});
                }
            }
        }, row.values[column_index].value);

        int tumpstoneByteSize = sizeof(row.tumpstoned);
        int sizeOfRowByteSize = sizeof(row.sizeOfRow);
        int totalSkipSize = tumpstoneByteSize + sizeOfRowByteSize + row.sizeOfRow;
        cursor += totalSkipSize;
    }
    return true;
}
std::optional<std::vector<ScanResult>>
ScanTable(uint32_t tableID, Logger& logger, Pager& pager, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
    
    std::vector<ScanResult> results;

    if(pager.tableMetadata[tableID].PAGECOUNT <= 0) {
        std::cout << "Table has no pages\n";
        return results;
    } 
    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////

    for (uint32_t id = 0; id < pager.tableMetadata[tableID].PAGECOUNT; id++) {
        
        Page* page = requestPage(file, pager, {tableID, id}, &logger);

        std::vector<ScanResult> result;       
        if(!ScanPage(result, *page, column_index, conditional, value)) {
            std::cerr << "Failed to scan table\n";
            return std::nullopt;
        }
        results.insert(results.end(), result.begin(), result.end());
    }

    return results;

}

struct QueryParams {
    
};
 
bool SELECT(std::vector<Row>& resultSet, uint32_t tableID, Logger& logger, Pager& pager, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
    auto resultsPtr = ScanTable(tableID, logger, pager, column_index, conditional, value);
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

void insertRowIntoBuff(std::vector<char>& buff, Row& row, size_t offset) {
    std::vector<char> RowBytes = serializeRow(row);
    std::memcpy(buff.data() + offset, RowBytes.data(), RowBytes.size());
}
void eraseRowFromBuff(std::vector<char>& buff, Row& row, size_t offset) {
    std::vector<char> emptyBytes;
    emptyBytes.resize(row.size());
    std::memcpy(buff.data() + offset, emptyBytes.data(), emptyBytes.size());
}

bool DELETE(uint32_t tableID, Pager& pager, Logger& logger, 
            size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {

    auto resultsPtr = ScanTable(tableID, logger, pager, column_index, conditional, value);
    if(!resultsPtr) {
        return false;
    }

    std::vector<ScanResult> results = *resultsPtr;
    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////
    for (auto& result : results) {
        uint32_t ID = result.pageId;
        Page* page = requestPage(file, pager, {tableID, ID}, &logger);
        if (!page) {
            return false;
        }

        result.row.tumpstoned = 1;

        const LogCMD command = LogCMD::DELETE;
        Log log = CreateLog({tableID, page->header.id}, command, page->header, result.offset, result.row);
        size_t beforeSize = logger.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], page, log);
        AppendLog(logger, log);
        assert(logger.buffer.size() == (beforeSize + log.size()));
        logger.Txheader.NumLogs++;

        insertRowIntoBuff(page->buffer, result.row, result.offset);
        mark_dirty(*page);
    }

    return true;
}

struct Set {
    size_t column_index = 0;
    std::variant<int32_t, std::string, double> value;
};

bool UPDATE(uint32_t tableID, Pager& pager, Logger& logger,
            std::vector<Set> sets,
            size_t column_index, const Conditional conditional, 
            const std::variant<int32_t, std::string, double> value) {

    auto resultsPtr = ScanTable(tableID, logger, pager, column_index, conditional, value);
    if(!resultsPtr) {
        return false;
    }
    std::vector<ScanResult> results = *resultsPtr;

    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////
    for (auto& result : results) {
        uint32_t ID = result.pageId;
        Page* page = requestPage(file, pager, {tableID, ID}, &logger);
        if (!page) {
            return false;
        }
        result.row.tumpstoned = 1;

        const LogCMD DeleteCommand = LogCMD::DELETE;
        Log DeleteLog = CreateLog({tableID, page->header.id}, DeleteCommand,  page->header, result.offset, result.row);
        size_t beforeSize = logger.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], page, DeleteLog);
        AppendLog(logger, DeleteLog);
        assert(logger.buffer.size() == (beforeSize + DeleteLog.size()));
        logger.Txheader.NumLogs++;

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
        beforeSize = logger.buffer.size();
        UpdateLSN(logger, pager.tableMetadata[tableID], PageWithSpace, InsertLog);
        AppendLog(logger, InsertLog);
        assert(logger.buffer.size() == (beforeSize + InsertLog.size()));
        logger.Txheader.NumLogs++;

        insertRowIntoBuff(PageWithSpace->buffer, UpdatedRow, insert_position);
        mark_dirty(*PageWithSpace);
    }
    
    return true;
}

bool INSERT(uint32_t tableID, Pager& pager, Logger& logger, Row& row) {

    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////

    Page* page = requestPageWithSpace(file, logger, pager, tableID, row);
    if (!page) {
        return false;
    }

    size_t insert_position = page->header.freespace;

    page->header.freespace += row.size();
    page->header.NumRows += 1;

    const LogCMD command = LogCMD::INSERT;
    Log log = CreateLog({tableID, page->header.id}, command, page->header, insert_position, row);
    size_t beforeSize = logger.buffer.size();
    UpdateLSN(logger, pager.tableMetadata[tableID], page, log);
    AppendLog(logger, log);
    assert(logger.buffer.size() == (beforeSize + log.size()));
    logger.Txheader.NumLogs++;

    insertRowIntoBuff(page->buffer, row, insert_position);

    mark_dirty(*page);
      
    return true;
}

void printRow(Row& row) {
    std::cout << "tumpstoned: " << row.tumpstoned << "\n";
    std::cout << "row size:   " << row.sizeOfRow  << "\n";
    std::cout << "Entries: \n";
    for (auto& entry : row.values) {
        switch (entry.type) {
            case DataType::INTEIRO:
                std::cout << "Data Type: INTEIRO" << "\n";
                std::cout << "Value: " << std::get<int32_t>(entry.value) << "\n";
                break;
            case DataType::REAL:
                std::cout << "Data Type: REAL" << "\n";
                std::cout << "Value: " << std::get<double>(entry.value) << "\n";
                break;
            case DataType::TEXTO:
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
            case DataType::INTEIRO:
                integer = std::stoll(words[i]);
                entry.value = integer;
                break;
            case DataType::TEXTO:
                entry.value = words[i];
                break;
            case DataType::REAL:
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
            case DataType::INTEIRO:
                ss << std::get<int32_t>(entry.value);
                break;
            case DataType::TEXTO:
                ss << std::get<std::string>(entry.value);
                break;
            case DataType::REAL:
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

//////////////////////////////////////
/* TODO:
 * 1. Inegrate and TEST requestPageWithSpace() func into INSERT func
 * 2. Make and test Iviction policies in PAGER
 * 3. Make auto incriment on insertions for cols with Primary Key
 * 4. Make SURE that if the row has a Primary Key that UPDATE will not auto incriment when inserting
 */
int main() {
    /*
    Row row;
    row.add_entry(static_cast<DataType>(1), 5000);
    row.add_entry(static_cast<DataType>(2), "femboy fridays with yohan the butcher");
    row.add_entry(static_cast<DataType>(3), 12.4556);
    */

    RecordHeader globalRBHeader{0x44415441, 5};
    LoggerHeader globalLogHeader{0x44518449, 4};
    globalLogHeader.LatestLSN = 7;
    globalRBHeader.TABLEID = 1;
    /*
    std::fstream file2("logger.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_logger_header(file2, globalLogHeader)) {
        std::cout << "Failed to flush loggers header at beggining of file\n";
        return 1;
    }
    file2.close();
    */

    /*
    std::fstream file1("data.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!flush_metadata(file1, globalRBHeader)) {
        std::cout << "Failed to flush RB header at beggining of file\n";
        return 1;
    }
 
    file1.close();
    */

    if(!SYNC()) {
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

    if(!START(logger)){
        std::cout << "Failed to start Transaction\n";
        return 1;
    };

    /*
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
    */
    /*
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
    */

    std::ofstream rowFileout("rows4.txt");
    if(!rowFileout) {
        std::cout << "File not found\n";
        return 1;
    }
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    int numpage = pager.tableMetadata[1].PAGECOUNT;
    for(int i = 0; i < numpage; i++) {
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

}
