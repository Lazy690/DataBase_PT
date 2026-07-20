#pragma once 

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
