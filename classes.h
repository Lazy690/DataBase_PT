#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <memory>

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t TABLEID   = 0;
    uint32_t PAGECOUNT = 0;
    uint32_t LatestLSN = 0;
};

struct IndexHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    //This also has a dataType assiciated with it.
};
struct MetaDataHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
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



