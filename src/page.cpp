#include <iostream>
#include <fstream>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <cstddef> 
#include <optional>
#include <cassert>
#include <variant>
#include <vector>
#include <cstring>

const int KILOBYTE = 1024;
constexpr int PAGE_SIZE = KILOBYTE; 
const int MAXPAGES = 50;

enum class DataType : uint32_t {
    INTEIRO = 1, //int
    TEXTO = 2, //string
    REAL = 3 //double
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
        // note: don't add sizeof(t) here because we store type as uint32_t in serialization
        if      (t == DataType::INTEIRO) sizeOfRow += sizeof(int32_t);
        else if (t == DataType::TEXTO)   sizeOfRow += sizeof(uint32_t) + static_cast<uint32_t>(std::get<std::string>(v).size());
        else if (t == DataType::REAL)    sizeOfRow += sizeof(double);

        // add space for the type tag (we use uint32_t)
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
};

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t PAGECOUNT = 0;
};

struct PageHeader {
    uint32_t id        = 0;
    uint32_t freespace = 0;
    uint32_t EOP       = 0;
};

struct Page {
    PageHeader header;
    bool dirty = false;
    std::vector<char> buffer;
    Page() : buffer(PAGE_SIZE) {}
};

Page create_page(uint32_t id) {
    PageHeader newHeader = {id, 0, 0};
    Page newPage;
    newPage.header = newHeader;
    return newPage;
}

std::vector<char> serializeRow(Row& row) {

    std::vector<char> bytes;
    bytes.reserve(64 + row.sizeOfRow);

    // serialize tombstone as uint8_t
    uint8_t tomb = row.tumpstoned ? 1 : 0;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&tomb), reinterpret_cast<const char*>(&tomb) + sizeof(tomb));

    // write sizeOfRow (uint32_t) immediately
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

template<typename T>
std::optional<T> read_bytes(std::vector<char>& buff, std::size_t& index, std::optional<uint32_t> str_len = std::nullopt) {
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

std::optional<Row>
deserializeRow(std::vector<char>& rowBytes) {
    Row row;
    std::size_t index = 0;

    // read tombstone (uint8_t)
    auto tomb_u = read_bytes<uint8_t>(rowBytes, index);
    if(!tomb_u) { std::cerr << "index went over the buffer size\n"; return std::nullopt; }
    row.tumpstoned = (*tomb_u != 0);

    std::optional<uint32_t> sizeOfRow = read_bytes<uint32_t>(rowBytes, index);
    if(!sizeOfRow) {
        std::cerr << "index went over the buffer size" << std::endl;
        return std::nullopt;
    }

    std::vector<Entry> entries;
    std::size_t end = index + *sizeOfRow;

    while (index < end) {
        // read type as uint32_t then cast
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

        entries.push_back(std::move(entry));
    }
    
    row.sizeOfRow  = *sizeOfRow;
    row.values     = std::move(entries);

    return row;
}

std::optional<Page>
load_page(std::fstream& file, const int id) {

    Page page;

    int pageOffset = sizeof(RecordHeader) + (PAGE_SIZE * id);
    file.seekg(pageOffset, std::ios::beg);
    file.read(reinterpret_cast<char*>(&page.header), sizeof(PageHeader));

    if(!file) {
        std::cerr << "Failed to load page header" << std::endl;
        return std::nullopt;
    }
    if(page.header.id != id) {
        std::cerr << "Failed to load correct page" << std::endl;
        return std::nullopt;
    }

    const int DataBytesOffs = pageOffset + sizeof(PageHeader);
    file.seekg(DataBytesOffs, std::ios::beg);

    file.read(page.buffer.data(), PAGE_SIZE);

    if(!file) {
        std::cerr << "Failed to load byte buffer" << std::endl;
        return std::nullopt;
    }

    return page;
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

bool flush_page(std::fstream& file, const Page& page) {
    
    int pageOffset = sizeof(RecordHeader) + (PAGE_SIZE * page.header.id);
    file.seekg(pageOffset, std::ios::beg);

    file.write(reinterpret_cast<const char*>(&page.header), sizeof(PageHeader));
    if(!file || file.tellg() != (pageOffset + sizeof(PageHeader))) {
        std::cerr << "Failed to flash header" << std::endl;
        return false;
    }

    const int DataBytesOffs = pageOffset + sizeof(PageHeader);
    file.seekg(DataBytesOffs, std::ios::beg);

    file.write(page.buffer.data(), PAGE_SIZE);

    if(!file || file.tellg() != (DataBytesOffs + sizeof(page.buffer))) {
        std::cerr << "Failed to flash byte buffer" << std::endl;
        return false;
    }

    return true;
}

void mark_dirty(Page& page) {
    page.dirty = true;
}

bool INSERT(Page& page, const Row& row) {
    return true;
}

int main() {
    Entry entry01{static_cast<DataType>(1), 26};
    Entry entry02{static_cast<DataType>(2), "Monday"};
    Entry entry03{static_cast<DataType>(3), 20.4};
    Row row;

    row.add_entry(static_cast<DataType>(1), 26);
    row.add_entry(static_cast<DataType>(2), "Monday");
    row.add_entry(static_cast<DataType>(3), 20.4);

    std::vector<char> bytes = serializeRow(row);
    std::cout << "Size of bytes: " << bytes.size() << "\n";
    std::cout << "Size of row: " << sizeof(row) << "\n";
    std::ofstream file("file.bin", std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), bytes.size());
    file.close();
    std::optional<Row> deRow = deserializeRow(bytes);
    if(!deRow) {
        std::cerr << "Failed to deserialize Row \n";
        return 1;
    }
    printRow(*deRow);
}
