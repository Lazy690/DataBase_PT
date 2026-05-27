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
    
    const char* tumpstoned_byte = reinterpret_cast<const char*>(&row.tumpstoned);
    const char* sizeOfRow_byte  = reinterpret_cast<const char*>(&row.sizeOfRow);
    bytes.insert(bytes.end(), tumpstoned_byte, tumpstoned_byte + sizeof(tumpstoned_byte));
    bytes.insert(bytes.end(), sizeOfRow_byte, sizeOfRow_byte + sizeof(sizeOfRow_byte));

    for (auto& entry : row.values) {
        
        const char* type_byte = reinterpret_cast<const char*>(&entry.type);
        bytes.insert(bytes.end(), type_byte, type_byte + sizeof(type_byte));
          
        if(entry.type == DataType::INTEIRO) {
            const int32_t integer = std::get<int32_t>(entry.value);
            const char* integer_byte = reinterpret_cast<const char*>(&integer);
            bytes.insert(bytes.end(), integer_byte, integer_byte + sizeof(integer_byte));
        }
        if(entry.type == DataType::TEXTO) {
            const std::string str = std::get<std::string>(entry.value);
            const uint32_t len = str.size();

            const char* len_byte = reinterpret_cast<const char*>(&len_byte);

            bytes.insert(bytes.end(), len_byte, len_byte + sizeof(len_byte));
            bytes.insert(bytes.end(), str.begin(), str.end());
        }
        if(entry.type == DataType::REAL) {
            const double dub = std::get<double>(entry.value);
            const char* dub_byte = reinterpret_cast<const char*>(&dub);
            bytes.insert(bytes.end(), dub_byte, dub_byte + sizeof(dub_byte));
        }
    }

    return bytes;
}

template<typename T>
std::optional<T> read_bytes(std::vector<char>& buff, std::size_t index, std::optional<uint32_t> str_len = std::nullopt) {
    if(index + sizeof(T) > buff.size()) return std::nullopt; 

    T value;
    if constexpr (std::is_same_v<T, std::string>) {
        if(str_len == std::nullopt) return std::nullopt;
        value.resize(*str_len);
    }
    std::memcpy(&value, buff.data() + index, sizeof(T));
    index += sizeof(T);
    return value;
}

std::optional<Row>
deserializeRow(std::vector<char>& rowBytes) {
    Row row;
    std::size_t index = 0;

    std::optional<bool> tumpstoned = read_bytes<bool>(rowBytes, index);
    if(tumpstoned == std::nullopt) {
        std::cerr << "index went over the buffer size" << std::endl;
        return std::nullopt;
    } 
    std::optional<uint32_t> sizeOfRow = read_bytes<uint32_t>(rowBytes, index);
    if(!sizeOfRow) {
        std::cerr << "index went over the buffer size" << std::endl;
        return std::nullopt;
    }

    std::vector<Entry> entries;

    while (index < (index + *sizeOfRow)) {
        Entry entry;
        std::optional<DataType> type = read_bytes<DataType>(rowBytes, index);
        if(!type) {
            std::cerr << "index went over the buffer size" << std::endl;
            return std::nullopt;
        }

        entry.type = *type;

        std::optional<int32_t>     integer;
        std::optional<double>      dub;
        std::optional<uint32_t>    str_len;
        std::optional<std::string> str;

        switch (entry.type) {
            case DataType::INTEIRO:
                integer = read_bytes<int32_t>(rowBytes, index);
                if(!integer) {
                    std::cerr << "index went over the buffer size" << std::endl;
                    return std::nullopt;
                }
                entry.value = *integer;
                break;
            case DataType::REAL:
                dub     = read_bytes<double>(rowBytes, index);
                if(!dub) {
                    std::cerr << "index went over the buffer size" << std::endl;
                    return std::nullopt;
                }
                entry.value = *dub;
                break;
            case DataType::TEXTO:
                str_len = read_bytes<uint32_t>(rowBytes, index);
                if(!str_len) {
                    std::cerr << "index went over the buffer size or string len was null" << std::endl;
                    return std::nullopt;
                }
                str     = read_bytes<std::string>(rowBytes, index, str_len);
                if(!str) {
                    std::cerr << "index went over the buffer size" << std::endl;
                    return std::nullopt;
                }
                entry.value = *str;
        }

        entries.push_back(entry);

    }
    
    row.tumpstoned = *tumpstoned;
    row.sizeOfRow  = *sizeOfRow;
    row.values     = entries;

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

}
