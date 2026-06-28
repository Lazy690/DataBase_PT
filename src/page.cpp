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
#include <span>

const int KILOBYTE = 1024;
constexpr int PAGE_SIZE = KILOBYTE; 
const int MAXPAGES = 50;

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
};

struct RecordHeader {
    uint32_t MAGIC     = 0;
    uint32_t VERSION   = 0;
    uint32_t PAGECOUNT = 0;
};

struct PageHeader {
    uint32_t id        = 0;
    uint32_t freespace = 0;
    uint32_t NumRows   = 0;
};

struct Page {
    PageHeader header;
    bool dirty = false;
    std::vector<char> buffer;
    Page() : buffer(PAGE_SIZE) {}
};

struct Pager {
    std::unordered_map<std::string, RecordHeader> tableMetadata;
    std::unordered_map<int, Page> pages;
    Pager() {
        pages.reserve(MAXPAGES);
    }
};

Page create_page(uint32_t id) {
    PageHeader newHeader = {id, 0, 0};
    Page newPage;
    newPage.header = newHeader;
    newPage.dirty = true;
    return newPage;
}

std::vector<char> serializeRow(Row& row) {

    std::vector<char> bytes;
    bytes.reserve(64 + row.sizeOfRow);

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

std::optional<Row>
deserializeRow(std::span<const char> rowBytes) {
    Row row;
    std::size_t index = 0;

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

bool flush_page(std::fstream& file, const Page& page) {
    
    int pageOffset = sizeof(RecordHeader) + (PAGE_SIZE * page.header.id);
    file.seekg(pageOffset, std::ios::beg);

    file.write(reinterpret_cast<const char*>(&page.header), sizeof(PageHeader));
    if(!file || file.tellg() != (pageOffset + sizeof(PageHeader))) {
        std::cerr << "Failed to flush header" << std::endl;
        return false;
    }

    const int DataBytesOffs = sizeof(PageHeader) + pageOffset;
    file.seekg(DataBytesOffs, std::ios::beg);

    file.write(page.buffer.data(), PAGE_SIZE);

    if(!file) {
        std::cerr << "Failed to flush byte buffer" << std::endl;
        return false;
    }

    return true;
}

void mark_dirty(Page& page) {
    page.dirty = true;
}
void mark_clean(Page& page) {
    page.dirty = false;
}
bool will_fit(const Page& page, size_t rowSize) {
    auto space = PAGE_SIZE - page.header.freespace;
    if((space + rowSize) > space) return true;
    return false;
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

template<typename T>
bool compare(T RowValue, Conditional conditional, T value) {
    if (conditional == Conditional::EQUAL) {

        if(RowValue == value){
            //std::cout << value << " is equal to " << RowValue << "\n";
            return true;
        }
        else {
            //std::cout << value << " is NOT equal to " << RowValue << "\n";
            return false;

        } 

    }
    if (conditional == Conditional::GREATER) {

        if(RowValue < value){
            //std::cout << value << " is greater to " << RowValue << "\n";
            return true;
        }
        else {
            //std::cout << value << " is NOT greater to " << RowValue << "\n";
            return false;

        } 

    }
    if (conditional == Conditional::LESSER) {
      
        if(RowValue > value){
            //std::cout << value << " is lesser to " << RowValue << "\n";
            return true;
        }
        else {
            //std::cout << value << " is NOT lesser to " << RowValue << "\n";
            return false;

        } 

    }
    if (conditional == Conditional::GREATERorEQUAL) {

        if(RowValue <= value){
            //std::cout << value << " is greater or equal to " << RowValue << "\n";
            return true;
        }
        else {
            //std::cout << value << " is NOT greater or equal to " << RowValue << "\n";
            return false;

        } 

    }
    if (conditional == Conditional::LESSERorEQUAL) {
      
        if(RowValue >= value){
            //std::cout << value << " is lesser or equal to " << RowValue << "\n";
            return true;
        }
        else {
            //std::cout << value << " is NOT lesser or equal to " << RowValue << "\n";
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
            continue;
        }

        size_t rowOffset  = std::distance(page.buffer.begin(), cursor);
        size_t ID         = page.header.id;

        std::visit([&value, conditional, &results, row, rowOffset, ID](const auto& x) {
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
ScanTable(std::string TableName, Pager& pager, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
    
    std::vector<ScanResult> results;

    if(pager.tableMetadata[TableName].PAGECOUNT <= 0) {
        std::cout << "Table has no pages\n";
        return results;
    } 
    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////

    for (int i = 0; i < pager.tableMetadata[TableName].PAGECOUNT; i++) {
        auto it = pager.pages.find(i);
        if(it == pager.pages.end()) {
          
            std::cout << "Did not find page id: " << i << "\n"; 
            auto loadedPage = load_page(file, i);
            if(!loadedPage) {
                std::cerr << "Failed to load page of ID: " << i << " When scanning the table\n";
                return std::nullopt;
            }

            std::cout << "loaded Page id: " << i << "\n";

            pager.pages.insert({i, *loadedPage});
        }
        
        std::vector<ScanResult> result;       
        if(!ScanPage(result, pager.pages.at(i), column_index, conditional, value)) {
            std::cerr << "Failed to scan table\n";
            return std::nullopt;
        }

        std::cout << "Result for page: " << i << " is " << result.size() << "\n";

        results.insert(results.end(), result.begin(), result.end());
    }

    return results;

}

struct QueryParams {
    
};

bool INSERT(Pager& tracker, Row& row) {

    std::vector<char> rowBytes = serializeRow(row);
    bool found = false;
    Page* page = find_free_page(tracker, rowBytes.size());
    /*
    if(page == nullptr) {
        auto it = tracker.pages.end();
        tracker.pages.insert(it, create_page(tracker.latestID));
        page = &tracker.pages.back();
    }
    */

    auto insert_position = page->buffer.begin() + page->header.freespace;
    
    page->buffer.insert(insert_position, rowBytes.data(), rowBytes.data() + rowBytes.size());

    page->header.freespace += rowBytes.size();
    mark_dirty(*page);
    page->header.NumRows += 1;
    
    return true;
}
 
bool SELECT(std::vector<Row>& resultSet, std::string TableName, Pager& pager, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
    auto resultsPtr = ScanTable(TableName, pager, column_index, conditional, value);
    if (!resultsPtr) {
        return false;
    }

    std::vector<ScanResult> results = *resultsPtr;
    resultSet.resize(results.size());
    for (int i = 0; i < results.size(); i++) {
        resultSet.push_back(results[i].row);
    }
    return true;
}

bool DELETE(std::string TableName, Pager& pager, size_t column_index, const Conditional conditional, const std::variant<int32_t, std::string, double> value) {
    auto resultsPtr = ScanTable(TableName, pager, column_index, conditional, value);
    if(!resultsPtr) {
        return false;
    }

    std::vector<ScanResult> results = *resultsPtr;
    //!!!!!!Temporary!!!!!!
    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in);
    ///////////////////////
    for (auto& result : results) {
        size_t ID = result.pageId;
        auto it = pager.pages.find(ID);

        if(it == pager.pages.end()) {
            auto loadedPage = load_page(file, ID);
            if(!loadedPage) {
                return false;
            }
            pager.pages.insert({ID, *loadedPage});
        }

        result.row.tumpstoned = 1;
        std::vector<char> RowBytes = serializeRow(result.row);
        
        auto buffBegin = pager.pages[ID].buffer.begin();
        pager.pages[ID].buffer.insert(buffBegin + result.offset, RowBytes.begin(), RowBytes.end());
        mark_dirty(pager.pages[ID]);
        pager.pages[ID].header.NumRows--;
    }

    return true;
}

bool UPDATE() {
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

void test_serialize() {
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
    std::optional<Row> deRow = deserializeRow({bytes.begin(), bytes.end()});
    if(!deRow) {
        std::cerr << "Failed to deserialize Row \n";
        return;
    }
    printRow(*deRow);
}

bool INSERT(Page& page, Row& row) {

    std::vector<char> rowBytes = serializeRow(row);
    /*
    if(page == nullptr) {
        auto it = tracker.pages.end();
        tracker.pages.insert(it, create_page(tracker.latestID));
        page = &tracker.pages.back();
    }
    */

    auto insert_position = page.buffer.begin() + page.header.freespace;
    
    page.buffer.insert(insert_position, rowBytes.data(), rowBytes.data() + rowBytes.size());

    page.header.freespace += rowBytes.size();
    mark_dirty(page);
      page.header.NumRows += 1;
      
      return true;
  }
int main() {

    Row row1;
    row1.add_entry(static_cast<DataType>(1), 26);
    row1.add_entry(static_cast<DataType>(2), "Monday");
    row1.add_entry(static_cast<DataType>(3), 20.4);

    Row row2;
    row2.add_entry(static_cast<DataType>(1), 100);
    row2.add_entry(static_cast<DataType>(2), "Tuesday");
    row2.add_entry(static_cast<DataType>(3), 120.4);

    Row row3;
    row3.add_entry(static_cast<DataType>(1), 5000);
    row3.add_entry(static_cast<DataType>(2), "Wensday");
    row3.add_entry(static_cast<DataType>(3), 12.4556);

    Row row4;
    row4.add_entry(static_cast<DataType>(1), 26);
    row4.add_entry(static_cast<DataType>(2), "Thursday");
    row4.add_entry(static_cast<DataType>(3), 20.4);

    Row row5;
    row5.add_entry(static_cast<DataType>(1), 100);
    row5.add_entry(static_cast<DataType>(2), "Friday");
    row5.add_entry(static_cast<DataType>(3), 120.4);

    Row row6;
    row6.add_entry(static_cast<DataType>(1), 5000);
    row6.add_entry(static_cast<DataType>(2), "Saturday");
    row6.add_entry(static_cast<DataType>(3), 12.4556);

    Row row7;
    row7.add_entry(static_cast<DataType>(1), 26);
    row7.add_entry(static_cast<DataType>(2), "Sunday");
    row7.add_entry(static_cast<DataType>(3), 20.4);

    Row row8;
    row8.add_entry(static_cast<DataType>(1), 100);
    row8.add_entry(static_cast<DataType>(2), "Monday Funday");
    row8.add_entry(static_cast<DataType>(3), 120.4);

    Row row9;
    row9.add_entry(static_cast<DataType>(1), 5000);
    row9.add_entry(static_cast<DataType>(2), "Femboy fridays with yohan the butcher");
    row9.add_entry(static_cast<DataType>(3), 12.4556);


    PageHeader header;
    Page page;
    header.id = 0;
    page.header = header;

    PageHeader header2;
    Page page2;
    header2.id = 1;
    page2.header = header2;

    PageHeader header3;
    Page page3;
    header3.id = 2;
    page3.header = header3;

    RecordHeader RH;
    RH.PAGECOUNT = 3;


    if(!INSERT(page, row1)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page, row2)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page, row3)) {
        std::cerr << "failed to insert\n";
        return 1;
    }



    if(!INSERT(page2, row4)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page2, row5)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page2, row6)) {
        std::cerr << "failed to insert\n";
        return 1;
    }



    if(!INSERT(page3, row7)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page3, row8)) {
        std::cerr << "failed to insert\n";
        return 1;
    }
    if(!INSERT(page3, row9)) {
        std::cerr << "failed to insert\n";
        return 1;
    }

    Pager tracker;
    tracker.tableMetadata["Schedules"] = RH;
    tracker.pages[0] = page;
    tracker.pages[1] = page2;
    tracker.pages[2] = page3;

    std::fstream file("data.bin", std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    flush_page(file, tracker.pages[0]);
    flush_page(file, tracker.pages[1]);
    flush_page(file, tracker.pages[2]);


    if(!file) {
        std::cout << "failed to find or open file\n";
        return 1;
    }
    file.close();

    Pager pager;
    pager.tableMetadata["Schedules"] = RH;

    if (!DELETE("Schedules", pager, 1, Conditional::EQUAL, "Femboy fridays with yohan the butcher")) {
        std::cout << "SELECT Failed\n";
        return 1;
    }

    std::vector<Row> resultSet;

    if (!SELECT(resultSet, "Schedules", pager, 2, Conditional::EQUAL, 12.4556)) {
        std::cout << "SELECT Failed\n";
        return 1;
    }

    for (auto result : resultSet) {
        printRow(result);
    }

    std::cout << "Compiles!\n";

}
