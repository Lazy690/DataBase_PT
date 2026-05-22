#include <iostream>
#include <fstream>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <cstddef> 
#include <expected>

const int KILOBYTE = 1024;
const int MAXPAGES = 50;

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
    char buffer[KILOBYTE] = {};
};


bool load_page(std::fstream& file, const int id, Page& page) {
    
    int pageOffset = sizeof(RecordHeader) + (KILOBYTE * id);
    file.seekg(pageOffset, std::ios::beg);
    file.read(reinterpret_cast<char*>(&page.header), sizeof(PageHeader));

    if(!file) {
        std::cerr << "Failed to load page header" << std::endl;
        return false;
    }
    if(page.header.id != id) {
        std::cerr << "Failed to load correct page" << std::endl;
        return false;
    }

    const int DataBytesOffs = pageOffset + sizeof(PageHeader);
    file.seekg(DataBytesOffs, std::ios::beg);

    file.read(page.buffer, sizeof(page.buffer));

    if(!file) {
        std::cerr << "Failed to load byte buffer" << std::endl;
        return false;
    }

    return true;
}
bool flush_page(std::fstream& file, const Page& page) {
    
    int pageOffset = sizeof(RecordHeader) + (KILOBYTE * page.header.id);
    file.seekg(pageOffset, std::ios::beg);

    file.write(reinterpret_cast<const char*>(&page.header), sizeof(PageHeader));
    if(!file || file.tellg() != (pageOffset + sizeof(PageHeader))) {
        std::cerr << "Failed to flash header" << std::endl;
        return false;
    }

    const int DataBytesOffs = pageOffset + sizeof(PageHeader);
    file.seekg(DataBytesOffs, std::ios::beg);

    file.write(page.buffer, sizeof(page.buffer));

    if(!file || file.tellg() != (DataBytesOffs + sizeof(page.buffer))) {
        std::cerr << "Failed to flash byte buffer" << std::endl;
        return false;
    }

    return true;
}

bool mark_dirty(const std::string& table, const int id) {

}

