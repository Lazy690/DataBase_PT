#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <variant>
#include <span>
#include <cstddef>
#include <cstring>
#include <fstream>
namespace fs = std::filesystem;

#include "src/storage.hpp"
#include "src/filesys.hpp"
#include "classes.h"

const uint32_t FIRST_PAGE_ID = 100;
const uint32_t ROOT = FIRST_PAGE_ID;
const uint32_t NULLPAGE = 0;

//Temporary
const uint32_t fileID = 1;
using PageID = uint32_t;
using var = std::variant <int32_t, std::string, double>;


struct LeafEntry {
    std::variant <int32_t, std::string, double> key;
    PageID data_page;
    uint32_t data_offset;
};
struct InternalEntry {
    std::variant <int32_t, std::string, double> key;
    PageID left_child = 0;
    PageID right_child = 0;

};

struct BTreeNode {
    uint32_t page_id = 0;
    bool is_leaf = true;
    PageID next_leaf = 0;

    std::vector<InternalEntry> children;
    std::vector<LeafEntry> entries;
};

struct BPlusTree {
    IndexHeader header;
    PageID root = ROOT;
    Pager pager;
    BPlusTree() {pager.type = PagerType::INDEX;}
};

//=============================
// Node Section
//=============================

std::vector<char> serializeInternalEntry(const InternalEntry& entry, const DataType type) {
    std::vector<char> bytes;
    switch(type) {
        case DataType::INT:
        {
            auto key = std::get<int32_t>(entry.key);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key), reinterpret_cast<const char*>(&key) + sizeof(key));
        }
            break;
        case DataType::STRING:
        {
            auto key = std::get<std::string>(entry.key);
            uint32_t key_len = static_cast<uint32_t>(key.size());
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key_len), reinterpret_cast<const char*>(&key_len) + sizeof(key_len));
            bytes.insert(bytes.end(), key.begin(), key.end());
        }
            break;
        case DataType::DOUBLE:
        {
            auto key = std::get<double>(entry.key);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key), reinterpret_cast<const char*>(&key) + sizeof(key));
        }
            break;
    }
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&entry.left_child), reinterpret_cast<const char*>(&entry.left_child) + sizeof(entry.left_child));
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&entry.right_child), reinterpret_cast<const char*>(&entry.right_child) + sizeof(entry.right_child));
    return bytes;
}
InternalEntry deserializeInternalEntry(std::span<const char> bytes, const DataType type, size_t& size) {
    InternalEntry entry;
    size_t index = 0;
    switch(type) {
        case DataType::INT:
        {
            auto key = read_bytes<int32_t>(bytes, index);
            assert(key);
            entry.key = *key;
        }
            break;
        case DataType::STRING:
        {
            
            auto key_len = read_bytes<uint32_t>(bytes, index);
            assert(key_len);
            auto key = read_bytes<std::string>(bytes, index, key_len);
            assert(key);
            entry.key = *key;
        }
            break;
        case DataType::DOUBLE:
        {
            auto key = read_bytes<double>(bytes, index);
            assert(key);
            entry.key = *key;
        }
            break;
    }
    auto left_child = read_bytes<uint32_t>(bytes, index);
    auto right_child = read_bytes<uint32_t>(bytes, index);
    assert(left_child);
    assert(right_child);
    entry.left_child = *left_child;
    entry.right_child = *right_child;
    size = index;
    return entry;
}

std::vector<char> serializeLeafEntry(const LeafEntry& entry, const DataType type) {
    std::vector<char> bytes;
    switch(type) {
        case DataType::INT:
        {
            auto key = std::get<int32_t>(entry.key);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key), reinterpret_cast<const char*>(&key) + sizeof(key));
        }
            break;
        case DataType::STRING:
        {
            auto key = std::get<std::string>(entry.key);
            uint32_t key_len = static_cast<uint32_t>(key.size());
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key_len), reinterpret_cast<const char*>(&key_len) + sizeof(key_len));
            bytes.insert(bytes.end(), key.begin(), key.end());
        }
            break;
        case DataType::DOUBLE:
        {
            auto key = std::get<double>(entry.key);
            bytes.insert(bytes.end(), reinterpret_cast<const char*>(&key), reinterpret_cast<const char*>(&key) + sizeof(key));
        }
            break;
    }
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&entry.data_page), reinterpret_cast<const char*>(&entry.data_page) + sizeof(entry.data_page));
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&entry.data_offset), reinterpret_cast<const char*>(&entry.data_offset) + sizeof(entry.data_offset));
    return bytes;
}
LeafEntry deserializeLeafEntry(std::span<const char> bytes, const DataType type, size_t& size) {
    LeafEntry entry;
    size_t index = 0;
    switch(type) {
        case DataType::INT:
        {
            auto key = read_bytes<int32_t>(bytes, index);
            assert(key);
            entry.key = *key;
        }
            break;
        case DataType::STRING:
        {
            
            auto key_len = read_bytes<uint32_t>(bytes, index);
            assert(key_len);
            auto key = read_bytes<std::string>(bytes, index, key_len);
            assert(key);
            entry.key = *key;
        }
            break;
        case DataType::DOUBLE:
        {
            auto key = read_bytes<double>(bytes, index);
            assert(key);
            entry.key = *key;
        }
            break;
    }
    auto data_page = read_bytes<uint32_t>(bytes, index);
    assert(data_page);
    entry.data_page = *data_page;
    auto data_offset = read_bytes<uint32_t>(bytes, index);
    assert(data_offset);
    entry.data_offset = *data_offset;
    size = index;
    return entry;
}

std::vector<char> serializeNode(const BTreeNode& node, const DataType type) {
    std::vector<char> bytes;

    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&node.page_id), reinterpret_cast<const char*>(&node.page_id) + sizeof(node.page_id));
    
    uint8_t is_leaf = node.is_leaf ? 1 : 0;
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&is_leaf), reinterpret_cast<const char*>(&is_leaf) + sizeof(is_leaf));
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&node.next_leaf), reinterpret_cast<const char*>(&node.next_leaf) + sizeof(node.next_leaf));
    //bytes.insert(bytes.end(), reinterpret_cast<const char*>(&node.prev_leaf), reinterpret_cast<const char*>(&node.prev_leaf) + sizeof(node.prev_leaf));

    if(node.is_leaf) {
        assert(node.children.empty());
        for (const auto& entry : node.entries) {
            std::vector<char> leaf_bytes = serializeLeafEntry(entry, type);
            bytes.insert(bytes.end(), leaf_bytes.begin(), leaf_bytes.end());
        }
    }
    else {
        assert(node.entries.empty());
        assert(node.next_leaf == 0);
        for (const auto& child : node.children) {
            std::vector<char> child_bytes = serializeInternalEntry(child, type);
            bytes.insert(bytes.end(), child_bytes.begin(), child_bytes.end());
        }
    }


    return bytes;
}
BTreeNode deserializeNode(const std::span<const char> bytes, const DataType type, const uint32_t NumEntry) {
    BTreeNode node;
    size_t index = 0;

    auto page_id = read_bytes<uint32_t>(bytes, index);
    assert(page_id);
    node.page_id = *page_id;

    auto is_leaf = read_bytes<uint8_t>(bytes, index);
    assert(is_leaf);
    node.is_leaf = (*is_leaf != 0);

    auto next_leaf = read_bytes<uint32_t>(bytes, index);
    assert(next_leaf);
    node.next_leaf = *next_leaf;

    /*
    auto prev_leaf = read_bytes<uint32_t>(bytes, index);
    assert(prev_leaf);
    node.prev_leaf = *prev_leaf;
    */

    auto cursor = bytes.begin() + index;
    if (node.is_leaf) {
        node.entries.reserve(NumEntry);
        for (int i = 0; i < NumEntry; i++) {
            size_t size = 0;
            std::span bytes_range = {cursor, bytes.end()};

            node.entries.push_back(deserializeLeafEntry(bytes_range, type, size));
            cursor += size;
            //std::cout << "size : " << size << "\n";
        }
    }
    else {
        //assert(node.next_leaf == 0 && node.prev_leaf == 0);
        node.children.reserve(NumEntry);
        for (int i = 0; i < NumEntry; i++) {
            size_t size = 0;
            std::span bytes_range = {cursor, bytes.end()};

            node.children.push_back(deserializeInternalEntry(bytes_range, type, size));
            cursor += size;
        }
    }

    return node;
}

void printLeaf(const LeafEntry& leaf, DataType type) {
    switch(type) {
        case DataType::INT:
        {
            std::cout << "Key: " << std::get<int32_t>(leaf.key) << "\n";
        }
            break;
        case DataType::STRING:
        {
            std::cout << "Key: " << std::get<std::string>(leaf.key) << "\n";
        }
            break;
        case DataType::DOUBLE:
        {
            std::cout << "Key: " << std::get<double>(leaf.key) << "\n";
        }
            break;
        default:
            assert(false);
            break;
    }
    std::cout << "Data Page: " << leaf.data_page << "\n";
    std::cout << "Data Offset: " << leaf.data_offset << "\n";
}
void printInternalEntry(const InternalEntry& entry, DataType type) {
    switch(type) {
        case DataType::INT:
        {
            std::cout << "Key: " << std::get<int32_t>(entry.key) << "\n";
        }
            break;
        case DataType::STRING:
        {
            std::cout << "Key: " << std::get<std::string>(entry.key) << "\n";
        }
            break;
        case DataType::DOUBLE:
        {
            std::cout << "Key: " << std::get<double>(entry.key) << "\n";
        }
            break;
    }

    if(entry.left_child != 0) std::cout << "Left Child: " << entry.left_child << "\n";
    if(entry.right_child != 0) std::cout << "Right Child: " << entry.right_child << "\n";
}

void printNode(const BTreeNode& node, DataType type) {
    std::cout << "PageID: " << node.page_id << "\n";
    std::string leaf_status = node.is_leaf ? "is leaf\n" : "is not leaf\n";
    
    std::cout << leaf_status;
    
    if(node.next_leaf != 0) std::cout << "Next Leaf: " << node.next_leaf << "\n";
    //if(node.prev_leaf != 0) std::cout << "prev Leaf: " << node.prev_leaf << "\n";

    if(node.is_leaf) {
        for (auto entry : node.entries) {
            printLeaf(entry, type);
        }
    }
    else {
        for (auto child : node.children) {
            printInternalEntry(child, type);
        }
    }
}

void printTree(std::fstream& file, Pager& pager, BTreeNode& node, DataType type) {

    static int internal_count = 1;

    if(node.is_leaf) {
        printNode(node, type);
    }
    else {
        std::cout << "----Internal Node: " << internal_count++ << " ----\n";
        printNode(node, type);

        int count = 1;
        for (auto child : node.children) {
            std::cout << "----------\nInternal entry: " << count++ << "\n----------\n";
            printInternalEntry(child, type);
            std::cout << "\n\n";
            if (child.left_child == 0) continue; 
            Page* lp = requestPage(file, pager, {fileID, child.left_child}); assert(lp);
            assert(lp);
            BTreeNode lnode = deserializeNode(lp->buffer, type, lp->header.NumRows);
            std::cout << " << LEFT\n";
            printTree(file, pager, lnode, type);

            if (child.right_child == 0) continue;
            Page* rp = requestPage(file, pager, {fileID, child.right_child}); 
            assert(rp);
            BTreeNode rnode = deserializeNode(rp->buffer, type, rp->header.NumRows);
            std::cout << "RIGHT >>\n";
            printTree(file, pager, rnode, type);
        }
        std::cout << "-----------------\n";

    }




}
void overriteNodeIntoBuff(std::vector<char>& buff, const std::vector<char>& nodeBytes, const DataType type) {
    buff = {};
    buff.resize(PAGE_SIZE);
    std::memcpy(buff.data(), nodeBytes.data(), nodeBytes.size());
}
void insertNodeIntoPage(const BTreeNode& node, Page& page, const DataType type) {
    uint32_t NumEntry = 0;
    if (node.is_leaf) {
        assert(node.children.size() == 0);
        NumEntry = node.entries.size();
    }
    else {
        assert(node.entries.size() == 0);
        NumEntry = node.children.size();
    }
    std::vector<char> bytes = serializeNode(node, type);
    if (!(page.header.freespace + bytes.size() <= PAGE_SIZE)) {
        std::cout << "inside Page.header.freespace + bytes.size(): " << page.header.freespace + bytes.size() << " <= " << PAGE_SIZE << "\n";
    }
    assert(page.header.freespace + bytes.size() <= PAGE_SIZE);
    overriteNodeIntoBuff(page.buffer, bytes, type);
    page.header.freespace = bytes.size();
    page.header.NumRows = NumEntry;
}

size_t sorted_insert_position(const std::variant<int32_t, std::string, double>& key, const std::vector<LeafEntry>& buffer, const DataType& type) {
    if(buffer.size() == 0) return 0;
    for(size_t i = 0; i < buffer.size(); i++) {
        switch(type) {
            case DataType::INT:
            {
                if(std::get<int32_t>(key) < std::get<int32_t>(buffer[i].key)) return i;
                else if(std::get<int32_t>(key) == std::get<int32_t>(buffer[i].key)) {
                    while(std::get<int32_t>(buffer[i].key) == std::get<int32_t>(key)) {
                        if(i == buffer.size()) break;
                        i++;
                    }
                    return i;
                }
            }
                break;
            case DataType::STRING:
            {
                if(std::get<std::string>(key) < std::get<std::string>(buffer[i].key)) return i;
                else if(std::get<std::string>(key) == std::get<std::string>(buffer[i].key)) {
                    while(std::get<std::string>(buffer[i].key) == std::get<std::string>(key)) {
                        if(i == buffer.size()) break;
                        i++;
                    }
                    return i;
                }
            }
                break;
            case DataType::DOUBLE:
            {
                if(std::get<double>(key) < std::get<double>(buffer[i].key)) return i;
                else if(std::get<double>(key) == std::get<double>(buffer[i].key)) {
                    while(std::get<double>(buffer[i].key) == std::get<double>(key)) {
                        if(i == buffer.size()) break;
                        i++;
                    }
                    return i;
                }
            }
                break;
        }
    }
    return buffer.size();
}
size_t sorted_insert_position(const std::variant<int32_t, std::string, double>& key, const std::vector<InternalEntry>& buffer, const DataType& type) {
    for(size_t i = 0; i < buffer.size(); i++) {
        switch(type) {
            case DataType::INT:
            {
                //Temporary: evaluate cases of duplicate keys please
                std::cout << "Soriting values: " << std::get<int32_t>(key) << " and " << std::get<int32_t>(buffer[i].key) << "\n";
                assert(std::get<int32_t>(key) != std::get<int32_t>(buffer[i].key));
                if(std::get<int32_t>(key) < std::get<int32_t>(buffer[i].key)) return i;
            }
                break;
            case DataType::STRING:
            {
                assert(std::get<std::string>(key) != std::get<std::string>(buffer[i].key));
                if(std::get<std::string>(key) < std::get<std::string>(buffer[i].key)) return i;
            }
                break;
            case DataType::DOUBLE:
            {
                assert(std::get<double>(key) != std::get<double>(buffer[i].key));
                if(std::get<double>(key) < std::get<double>(buffer[i].key)) return i;
            }
                break;
        }
    }
    return buffer.size();
}
std::optional<size_t> find_leaf_entry_position(const var key, const std::vector<LeafEntry> buffer, const DataType type) {
    for(size_t i = 0; i < buffer.size(); i++) {
        switch(type) {
            case DataType::INT:
            {
                if(std::get<int32_t>(key) == std::get<int32_t>(buffer[i].key)) return i;
            }
                break;
            case DataType::STRING:
            {
                if(std::get<std::string>(key) == std::get<std::string>(buffer[i].key)) return i;
            }
                break;
            case DataType::DOUBLE:
            {
                if(std::get<double>(key) == std::get<double>(buffer[i].key)) return i;
            }
                break;
        }
    }
    return std::nullopt;
}
std::optional<size_t> find_internal_entry_position(const var key, const std::vector<InternalEntry>& buffer, const DataType type) {
    for(size_t i = 0; i < buffer.size(); i++) {
        switch(type) {
            case DataType::INT:
            {
                if(std::get<int32_t>(key) == std::get<int32_t>(buffer[i].key)) return i;
            }
                break;
            case DataType::STRING:
            {
                if(std::get<std::string>(key) == std::get<std::string>(buffer[i].key)) return i;
            }
                break;
            case DataType::DOUBLE:
            {
                if(std::get<double>(key) == std::get<double>(buffer[i].key)) return i;
            }
                break;
        }
    }
    return std::nullopt;
}

void insert_into_leaf(BTreeNode& node, const LeafEntry& entry, const DataType type) {
    assert(node.is_leaf);
    auto insert_pos = node.entries.begin();
    insert_pos += sorted_insert_position(entry.key, node.entries, type);
    node.entries.insert(insert_pos, entry);
}
void delete_from_leaf(BTreeNode& node, const LeafEntry& entry, const DataType type) {
    assert(node.is_leaf);
    auto it = node.entries.begin();
    auto delete_pos = find_leaf_entry_position(entry.key, node.entries, type); 
    if(!delete_pos) return;
    it += *delete_pos;
    node.entries.erase(it);
}
std::vector<LeafEntry> select_from_leaf(const BTreeNode& node, const Conditional conditional, const var key, const DataType type) {
    assert(node.is_leaf);
    std::vector<LeafEntry> results;
    for (const auto entry : node.entries) {
        switch(type) {
            case DataType::INT:
            {
                if(compare(std::get<int32_t>(entry.key), conditional, std::get<int32_t>(key))) {
                    results.push_back(entry);
                }
            }
                break;
            case DataType::STRING:
            {
                if(compare(std::get<std::string>(entry.key), conditional, std::get<std::string>(key))) {
                    results.push_back(entry);
                }
            }
                break;
            case DataType::DOUBLE:
            {
                if(compare(std::get<double>(entry.key), conditional, std::get<double>(key))) {
                    results.push_back(entry);
                }
            }
                break;
        }
    }
    return results;
}
size_t insert_into_internal(BTreeNode& node, const InternalEntry& child, const DataType type) {
    assert(!node.is_leaf);
    size_t insert_pos = sorted_insert_position(child.key, node.children, type);

    auto it = node.children.begin();
    it += insert_pos;
    node.children.insert(it, child);

    return insert_pos;
}
void delete_from_internal(BTreeNode& node, const InternalEntry& child, const DataType type) {
    assert(!node.is_leaf);
    auto it = node.children.begin();
    auto delete_pos = find_internal_entry_position(child.key, node.children, type); 
    if(!delete_pos) return;
    it += *delete_pos;
    node.children.erase(it);
}

PageID FindNextChild(const BTreeNode& node, const var& key, const DataType type) {
    assert(!node.is_leaf);
    assert(!node.children.empty());
    size_t index = 0;
    for (const auto& child : node.children) {
        index++;
        switch(type) {
            case DataType::INT:
            {
                if(std::get<int32_t>(key) < std::get<int32_t>(child.key)) {
                    std::cout << "key: " << std::get<int32_t>(key) << " is lesser then " << std::get<int32_t>(child.key) << "\n";
                    std::cout << "Returning left pointer\n";
                    return child.left_child;
                }
                if(std::get<int32_t>(key) >= std::get<int32_t>(child.key) &&
                   index == node.children.size()) {
                    std::cout << "key: " << std::get<int32_t>(key) << " is greater or equal to " << std::get<int32_t>(child.key) << "\n";
                    std::cout << "Returning right pointer: page " << child.right_child << "\n";
                    std::cout << "----internal entry----\n";
                    printInternalEntry(child, type);
                    return child.right_child;
                }
            }
                break;
            case DataType::STRING:
            {
                if(std::get<std::string>(key) < std::get<std::string>(child.key)) { 
                    if (index == node.children.size()) return child.right_child;
                    return child.left_child;
                }
            }
                break;
            case DataType::DOUBLE:
            {
                if(std::get<double>(key) < std::get<double>(child.key)) {
                    if (index == node.children.size()) return child.right_child;
                    return child.left_child;
                }
            }
                break;
        }
    }
    return 0;
}
PageID FindLeftMostChild(const BTreeNode& node, const DataType type) {
    assert(!node.is_leaf);
    assert(!node.children.empty());
    size_t index = 0;
    return node.children.front().left_child;
}

struct TraverseResult {
    PageID page_id = 0;
    BTreeNode node;
};
struct TraversalHistory {
    std::vector<PageID> nodes;
};

TraverseResult Traverse(std::fstream& file, const var& key, const PageID id, Pager& pager, TraversalHistory& history,const DataType type, int it) {
    TraverseResult result;

    //std::cout << "iteration: " << ++it << "\n";
    Page* nodePage = requestPage(file, pager, {fileID, id});
    assert(nodePage);
    history.nodes.push_back(id);
    BTreeNode node = deserializeNode(nodePage->buffer, type, nodePage->header.NumRows);
    //printNode(node, type);
    //std::cout << "----------\n";
    if(!node.is_leaf) {
        PageID next_child = FindNextChild(node, key, type);
        //std::cout << "next child >> " << next_child<< "\n";
        result = Traverse(file, key, next_child, pager, history, type, it);
    }
    else {
        result = {id, node};
    }
        return result;
}
TraverseResult Traverse_leftmost_leaf(std::fstream& file, const PageID id, Pager& pager, TraversalHistory& history, const DataType type, int it) {
    TraverseResult result;

    Page* nodePage = requestPage(file, pager, {fileID, id});
    assert(nodePage);
    history.nodes.push_back(id);
    BTreeNode node = deserializeNode(nodePage->buffer, type, nodePage->header.NumRows);
    if(!node.is_leaf) {
        PageID left_most_child = FindLeftMostChild(node, type);
        result = Traverse_leftmost_leaf(file, left_most_child, pager, history, type, it);
    }
    else {
        result = {id, node};
    }
        return result;
}

bool will_fit(std::vector<char> nodeBytes) { 
    //Temporary
    //return nodeBytes.size() <= PAGE_SIZE;
    return nodeBytes.size() <= 3;
}
bool will_fit(const size_t size) {
    return size <= 3;
}
bool is_underfull(const size_t size) {
    return size < 2;
}
bool is_at_minimum(const size_t size) {
    return size == 2;
}
bool is_above_minimum(const size_t size) {
    return size > 2;
}

bool is_same_var(var a, var b, DataType type) {
    switch(type) {
        case DataType::INT:
            return std::get<int32_t>(a) == std::get<int32_t>(b);
            break;
        case DataType::STRING:
            return std::get<std::string>(a) == std::get<std::string>(b);
            break;
        case DataType::DOUBLE:
            return std::get<double>(a) == std::get<double>(b);
            break;
    }
    return false;
}

std::optional<size_t> FindNextInternalEntry(const BTreeNode& node, const var key, DataType type) {
    assert(!node.is_leaf);
    //size_t i = 0;
    if(is_same_var(node.children.back().key, key, type)) return std::nullopt;
    for (size_t i = 0; i < node.children.size(); i++) {
        if (is_same_var(node.children[i].key, key, type)) return ++i;
    }
    return std::nullopt;
}
std::optional<size_t> FindPrevInternalEntry(const BTreeNode& node, const var key, DataType type) {
    assert(!node.is_leaf);
    //size_t i = 0;
    //if(node.children.size() <= 1) return std::nullopt;
    if(is_same_var(node.children.front().key, key, type)) return std::nullopt;
    for (size_t i = 0; i < node.children.size(); i++) {
        if (is_same_var(node.children[i].key, key, type)) return --i;
    }
    std::cout << "found nothing\n";
    return std::nullopt;
}

Page* MakeNewRoot(std::fstream& file, BTreeNode& left, const BTreeNode& right, Pager& pager, uint32_t& latest_page_id, DataType type) {
    BTreeNode root;
    root.page_id = ROOT;
    root.is_leaf = false;
    left.page_id = ++latest_page_id;
    Page root_page = create_page(ROOT);
    insertNodeIntoPage(root, root_page, type);
    pager.pages.erase({fileID, ROOT});
    pager.pages.insert({{fileID, ROOT}, root_page});
    return requestPage(file, pager, {fileID, ROOT});
}

void Update_frontNback_entry_pointers(BTreeNode& parent, const InternalEntry& pivot, DataType type) {
    assert(!parent.is_leaf);

    auto next_internal_entry_pos = FindNextInternalEntry(parent, pivot.key, type);
    
    if(next_internal_entry_pos) {
        parent.children[*next_internal_entry_pos].left_child = pivot.right_child;
    }

    auto prev_internal_entry_pos = FindPrevInternalEntry(parent, pivot.key, type);
    
    if(prev_internal_entry_pos) {
        parent.children[*prev_internal_entry_pos].right_child = pivot.left_child;
    }
}
void Update_frontNback_entry_pointers(BTreeNode& parent, const InternalEntry& pivot, BTreeNode& right_node, DataType type) {
    assert(!parent.is_leaf);

    auto next_internal_entry_pos = FindNextInternalEntry(parent, pivot.key, type);
 
    if(next_internal_entry_pos) {
        std::cout << "next internal: " << std::get<int32_t>(parent.children[*next_internal_entry_pos].key) << "\n"; 
        parent.children[*next_internal_entry_pos].left_child = pivot.right_child;
        std::cout << "set left child to page " << parent.children[*next_internal_entry_pos].left_child << "\n";
        std::cout << "right child is page " << parent.children[*next_internal_entry_pos].right_child << "\n";
        right_node.next_leaf = parent.children[*next_internal_entry_pos].right_child;
    }

    auto prev_internal_entry_pos = FindPrevInternalEntry(parent, pivot.key, type);
 
    if(prev_internal_entry_pos) {
        std::cout << "prev internal: " << std::get<int32_t>(parent.children[*prev_internal_entry_pos].key) << "\n"; 
        parent.children[*prev_internal_entry_pos].right_child = pivot.left_child;
        std::cout << "set left child to page " << parent.children[*prev_internal_entry_pos].left_child << "\n";
        std::cout << "right child is page " << parent.children[*prev_internal_entry_pos].right_child << "\n";
    }
}

void SPLIT(std::fstream& file, BTreeNode& node, Pager& pager, const TraversalHistory& history, uint32_t& latest_page_id, const DataType type) {
    if(node.is_leaf) {
        assert(node.children.empty());

        std::vector<LeafEntry> copy = node.entries;
        auto split_pos = copy.begin() + static_cast<uint64_t>(copy.size() / 2); 
        auto beg = copy.begin();
        auto end = copy.end();

        InternalEntry pivot_entry;
        pivot_entry.key = split_pos->key;
        std::vector<LeafEntry> left_entries  = {beg, split_pos};
        std::vector<LeafEntry> right_entries = {split_pos, end};

        BTreeNode& left_node = node;
        BTreeNode right_node;

        right_node.page_id = ++latest_page_id;

        right_node.entries = right_entries;
        left_node.entries = {};
        left_node.entries = left_entries;

        Page* parentPage = nullptr;
        PageID parentID = 0;

        if(left_node.page_id == ROOT) {
            parentPage = MakeNewRoot(file, left_node, right_node, pager, latest_page_id, type);
            parentID == ROOT;
        }
        else {
            assert(history.nodes.size() > 1);
            parentID = history.nodes[history.nodes.size() - 2];
            parentPage = requestPage(file, pager, {fileID, parentID});
        }
        assert(parentPage);

        BTreeNode parent = deserializeNode(parentPage->buffer, type, parentPage->header.NumRows);

        size_t insert_pos = sorted_insert_position(pivot_entry.key, parent.children, type);
        //if children is empty it will skipt these ifs
        std::cout << "insert_pos: " << insert_pos << ", size: " << parent.children.size() << "\n";
        if(insert_pos == parent.children.size() &&
          (parent.children.size() >= 1)) {

            size_t prev_int_pos = insert_pos - 1;
            if (parent.children.size() == 1) {
                prev_int_pos = 0;
            }
            else {
                prev_int_pos = insert_pos - 1;
            }
            InternalEntry& prev_int = parent.children.at(prev_int_pos);
            prev_int.right_child = NULLPAGE;
            std::cout << "\n\nprev key is: " << std::get<int32_t>(prev_int.key) << " left_child : " << prev_int.left_child << " | " << prev_int.right_child << "\n";

            pivot_entry.left_child  = left_node.page_id;
            pivot_entry.right_child = right_node.page_id;

        }
        else if (insert_pos != parent.children.size()){

            auto next_int_pos = FindNextInternalEntry(parent, pivot_entry.key, type);
            assert(next_int_pos);
            InternalEntry& next_int = parent.children.at(*next_int_pos);

            next_int.left_child   = right_node.page_id;
            pivot_entry.right_child = NULLPAGE;
            pivot_entry.left_child  = left_node.page_id;
        }
        else {
            pivot_entry.right_child = right_node.page_id;
            pivot_entry.left_child  = left_node.page_id;
            //std::cout << "key is : " << std::get<int32_t>(pivot_entry.key) << " right: " << pivot_entry.right_child << " left : " << pivot_entry.left_child << "\n";
        }

        if(parent.children.size() > 0) {
            InternalEntry prev_int = parent.children.at(insert_pos - 1);

        }
        insert_into_internal(parent, pivot_entry, type);
        left_node.next_leaf = right_node.page_id;
 
        //Update_frontNback_entry_pointers(parent, pivot_entry, right_node, type);

        Page right_page = create_page(right_node.page_id);
        Page* left_page = requestPage(file, pager, {fileID, left_node.page_id});
        if (!left_page) {
           Page new_page = create_page(left_node.page_id);
           pager.pages.insert({{fileID, left_node.page_id}, new_page});
           left_page = requestPage(file, pager, {fileID, left_node.page_id});
        }

        insertNodeIntoPage(parent, *parentPage, type);
        insertNodeIntoPage(right_node, right_page, type);
        insertNodeIntoPage(left_node, *left_page, type);
        pager.pages.insert({{fileID, right_node.page_id}, right_page});
        pager.pages.insert({{fileID, left_node.page_id}, *left_page});

        if(!will_fit(parent.children.size())) { 
            SPLIT(file, parent, pager, history, latest_page_id, type);
        }
        return;
    }
    else {
        assert(node.entries.empty());

        std::vector<InternalEntry> copy = node.children;
        auto split_pos = copy.begin() + static_cast<uint64_t>(copy.size() / 2); 
        auto beg = copy.begin();
        auto end = copy.end();

        InternalEntry pivot_entry;
        pivot_entry.key = split_pos->key;
        std::vector<InternalEntry> left_chilren   = {beg, split_pos};
        std::vector<InternalEntry> right_children = {split_pos, end};

        BTreeNode& left_node = node;
        left_node.is_leaf = false;
        BTreeNode  right_node;
        right_node.is_leaf = false;

        right_node.page_id = ++latest_page_id;

        right_node.children = right_children;
        left_node.children  = {};
        left_node.children  = left_chilren;

        Page* parentPage = nullptr;
        PageID parentID = 0;

        if(left_node.page_id == ROOT) {
            parentPage = MakeNewRoot(file, left_node, right_node, pager, latest_page_id, type);
            parentID == ROOT;
        }
        else {
            assert(history.nodes.size() > 1);
            size_t prev_node = 0;
            for(size_t i = history.nodes.size(); --i > 0;) {
                if(history.nodes[i] == node.page_id) {
                    prev_node = --i;
                    break;
                }
            }
            //assert(prev_node != 0);
            parentID = history.nodes[prev_node];
            parentPage = requestPage(file, pager, {fileID, parentID});
        }
        assert(parentPage);

        BTreeNode parent = deserializeNode(parentPage->buffer, type, parentPage->header.NumRows);

        size_t insert_pos = sorted_insert_position(pivot_entry.key, parent.children, type);

        if(insert_pos == parent.children.size() &&
          (parent.children.size() > 1)) {

            //std::cout << "key is: " << std::get<int32_t>(pivot_entry.key) << "\n";
            size_t prev_int_pos = insert_pos - 1;
            InternalEntry& prev_int = parent.children.at(prev_int_pos);
            prev_int.right_child = NULLPAGE;

            pivot_entry.left_child  = left_node.page_id;
            pivot_entry.right_child = right_node.page_id;

        }
        else if (insert_pos != parent.children.size()){

            auto next_int_pos = FindNextInternalEntry(parent, pivot_entry.key, type);
            assert(next_int_pos);
            InternalEntry& next_int = parent.children.at(*next_int_pos);

            pivot_entry.right_child = NULLPAGE;
            pivot_entry.left_child  = left_node.page_id;
        }
        else {
            pivot_entry.right_child = right_node.page_id;
            pivot_entry.left_child  = left_node.page_id;
            std::cout << "key is : " << std::get<int32_t>(pivot_entry.key) << " right: " << pivot_entry.right_child << " left : " << pivot_entry.left_child << "\n";
        }
        /*
        pivot_entry.left_child  = left_node.page_id;
        pivot_entry.right_child = right_node.page_id;
        */
        insert_into_internal(parent, pivot_entry, type);
        left_node.children.back().right_child = right_node.children.front().left_child;
        right_node.children.erase(right_node.children.begin());


        //Update_frontNback_entry_pointers(parent, pivot_entry, type);

        Page right_page = create_page(right_node.page_id);
        Page* left_page = requestPage(file, pager, {fileID, left_node.page_id});
        if (!left_page) {
           Page new_page = create_page(left_node.page_id);
           pager.pages.insert({{fileID, left_node.page_id}, new_page});
           left_page = requestPage(file, pager, {fileID, left_node.page_id});
        }
        insertNodeIntoPage(parent, *parentPage, type);
        insertNodeIntoPage(right_node, right_page, type);
        insertNodeIntoPage(left_node, *left_page, type);
        pager.pages.insert({{fileID, right_node.page_id}, right_page});
        pager.pages.insert({{fileID, left_node.page_id}, *left_page});

        if(!will_fit(parent.children.size())) { 
            SPLIT(file, parent, pager, history, latest_page_id, type);
        }
        return;
    }
}
enum class Nstatus {
    ABOVEMIN,
    ATMIN,
    NOTEXISTS
};
struct NeighborStatus {
    Nstatus left;
    Nstatus right;
    std::optional<BTreeNode> right_node;
    std::optional<BTreeNode> left_node;
    BTreeNode parent;
    TraversalHistory history;
};

std::optional<size_t> FindEntryPosition(const BTreeNode internal, const var key, const DataType type) {
    assert(!internal.is_leaf);
    assert(!internal.children.empty());
    size_t index = 0;
    for (const auto& child : internal.children) {
        switch(type) {
            case DataType::INT:
            {
                if(std::get<int32_t>(key) < std::get<int32_t>(child.key)) {
                    std::cout << "key: " << std::get<int32_t>(key) << " is lesser then " << std::get<int32_t>(child.key) << "\n";
                    std::cout << "Returning index " << index << "\n";
                    return index;
                }
                else if(std::get<int32_t>(key) >= std::get<int32_t>(child.key) &&
                    (index + 1) == internal.children.size()) {
                    std::cout << "key: " << std::get<int32_t>(key) << " is greater or equal to " << std::get<int32_t>(child.key) << "\n";
                    std::cout << "Returning index " << index << "\n";
                    std::cout << "----internal entry----\n";
                    printInternalEntry(child, type);
                    return index;
                }
            }
                break;
            case DataType::STRING:
            {
                if(std::get<std::string>(key) < std::get<std::string>(child.key)) {
                    return index;
                }
                else if(std::get<std::string>(key) >= std::get<std::string>(child.key) &&
                    index == internal.children.size()) {
                    return index;
                }
            }
                break;
            case DataType::DOUBLE:
            {
                if(std::get<int32_t>(key) < std::get<int32_t>(child.key)) {
                    return index;
                }
                else if(std::get<int32_t>(key) >= std::get<int32_t>(child.key) &&
                    index == internal.children.size()) {
                    return index;
                }
            }
                break;
        }
        index++;
    }
    return std::nullopt;
}

InternalEntry* FindMiddleIntEntry(BTreeNode& parent,
                                  const BTreeNode& recieverNode, 
                                  const BTreeNode& mergerNode,
                                  const DataType type) {

    InternalEntry* middle_entry = nullptr;
    assert(!recieverNode.entries.empty());

    std::cout << "finding pos: " << std::get<int32_t>(recieverNode.entries.front().key) << "\n";
    auto mergerInternalPos = FindEntryPosition(parent, recieverNode.entries.front().key, type);
    assert(mergerInternalPos);
    std::cout << "finding pos: " << std::get<int32_t>(mergerNode.entries.front().key) << "\n";
    auto recieverInternalPos = FindEntryPosition(parent, mergerNode.entries.front().key, type);
    assert(recieverInternalPos);

    if (mergerInternalPos != recieverInternalPos) {
        if (parent.children.at(*mergerInternalPos).right_child ==
            parent.children.at(*recieverInternalPos).left_child) {
            *recieverInternalPos = *mergerInternalPos;
        }
    }
    assert(is_same_var(parent.children.at(*mergerInternalPos).key,
                     parent.children.at(*recieverInternalPos).key,
                     type) && "Failed to find middle internal entry.\n");

    middle_entry = &parent.children.at(*mergerInternalPos);
    return middle_entry;
}

PageID FindParentPageID(const TraversalHistory& history, const BTreeNode childNode, const size_t* pos = nullptr) {
    if(history.nodes.size() == 1) {
        return 0;
    }
    if (pos) {
        std::cout << "pos: " << *pos << " his size: " << history.nodes.size() << "\n";
        assert(*pos < history.nodes.size());
        if (*pos == 0) return 0;
        return history.nodes.at(*pos - 1);
    }

    size_t prev_node = 0;
    for(size_t i = history.nodes.size(); --i > 0;) {
        if(history.nodes[i] == childNode.page_id) {
            assert(history.nodes[i] == childNode.page_id);
            prev_node = --i;
            break;
        }
    }
    return history.nodes.at(prev_node);
}

void UpdatePointerFromOtherInternal(std::fstream& file, Pager& pager, PageID UpperPageID, InternalEntry* middle_entry, NeighborStatus& status, DataType type) {
    Page* UpperPage = requestPage(file, pager, {fileID, UpperPageID});
    assert(UpperPage);

    BTreeNode upperParent = deserializeNode(UpperPage->buffer, type, UpperPage->header.NumRows);
    assert(!upperParent.is_leaf);

    auto internal_pos = FindEntryPosition(upperParent, middle_entry->key, type);
    assert(internal_pos);

    PageID adjacentInternalID = 0;
    if (status.left_node && !status.right_node) {
        adjacentInternalID = upperParent.children.at(*internal_pos).right_child; 
    }
    else if (!status.left_node && status.right_node) {
        adjacentInternalID = upperParent.children.at(*internal_pos).left_child; 
    }
    assert(adjacentInternalID != 0);

    Page* adjacentInternalPage = requestPage(file, pager, {fileID, adjacentInternalID}); 
    assert(adjacentInternalID);

    BTreeNode AdjNode = deserializeNode(adjacentInternalPage->buffer, type, adjacentInternalPage->header.NumRows);
    assert(!AdjNode.is_leaf);

    if(status.left_node && !status.right_node) {
        std::cout << "left node exists\n\n";

        InternalEntry& leftMostIntEntry = AdjNode.children.front();

        leftMostIntEntry.left_child = status.left_node->page_id;
        std::cout << "Asigning new next_leaf from " << status.left_node->next_leaf << " to " << leftMostIntEntry.right_child << "\n";
        status.left_node->next_leaf = leftMostIntEntry.right_child;
        std::cout << "after: " << status.left_node->next_leaf << "\n\n\n\n\n\n";

        Page* leftPage = requestPage(file, pager, {fileID, status.left_node->page_id});
        assert(leftPage);

        insertNodeIntoPage(AdjNode, *adjacentInternalPage, type);
        insertNodeIntoPage(*status.left_node, *leftPage, type);
    }
    else if(!status.left_node && status.right_node) {

        InternalEntry& rightMostIntEntry = AdjNode.children.back();

        rightMostIntEntry.right_child = status.right_node->page_id;

        Page* leftMostLeafPage = requestPage(file, pager, {fileID, rightMostIntEntry.right_child});
        assert(leftMostLeafPage);

        BTreeNode leftMostLeaf = deserializeNode(leftMostLeafPage->buffer, type, leftMostLeafPage->header.NumRows);
        assert(leftMostLeaf.is_leaf);

        leftMostLeaf.next_leaf = rightMostIntEntry.right_child;

        auto next_internal_pos = FindNextInternalEntry(status.parent, status.parent.children.front().key, type);
        assert(next_internal_pos);
        std::cout << "----\n internal pos on update: " << *next_internal_pos << "\n----\n\n\n";
        status.parent.children.at(*next_internal_pos).left_child = leftMostLeaf.page_id;

        insertNodeIntoPage(AdjNode, *adjacentInternalPage, type);
        insertNodeIntoPage(leftMostLeaf, *leftMostLeafPage, type);
    }
}

void MERGE(std::fstream& file, BTreeNode& mergerNode, BTreeNode& recieverNode, NeighborStatus status, Pager& pager, const DataType type) {
    
    BTreeNode parent = status.parent;
    
    assert(!parent.is_leaf);
    if(mergerNode.is_leaf) {
        assert(recieverNode.is_leaf);

        
        InternalEntry* middle_entry = nullptr;
        assert(!recieverNode.entries.empty());
        if (status.left_node && !status.right_node) {

            for(auto& entry : parent.children) {
                if(is_same_var(mergerNode.children.front().key, entry.key, type)) {
                    middle_entry = &entry;
                    break;
                }
            }
          
        }
        else {
            for(auto& entry : parent.children) {
                if(is_same_var(recieverNode.children.front().key, entry.key, type)) {
                    middle_entry = &entry;
                    break;
                }
            }
        }
        assert(middle_entry);

        middle_entry = FindMiddleIntEntry(status.parent, recieverNode, mergerNode, type);

        for(const auto& entry : mergerNode.entries) {
            insert_into_leaf(recieverNode, entry, type);
        }
        
        //case where middle leaf was eliminated
        if(status.left_node && status.right_node) {
            assert(status.right_node->page_id == recieverNode.page_id);
            auto prev_child_pos = FindPrevInternalEntry(parent, middle_entry->key, type);
            assert(prev_child_pos);
            InternalEntry& prev_child  = parent.children.at(*prev_child_pos);
            prev_child.right_child = status.right_node->page_id;

            status.left_node->next_leaf = status.right_node->page_id;

            Page* leftPage = requestPage(file, pager, {fileID, status.left_node->page_id});
            assert(leftPage);
            insertNodeIntoPage(*status.left_node, *leftPage, type);
        }
        //left most leaf 
        else if(!status.left_node && status.right_node) {
            std::cout << "Runtime got here\n\n";
            const size_t parent_history_position = 1;
            PageID upperPageID = FindParentPageID(status.history, status.parent, &parent_history_position);
            if (upperPageID != 0) {
                UpdatePointerFromOtherInternal(file, pager, upperPageID, middle_entry, status, type);
            }
            recieverNode.next_leaf = status.right_node->next_leaf;
        }
        //right most leaf
        else if(status.left_node && !status.right_node) {
            //0 is null

            const size_t parent_history_position = 1;
            PageID upperPageID = FindParentPageID(status.history, status.parent, &parent_history_position);

            if (upperPageID != 0) {
                UpdatePointerFromOtherInternal(file, pager, upperPageID, middle_entry, status, type);
            }
            else {
                status.left_node->next_leaf = 0;
                Page* leftPage = requestPage(file, pager, {fileID, status.left_node->page_id});
                assert(leftPage);
                insertNodeIntoPage(*status.left_node, *leftPage, type);
            }
            recieverNode.next_leaf = status.left_node->next_leaf;
        }
        else {
            assert(false);
        }

        Page* parentPage   = requestPage(file, pager, {fileID, parent.page_id});
        Page* recieverPage = requestPage(file, pager, {fileID, recieverNode.page_id});
        assert(parentPage);
        assert(recieverPage);

        assert(recieverNode.next_leaf == status.left_node->next_leaf);
        insertNodeIntoPage(recieverNode, *recieverPage, type);

        std::cout << "[Deleting] internal entry: " << std::get<int32_t>(middle_entry->key) << "\n";
        delete_from_internal(parent, *middle_entry,  type);
        std::cout << "post deleteion from internal: \n";
        for(auto entry : parent.children) {
            std::cout << std::get<int32_t>(entry.key) << "\n";
        }
        if (parent.page_id == ROOT && parent.children.empty()) {
            assert(parent.entries.empty());
            parent.entries = recieverNode.entries;
            std::cout << "erasing page leaf: " << mergerNode.page_id << "\n";
            pager.pages.erase({fileID, recieverNode.page_id});
            parent.is_leaf = true;
        }

        std::cout << "------\n";
        insertNodeIntoPage(parent, *parentPage, type);
        std::cout << "erasing page leaf: " << mergerNode.page_id << "\n";
        pager.pages.erase({fileID, mergerNode.page_id});

    }
    else if (!mergerNode.is_leaf) {
        assert(!recieverNode.is_leaf);

        InternalEntry* middle_entry = nullptr;
        assert(!recieverNode.children.empty());
        if (status.left_node && !status.right_node) {

            for(auto& entry : parent.children) {
                if(is_same_var(mergerNode.children.front().key, entry.key, type)) {
                    middle_entry = &entry;
                    break;
                }
            }
          
        }
        else {
            for(auto& entry : parent.children) {
                if(is_same_var(recieverNode.children.front().key, entry.key, type)) {
                    middle_entry = &entry;
                    break;
                }
            }
        }

        for(auto& entry : parent.children) {
            if(is_same_var(recieverNode.children.front().key, entry.key, type)) {
                middle_entry = &entry;
                break;
            }
        }
        assert(middle_entry);

        for(const auto& entry : mergerNode.children) {
            insert_into_internal(recieverNode, entry, type);
        }

        if(status.left_node && status.right_node) {
            //case where middle internal entry was eliminated
            assert(status.right_node->page_id == recieverNode.page_id);
            auto prev_child_pos = FindPrevInternalEntry(parent, middle_entry->key, type);
            assert(prev_child_pos);
            InternalEntry& prev_child  = parent.children.at(*prev_child_pos);
            prev_child.right_child = status.right_node->page_id;

            Page* leftPage = requestPage(file, pager, {fileID, status.left_node->page_id});
            assert(leftPage);
            insertNodeIntoPage(*status.left_node, *leftPage, type);
        }

        Page* parentPage   = requestPage(file, pager, {fileID, parent.page_id});
        Page* recieverPage = requestPage(file, pager, {fileID, recieverNode.page_id});
        assert(parentPage);
        assert(recieverPage);

        insertNodeIntoPage(recieverNode, *recieverPage, type);

        delete_from_internal(parent, *middle_entry,  type);
        std::cout << "post deleteion from internal: \n";
        for(auto entry : parent.children) {
            std::cout << std::get<int32_t>(entry.key) << "\n";
        }
        if (parent.page_id == ROOT && parent.children.empty()){
            parent.children = {};
            parent.children = recieverNode.children;
            std::cout << "erasing page internal: " << mergerNode.page_id << "\n";
            pager.pages.erase({fileID, recieverNode.page_id});
        }

        std::cout << "------\n";
        insertNodeIntoPage(parent, *parentPage, type);
        std::cout << "erasing page internal: " << mergerNode.page_id << "\n";
        pager.pages.erase({fileID, mergerNode.page_id});
    }
    return;
}

enum class BorrowDirection {
    fromRIGHT,
    fromLEFT
};
enum class Direction {
    RIGHT,
    LEFT
};
void BORROW(std::fstream& file, BTreeNode& recieverNode, BTreeNode& giverNode, BTreeNode& parent, 
            Pager& pager, const BorrowDirection direction, const DataType type) {

    assert(!parent.is_leaf);
    if(recieverNode.is_leaf) {
        assert(giverNode.is_leaf);
        switch(direction) {
            case BorrowDirection::fromRIGHT:
            {
                LeafEntry borrowed_entry = giverNode.entries.front();
                recieverNode.entries.push_back(borrowed_entry);

                InternalEntry* middle_entry = nullptr;
                for(auto& entry : parent.children) {
                    if(is_same_var(borrowed_entry.key, entry.key, type)) {
                        middle_entry = &entry;
                        break;
                    }
                }
                assert(middle_entry);

                giverNode.entries.erase(giverNode.entries.begin());
                std::cout << "Giver node entries after delete\n";
                for(auto n : giverNode.entries) {
                    std::cout << std::get<int32_t>(n.key) << "\n";

                }
                std::cout << "\n";
                middle_entry->key = giverNode.entries.front().key;
            }
                break;
            case BorrowDirection::fromLEFT:
            {
                LeafEntry borrowed_entry = giverNode.entries.back();
                recieverNode.entries.insert(recieverNode.entries.begin(), borrowed_entry);

                InternalEntry* middle_entry = nullptr;
                for(auto& entry : parent.children) {
                    if(std::get<int32_t>(entry.key) > std::get<int32_t>(giverNode.entries.back().key)) {
                        middle_entry = &entry;
                        break;
                    }
                }
                assert(middle_entry);

                std::cout << "its this: \n";
                giverNode.entries.erase(giverNode.entries.end() - 1);
                std::cout << "never mind\n";
                std::cout << "Giver node entries after delete\n";
                for(auto n : giverNode.entries) {
                    std::cout << std::get<int32_t>(n.key) << "\n";
                }
                std::cout << "\n";
                middle_entry->key = recieverNode.entries.front().key;
            }
                break;

        }
    }
    else {
        switch(direction) {
            case BorrowDirection::fromRIGHT:
            {
                InternalEntry borrowed_entry = giverNode.children.front();
                recieverNode.children.push_back(borrowed_entry);

                InternalEntry* middle_entry = nullptr;
                for(auto& entry : parent.children) {
                    if(is_same_var(borrowed_entry.key, entry.key, type)) {
                        middle_entry = &entry;
                        break;
                    }
                }
                assert(middle_entry);

                giverNode.children.erase(giverNode.children.begin());
                std::cout << "Giver node entries after delete\n";
                for(auto n : giverNode.children) {
                    std::cout << std::get<int32_t>(n.key) << "\n";

                }
                std::cout << "\n";
                middle_entry->key = giverNode.children.front().key;
            }
                break;
            case BorrowDirection::fromLEFT:
            {
                InternalEntry borrowed_entry = giverNode.children.back();
                recieverNode.children.insert(recieverNode.children.begin(), borrowed_entry);

                InternalEntry* middle_entry = nullptr;
                for(auto& entry : parent.children) {
                    if(std::get<int32_t>(entry.key) > std::get<int32_t>(giverNode.entries.back().key)) {
                        middle_entry = &entry;
                        break;
                    }
                }
                assert(middle_entry);

                giverNode.children.erase(giverNode.children.end() - 1);
                std::cout << "Giver node entries after delete\n";
                for(auto n : giverNode.children) {
                    std::cout << std::get<int32_t>(n.key) << "\n";
                }
                std::cout << "\n";
                middle_entry->key = recieverNode.children.front().key;
            }
                break;
        }
    }

    Page* parentPage   = requestPage(file, pager, {fileID, parent.page_id});
    Page* recieverPage = requestPage(file, pager, {fileID, recieverNode.page_id});
    Page* giverPage    = requestPage(file, pager, {fileID, giverNode.page_id});
    assert(parentPage);
    assert(recieverPage);
    assert(giverPage);

    insertNodeIntoPage(parent, *parentPage, type);
    insertNodeIntoPage(recieverNode, *recieverPage, type);
    insertNodeIntoPage(giverNode, *giverPage, type);

    return;
}
NeighborStatus CheckNeighborStatus(std::fstream& file, BTreeNode& node, Pager& pager, const TraversalHistory& history, const DataType type) {
    NeighborStatus status;
    status.history = history;
    assert(node.page_id != ROOT);

    if(node.is_leaf) {
        /*
        if(node.next_leaf == 0) status.right == Nstatus::NOTEXISTS;
        else {
            auto next_leafId = node.next_leaf;
            Page* nextPage   = requestPage(file, pager, {fileID, next_leafId});
            assert(nextPage);
            BTreeNode next_node = deserializeNode(nextPage->buffer, type, nextPage->header.NumRows);
            assert(!is_underfull(next_node.entries.size()));
            if(is_above_minimum(next_node.entries.size())) {
                status.right = Nstatus::ABOVEMIN;
            }
            if(is_at_minimum(next_node.entries.size())) {
                status.right = Nstatus::ATMIN;
            }
            status.right_node = next_node;
        }
        */

        assert(history.nodes.size() > 1);
        size_t prev_node = 0;
        for(size_t i = history.nodes.size(); --i > 0;) {
            if(history.nodes[i] == node.page_id) {
                prev_node = --i;
                break;
            }
        }
        //assert(prev_node != 0);
        PageID parentID = history.nodes[prev_node];
        Page* parentPage = requestPage(file, pager, {fileID, parentID});
        assert(parentPage);

        BTreeNode parent = deserializeNode(parentPage->buffer, type, parentPage->header.NumRows);
        status.parent = parent;

        InternalEntry current_entry;
        if(parent.children.size() == 1) {
            current_entry = parent.children[0];
        }
        else {
            size_t pos = 0;
            for(const auto& child : parent.children) {
                if(child.left_child == node.page_id) {
                    if (pos == 0 && history.nodes.size() > 2) {
                        status.left = Nstatus::NOTEXISTS;
                    }
                    current_entry = child;
                    break;
                }
                if(child.right_child == node.page_id) {
                    if (pos == (parent.children.size() - 1) && history.nodes.size() > 2) {
                        status.right = Nstatus::NOTEXISTS;
                    }
                    current_entry = child;
                    break;
                }
                pos++;
            }
        }

        PageID prev_leafID = 0;
        PageID next_leafID = 0;
        Page* prev_page = nullptr;
        Page* next_page = nullptr;

        if(current_entry.right_child == node.page_id) {
            prev_leafID = current_entry.left_child;
            Page* prev_page = requestPage(file, pager, {fileID, prev_leafID});
            assert(prev_page);

            BTreeNode prev_leaf = deserializeNode(prev_page->buffer, type, prev_page->header.NumRows);
            assert(!is_underfull(prev_leaf.entries.size()));
            if(is_at_minimum(prev_leaf.entries.size())) {
                std::cout << "Left one was detected as at minimum\n\n\n ---------------";
                int pos = 1;
                for (auto entries: prev_leaf.entries) {
                    std::cout << pos << ". " << std::get<int32_t>(entries.key) << "\n";
                }
                std::cout << "---------------\n\n\n";
                status.left = Nstatus::ATMIN;
            }
            if(is_above_minimum(prev_leaf.entries.size())) {
                std::cout << "Left one was detected as overfull\n\n\n";
                status.left = Nstatus::ABOVEMIN;
            }
            status.left_node = prev_leaf;
            return status;
        }
        else if (current_entry.left_child == node.page_id) {
            next_leafID = current_entry.right_child;
            Page* next_page = requestPage(file, pager, {fileID, next_leafID});
            assert(next_page);

            BTreeNode next_leaf = deserializeNode(next_page->buffer, type, next_page->header.NumRows);
            assert(next_leaf.is_leaf);
            assert(!is_underfull(next_leaf.entries.size()));
            if(is_at_minimum(next_leaf.entries.size())) {
                std::cout << "right one was detected at minimum\n\n\n ---------------";
                int pos = 1;
                for (auto entries: next_leaf.entries) {
                    std::cout << pos << ". " << std::get<int32_t>(entries.key) << "\n";
                }
                std::cout << "---------------\n\n\n";
                status.right = Nstatus::ATMIN;
            }
            if(is_above_minimum(next_leaf.entries.size())) {
                std::cout << "right one was detected as overfull\n\n\n";
                status.right = Nstatus::ABOVEMIN;
            }
            status.right_node = next_leaf;
            return status;
        }

        if(status.right == Nstatus::NOTEXISTS || status.left == Nstatus::NOTEXISTS) {
            return status;
        }

        if(!prev_page) {
            if(parent.children.size() == 1) {
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            auto prev_internal_pos = FindPrevInternalEntry(parent, current_entry.key, type);
            if(!prev_internal_pos) {
                std::cout << "\n\n\n------\nRunTime got here\n------\n\n\n";
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            InternalEntry previous_internal = parent.children[*prev_internal_pos];
            
            assert(previous_internal.left_child != NULLPAGE);
            prev_leafID = previous_internal.left_child;

            Page* prev_page = requestPage(file, pager, {fileID, prev_leafID});
            assert(prev_page);

            BTreeNode prev_leaf = deserializeNode(prev_page->buffer, type, prev_page->header.NumRows);
            assert(!is_underfull(prev_leaf.entries.size()));
            if(is_at_minimum(prev_leaf.entries.size())) {
                status.left = Nstatus::ATMIN;
            }
            if(is_above_minimum(prev_leaf.entries.size())) {
                status.left = Nstatus::ABOVEMIN;
            }
            status.left_node = prev_leaf;
            return status;
        }
        else if(!next_page) {
            if(parent.children.size() == 1) {
                status.right = Nstatus::NOTEXISTS;
                return status;
            }
            auto next_internal_pos = FindNextInternalEntry(parent, current_entry.key, type);
            if(!next_internal_pos) {
                std::cout << "\n\n\n------\nRunTime got here\n------\n\n\n";
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            InternalEntry Next_internal = parent.children[*next_internal_pos];
            
            next_leafID = Next_internal.left_child;
            Page* next_page = requestPage(file, pager, {fileID, next_leafID});
            assert(next_page);

            BTreeNode next_leaf = deserializeNode(next_page->buffer, type, next_page->header.NumRows);
            assert(!is_underfull(next_leaf.entries.size()));
            if(is_at_minimum(next_leaf.entries.size())) {
                status.right = Nstatus::ATMIN;
            }
            if(is_above_minimum(next_leaf.entries.size())) {
                status.right = Nstatus::ABOVEMIN;
            }
            status.right_node = next_leaf;
            return status;
        }
        else {
            assert(false && "Internal entry does not have any Neighbors");
        }

        return status;
    }
    else {

        assert(history.nodes.size() > 1);
        size_t prev_node = 0;
        for(size_t i = history.nodes.size(); --i > 0;) {
            if(history.nodes[i] == node.page_id) {
                prev_node = --i;
                break;
            }
        }
        //assert(prev_node != 0);
        PageID parentID = history.nodes[prev_node];
        Page* parentPage = requestPage(file, pager, {fileID, parentID});
        assert(parentPage);

        BTreeNode parent = deserializeNode(parentPage->buffer, type, parentPage->header.NumRows);
        status.parent = parent;

        InternalEntry current_entry;
        if(parent.children.size() == 1) {
            current_entry = parent.children[0];
        }
        else {
            size_t pos = 0;
            for(const auto& child : parent.children) {
                if(child.left_child == node.page_id) {
                    if (pos == 0) {
                        status.left == Nstatus::NOTEXISTS;
                        return status;
                    }
                    current_entry = child;
                    break;
                }
                if(child.right_child == node.page_id) {
                    current_entry = child;
                    break;
                }
                pos++;
            }
        }
        PageID prev_internalID = 0;
        PageID next_internalID = 0;
        Page* prev_page = nullptr;
        Page* next_page = nullptr;

        if(current_entry.right_child == node.page_id) {
            prev_internalID = current_entry.left_child;
            Page* prev_page = requestPage(file, pager, {fileID, prev_internalID});
            assert(prev_page);

            BTreeNode prev_internal = deserializeNode(prev_page->buffer, type, prev_page->header.NumRows);
            assert(!prev_internal.is_leaf);
            assert(!is_underfull(prev_internal.entries.size()));
            if(is_at_minimum(prev_internal.children.size())) {
                std::cout << "Left one was detected as at minimum\n\n\n ---------------";
                int pos = 1;
                for (auto entries: prev_internal.children) {
                    std::cout << pos << ". " << std::get<int32_t>(entries.key) << "\n";
                }
                std::cout << "---------------\n\n\n";
                status.left = Nstatus::ATMIN;
            }
            if(is_above_minimum(prev_internal.children.size())) {
                std::cout << "Left one was detected as overfull\n\n\n";
                status.left = Nstatus::ABOVEMIN;
            }
            status.left_node = prev_internal;
            return status;
        }
        else if (current_entry.left_child == node.page_id) {
            next_internalID = current_entry.right_child;
            Page* next_page = requestPage(file, pager, {fileID, next_internalID});
            assert(next_page);

            BTreeNode next_internal = deserializeNode(prev_page->buffer, type, prev_page->header.NumRows);
            assert(!next_internal.is_leaf);
            assert(!is_underfull(next_internal.entries.size()));
            if(is_at_minimum(next_internal.children.size())) {
                std::cout << "Left one was detected as at minimum\n\n\n ---------------";
                int pos = 1;
                for (auto entries: next_internal.children) {
                    std::cout << pos << ". " << std::get<int32_t>(entries.key) << "\n";
                }
                std::cout << "---------------\n\n\n";
                status.right = Nstatus::ATMIN;
            }
            if(is_above_minimum(next_internal.children.size())) {
                std::cout << "Left one was detected as overfull\n\n\n";
                status.right = Nstatus::ABOVEMIN;
            }
            status.right_node = next_internal;
            return status;
        }

        if(!prev_page) {
            if(parent.children.size() == 1) {
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            auto prev_internal_pos = FindPrevInternalEntry(parent, current_entry.key, type);
            if(!prev_internal_pos) {
                std::cout << "\n\n\n------\nRunTime got here\n------\n\n\n";
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            InternalEntry previous_internal = parent.children[*prev_internal_pos];
            
            assert(previous_internal.left_child != NULLPAGE);
            prev_internalID = previous_internal.left_child;

            Page* prev_page = requestPage(file, pager, {fileID, prev_internalID});
            assert(prev_page);

            BTreeNode prev_internal = deserializeNode(prev_page->buffer, type, prev_page->header.NumRows);
            assert(!is_underfull(prev_internal.children.size()));
            if(is_at_minimum(prev_internal.children.size())) {
                status.left = Nstatus::ATMIN;
            }
            if(is_above_minimum(prev_internal.children.size())) {
                status.left = Nstatus::ABOVEMIN;
            }
            status.left_node = prev_internal;
            return status;
        }
        else if(!next_page) {
            if(parent.children.size() == 1) {
                status.right = Nstatus::NOTEXISTS;
                return status;
            }
            auto next_internal_pos = FindNextInternalEntry(parent, current_entry.key, type);
            if(!next_internal_pos) {
                std::cout << "\n\n\n------\nRunTime got here\n------\n\n\n";
                status.left = Nstatus::NOTEXISTS;
                return status;
            }
            InternalEntry Next_internal = parent.children[*next_internal_pos];
            
            next_internalID = Next_internal.left_child;
            Page* next_page = requestPage(file, pager, {fileID, next_internalID});
            assert(next_page);

            BTreeNode next_internal = deserializeNode(next_page->buffer, type, next_page->header.NumRows);
            assert(!is_underfull(next_internal.children.size()));
            if(is_at_minimum(next_internal.children.size())) {
                status.right = Nstatus::ATMIN;
            }
            if(is_above_minimum(next_internal.children.size())) {
                status.right = Nstatus::ABOVEMIN;
            }
            status.right_node = next_internal;
            return status;
        }
        else {
            assert(false && "Internal entry does not have any Neighbors");
        }
    }
    return status;
}
void REDESTRIBUTE(std::fstream& file, BTreeNode& node, Pager& pager, const TraversalHistory& history, const DataType type) {
    assert(node.page_id != ROOT);
    assert(is_underfull(node.entries.size()));
    NeighborStatus status = CheckNeighborStatus(file, node, pager, history, type);

    if(status.left == Nstatus::ABOVEMIN) {
        std::cout << "Borrwing from left leaf\n\n";
        assert(status.left_node);
        auto direction = BorrowDirection::fromLEFT;
        BORROW(file, node, *status.left_node, status.parent, pager, direction, type);
        return;
    }
    else if(status.right == Nstatus::ABOVEMIN) {
        std::cout << "Borrwing from right leaf\n\n";
        assert(status.right_node);
        auto direction = BorrowDirection::fromRIGHT;
        BORROW(file, node, *status.right_node, status.parent, pager, direction, type);
        return;
    }

    if(status.right == Nstatus::ATMIN) {
        std::cout << "Merging with right\n\n";
        assert(status.right_node);
        MERGE(file, node, *status.right_node, status, pager, type);
        return;
    }
    else if(status.left == Nstatus::ATMIN) {
        if (status.left_node->is_leaf) {
            std::cout << "Leaf node is ";
        }
        else {
            std::cout << "Internal Entry is ";
        }
        std::cout << "Merging with left\n\n";
        assert(status.left_node);
        MERGE(file, node, *status.left_node, status, pager, type);
        return;
    }

    std::cout << "Did not work properly\n\n\n";
    return;
}

void INSERT_INTO_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DataType type = tree.header.Type;
    TraversalHistory history;
    //std::cout << "Inserting: " << std::get<int32_t>(entry.key) << "\n";
    int it = 0;

    TraverseResult result = Traverse(file, entry.key, tree.root, tree.pager, history, type, it);
    assert(result.node.is_leaf);
    insert_into_leaf(result.node, entry, type);

    Page* page = requestPage(file, tree.pager, {fileID, result.page_id});
    assert(page);

    if (!will_fit(result.node.entries.size())) {
        SPLIT(file, result.node, tree.pager, history, tree.header.LatestPageID, type);
    }
    else {
        insertNodeIntoPage(result.node, *page, type);
    }
}
void DELETE_FROM_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DataType type = tree.header.Type;
    TraversalHistory history;
    //std::cout << "Deleting: " << std::get<int32_t>(entry.key) << "\n";
    int it = 0;
    TraverseResult result = Traverse(file, entry.key, tree.root, tree.pager, history,  type, it);
    assert(result.node.is_leaf);
    delete_from_leaf(result.node, entry, type);

    Page* page = requestPage(file, tree.pager, {fileID, result.page_id});
    assert(page);
    
    if(is_underfull(result.node.entries.size()) &&
        result.node.page_id != ROOT) {
        REDESTRIBUTE(file, result.node, tree.pager, history, type);
    }
    insertNodeIntoPage(result.node, *page, type);
}
void UPDATE_FROM_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DELETE_FROM_TREE(file, entry, tree);
    INSERT_INTO_TREE(file, entry, tree);
}
std::vector<LeafEntry> SELECT_FROM_TREE(std::fstream& file, BPlusTree& tree, const Conditional conditional, const var key) {
    std::vector<LeafEntry> results;
    TraversalHistory history;
    int it = 0;
    TraverseResult traverse_result = Traverse(file, key, tree.root, tree.pager, history,  tree.header.Type, it);
    assert(traverse_result.node.is_leaf);

    auto leaf_select = select_from_leaf(traverse_result.node, conditional, key, tree.header.Type);
    results.insert(results.end(), leaf_select.begin(), leaf_select.end());

    BTreeNode node = traverse_result.node;
    if((conditional == Conditional::GREATER || 
        conditional == Conditional::GREATERorEQUAL)) {
        while(node.next_leaf != 0) {
            Page* page = requestPage(file, tree.pager, {fileID, node.next_leaf});
            assert(page);
            node = {};
            node = deserializeNode(page->buffer, tree.header.Type, page->header.NumRows);
            auto returned_leafs = select_from_leaf(node, conditional, key, tree.header.Type);
            results.insert(results.end(), returned_leafs.begin(), returned_leafs.end());
        }
    }
    else if((conditional == Conditional::LESSER || 
        conditional == Conditional::LESSERorEQUAL)) {
        
        TraverseResult left_child = Traverse_leftmost_leaf(file, tree.root, tree.pager, history, tree.header.Type, it);
        if(left_child.page_id == node.page_id) return results;
        node.next_leaf = left_child.page_id;
        do {
            Page* page = requestPage(file, tree.pager, {fileID, node.next_leaf});
            assert(page);
            node = {};
            node = deserializeNode(page->buffer, tree.header.Type, page->header.NumRows);
            auto returned_leafs = select_from_leaf(node, conditional, key, tree.header.Type);
            results.insert(results.begin(), returned_leafs.begin(), returned_leafs.end());
        } while(node.next_leaf != traverse_result.page_id);
    }
    return results;
}

int main() {

    const auto type = DataType::INT;


    /*
    InternalEntry internal;
    internal.key = 50;
    internal.left_child = 101;
    internal.right_child = 102;
    insert_into_internal(root, internal, type);
    std::cout << "AT initialization: \n";
    printNode(root, type);
    std::cout << "---------------\n";
    std::cout << "After serial/deserial:\n";
    Page test_p;
    insertNodeIntoPage(root, test_p, type);
    BTreeNode test = deserializeNode(test_p.buffer, type, root.children.size());
    printNode(test, type);
    std::cout << "---------------\n";

    BTreeNode left;
    left.page_id = 101;
    left.is_leaf = true;
    left.next_leaf = 102;

    BTreeNode right;
    right.page_id = 102;
    right.is_leaf = true;
    //right.prev_leaf = 101;

    Page root_page;
    root_page.header.id = FIRST_PAGE_ID;
    insertNodeIntoPage(root, root_page, type);

    Page left_page;
    left_page.header.id = 101;
    insertNodeIntoPage(left, left_page, type);

    Page right_page;
    right_page.header.id = 102;
    insertNodeIntoPage(right, right_page, type);

    BPlusTree tree;
    tree.root = FIRST_PAGE_ID;
    tree.header.Type = type;
    tree.pager.pages.insert({{fileID, 100}, root_page});
    tree.pager.pages.insert({{fileID, 101}, left_page});
    tree.pager.pages.insert({{fileID, 102}, right_page});
    */
    BTreeNode root;
    root.page_id = ROOT;
    Page root_page;
    root_page.header.id = FIRST_PAGE_ID;
    insertNodeIntoPage(root, root_page, type);

    BPlusTree tree;
    tree.header.Type = type;
    tree.header.LatestPageID = 100;
    tree.pager.pages.insert({{fileID, 100}, root_page});

    std::fstream file;
    INSERT_INTO_TREE(file, {30, 100, 345}, tree);
    INSERT_INTO_TREE(file, {10, 100, 364}, tree);
    INSERT_INTO_TREE(file, {20, 103, 68}, tree);
    INSERT_INTO_TREE(file, {40, 104, 635}, tree);

    INSERT_INTO_TREE(file, {50, 104, 635}, tree);
    INSERT_INTO_TREE(file, {55, 104, 635}, tree);
    /*
    std::cout << "--TREE mid progress--\n";
    printTree(file, tree);
    std::cout << "---------------------\n";
    */

    INSERT_INTO_TREE(file, {100, 104, 0}, tree);
    INSERT_INTO_TREE(file, {60, 104, 364}, tree);
    INSERT_INTO_TREE(file, {80, 104, 635}, tree);
    INSERT_INTO_TREE(file, {200, 104, 635}, tree);

    INSERT_INTO_TREE(file, {300, 104, 635}, tree);
    INSERT_INTO_TREE(file, {250, 104, 635}, tree);
    INSERT_INTO_TREE(file, {400, 104, 635}, tree);

    DELETE_FROM_TREE(file, {200, 100, 100}, tree);
    std::vector<LeafEntry> results = SELECT_FROM_TREE(file, tree, Conditional::EQUAL, 250);
    /*
    std::cout << "-----RESULT-----" << "\n";
    if(results.empty()) std::cout << "No entries found\n";
    else {
        for (auto result: results) {
            printLeaf(result, type);
        }
    }
    */
  
    std::cout << "\n\n\n\n\n\n\n";
    std::cout << "---TREE---\n";

    Page* p = requestPage(file, tree.pager, {fileID, tree.root});
    assert(p);
    BTreeNode rootn = deserializeNode(p->buffer, type, p->header.NumRows);
    assert(root.page_id == ROOT);
    printTree(file, tree.pager, rootn, type);

    std::cout << "Compiles!\n\n";
    return 0;
}

