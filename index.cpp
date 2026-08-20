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
    PageID prev_leaf = 0;

    std::vector<InternalEntry> children;
    std::vector<LeafEntry> entries;
};

struct BPlusTree {
    IndexHeader header;
    PageID root;
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
    bytes.insert(bytes.end(), reinterpret_cast<const char*>(&node.prev_leaf), reinterpret_cast<const char*>(&node.prev_leaf) + sizeof(node.prev_leaf));

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

    auto prev_leaf = read_bytes<uint32_t>(bytes, index);
    assert(prev_leaf);
    node.prev_leaf = *prev_leaf;

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
        assert(node.next_leaf == 0 && node.prev_leaf == 0);
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
    if(node.prev_leaf != 0) std::cout << "prev Leaf: " << node.prev_leaf << "\n";

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
    assert(page.header.freespace + bytes.size() <= PAGE_SIZE);
    overriteNodeIntoBuff(page.buffer, bytes, type);
    page.header.freespace = bytes.size();
    page.header.NumRows = NumEntry;
}

size_t sorted_insert_position(const std::variant<int32_t, std::string, double>& key, const std::vector<LeafEntry>& buffer, const DataType& type) {
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
void insert_into_internal(BTreeNode& node, const InternalEntry& child, const DataType type) {
    assert(!node.is_leaf);
    auto insert_pos = node.children.begin();
    insert_pos += sorted_insert_position(child.key, node.children, type);
    node.children.insert(insert_pos, child);
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
                    return child.left_child;
                }
                if(std::get<int32_t>(key) >= std::get<int32_t>(child.key)) {
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

struct TraverseResult {
    PageID page_id = 0;
    BTreeNode node;
};

TraverseResult Traverse(std::fstream& file, const var& key, const PageID id, Pager& pager, const DataType type, int it) {
    TraverseResult result;

    //std::cout << "iteration: " << ++it << "\n";
    Page* nodePage = requestPage(file, pager, {fileID, id});
    assert(nodePage);
    BTreeNode node = deserializeNode(nodePage->buffer, type, nodePage->header.NumRows);
    //printNode(node, type);
    //std::cout << "----------\n";
    if(!node.is_leaf) {
        PageID next_child = FindNextChild(node, key, type);
        //std::cout << "next child >> " << next_child<< "\n";
        result = Traverse(file, key, next_child, pager, type, it);
    }
    else {
        result = {id, node};
    }
        return result;
}

void INSERT_INTO_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DataType type = tree.header.Type;
    //std::cout << "Inserting: " << std::get<int32_t>(entry.key) << "\n";
    int it = 0;
    TraverseResult result = Traverse(file, entry.key, tree.root, tree.pager, type, it);
    assert(result.node.is_leaf);
    insert_into_leaf(result.node, entry, type);

    Page* page = requestPage(file, tree.pager, {fileID, result.page_id});
    assert(page);
    insertNodeIntoPage(result.node, *page, type);
}
void DELETE_FROM_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DataType type = tree.header.Type;
    //std::cout << "Deleting: " << std::get<int32_t>(entry.key) << "\n";
    int it = 0;
    TraverseResult result = Traverse(file, entry.key, tree.root, tree.pager, type, it);
    assert(result.node.is_leaf);
    delete_from_leaf(result.node, entry, type);

    Page* page = requestPage(file, tree.pager, {fileID, result.page_id});
    assert(page);
    insertNodeIntoPage(result.node, *page, type);
}
void UPDATE_FROM_TREE(std::fstream& file, const LeafEntry& entry, BPlusTree& tree) {
    DELETE_FROM_TREE(file, entry, tree);
    INSERT_INTO_TREE(file, entry, tree);
}
std::vector<LeafEntry> SELECT_FROM_TREE(std::fstream& file, BPlusTree& tree, const Conditional conditional, const var key) {
    std::vector<LeafEntry> results;
    int it = 0;
    TraverseResult traverse_result = Traverse(file, key, tree.root, tree.pager, tree.header.Type, it);
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
        while(node.prev_leaf != 0) {
            Page* page = requestPage(file, tree.pager, {fileID, node.prev_leaf});
            assert(page);
            node = {};
            node = deserializeNode(page->buffer, tree.header.Type, page->header.NumRows);
            auto returned_leafs = select_from_leaf(node, conditional, key, tree.header.Type);
            results.insert(results.end(), returned_leafs.begin(), returned_leafs.end());
        }
    }
    return results;
}

int main() {

    const auto type = DataType::INT;

    BTreeNode root;
    root.page_id = FIRST_PAGE_ID;
    root.is_leaf = false;

    InternalEntry internal;
    internal.key = 50;
    internal.left_child = 101;
    internal.right_child = 102;
    insert_into_internal(root, internal, type);
    /*
    std::cout << "AT initialization: \n";
    printNode(root, type);
    std::cout << "---------------\n";
    std::cout << "After serial/deserial:\n";
    Page test_p;
    insertNodeIntoPage(root, test_p, type);
    BTreeNode test = deserializeNode(test_p.buffer, type, root.children.size());
    printNode(test, type);
    std::cout << "---------------\n";
    */

    BTreeNode left;
    left.page_id = 101;
    left.is_leaf = true;
    left.next_leaf = 102;

    BTreeNode right;
    right.page_id = 102;
    right.is_leaf = true;
    right.prev_leaf = 101;

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

    std::fstream file;
    INSERT_INTO_TREE(file, {30, 100, 345}, tree);
    INSERT_INTO_TREE(file, {10, 100, 364}, tree);
    INSERT_INTO_TREE(file, {20, 103, 68}, tree);

    INSERT_INTO_TREE(file, {100, 104, 0}, tree);
    INSERT_INTO_TREE(file, {60, 104, 364}, tree);
    INSERT_INTO_TREE(file, {80, 104, 635}, tree);

    std::vector<LeafEntry> results = SELECT_FROM_TREE(file, tree, Conditional::LESSERorEQUAL, 100);
    std::cout << "-----RESULT-----" << "\n";
    for (auto result: results) {
        printLeaf(result, type);
    }

    std::cout << "Compiles!\n\n";
    return 0;
}

