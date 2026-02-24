#include "indexer.h"
#include <iostream>
#include <type_traits>
using namespace std;



Indexer::Indexer() {}

struct Indexer::Overflow_Node {

};

bool Indexer::write_offset_pointers(fstream& file, const uint32_t& offset_rb) {
    //Save DataBank offset
    file.write(reinterpret_cast<const char*>(&offset_rb), sizeof(offset_rb));

    //Save the overflow pointer for dupe keys:
    uint32_t overflow_pointer = -1;
    file.write(reinterpret_cast<const char*>(&overflow_pointer), sizeof(overflow_pointer));

    //Save left and right pointer, its -1 because theres no children yet
    int32_t left_pointer = -1;
    int32_t right_pointer = -1;

    file.write(reinterpret_cast<const char*>(&left_pointer), sizeof(left_pointer));
    file.write(reinterpret_cast<const char*>(&right_pointer), sizeof(right_pointer));    
    file.seekp(0, ios::beg);
    return file.good();
}
bool Indexer::write_overflow_pointer(fstream& file, const uint32_t& offset_rb) {
    //Save DataBank offset
    file.write(reinterpret_cast<const char*>(&offset_rb), sizeof(offset_rb));

    //Save the overflow pointer for dupe keys:
    uint32_t overflow_pointer = -1;
    file.write(reinterpret_cast<const char*>(&overflow_pointer), sizeof(overflow_pointer));
    return file.good();
}