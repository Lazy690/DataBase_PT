#include <iostream>
#include <fstream>
#include <variant>
#include <string>      
#include <cstdint>
#include <vector>
#include <map>
#include <filesystem>
using namespace std;
namespace fs = std::filesystem;
    
struct Node {
    bool isleaf;
    vector<map<T, int>> keys;
    vector<Node*> children;
    Node* next;
    Node(bool leaf = false) : isLeaf(leaf), next(nullptr) {} 
};



