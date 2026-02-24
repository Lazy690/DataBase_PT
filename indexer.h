
#pragma once 

#include <iostream>
#include <fstream>
#include <variant>
#include <string>      
#include <cstdint>
#include <vector>
#include <filesystem>
using namespace std;
namespace fs = std::filesystem;

class Indexer {

    private:
        
        fs::path id_index_path;
        vector<fs::path> secondary_index_paths;

        template<typename T>
        struct BST_Node {
            T key{};

            // for strings
            uint32_t len = 0; 

            uint32_t offset = 0;
            uint32_t overflow_offset = uint32_t(-1);
            uint32_t left_pointer = uint32_t(-1);
            uint32_t right_pointer = uint32_t(-1);

        };

        struct Overflow_Node;

        bool write_offset_pointers(fstream& file, const uint32_t& offset_rb);
        bool write_overflow_pointer(fstream& file, const uint32_t& offset_rb);

        template<typename T>
        bool append_new_key(fstream& file, const T& item, const uint32_t& offset_rb) {

            file.seekp(0, ios::end);

            if constexpr (std::is_same_v<T, std::string>) {
                //Write key len and value string
                uint32_t len = item.size();
                file.write(reinterpret_cast<const char*>(&len), sizeof(len));
                file.write(item.data(), len);
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                file.write(reinterpret_cast<const char*>(&item), sizeof(item));
            }
            else if constexpr (std::is_same_v<T, double>) {
                file.write(reinterpret_cast<const char*>(&item), sizeof(item));
            }
            else {
                std::cout << "Unknown type\n";
                return false;
            }

            if(!this->write_offset_pointers(file, offset_rb)) {
                cerr << "Failed to save empty pointers." << endl;
            }
            return file.good(); 

        } 

    public:

        Indexer();

        template<typename T>
        bool insert_into_BST(fstream& file, const T& item, const int32_t& offset_rb) {

            //capture end of file
            file.seekg(0, ios::end);
            int end_of_file = file.tellg();
            
            //skip header
            file.seekg(12, ios::beg);
            
            //if no root found
            if (file.tellg() == end_of_file ) {
                cout << "No root found" << endl;
                cout << "Appending to root" << endl;
                if(!this->append_new_key(file, item, offset_rb)) {
                    cerr << "Failed to save root" << endl;
                    return false;
                }
                
                return file.good();
            }
            //reset read and write cursors
            file.seekp(0, ios::beg);
            file.seekg(12, ios::beg);

            int cycle = 0;

            while(true) {

                BST_Node<T> node;

                cout << "Key in node: " << node.key << endl;

                if constexpr (is_same_v<T, std::string>) {
                    
                    file.read(reinterpret_cast<char*>(&node.len), sizeof(node.len));
                    node.key.resize(node.len);
                    file.read(node.key.data(), node.len);
                
                }
                else {

                    file.read(reinterpret_cast<char*>(&node.key), sizeof(node.key));
                    
                }

                T& key = node.key; 
                uint32_t appended_offset = 0;
                
                if(key == item) {

                    while(true) {
                        //jump 4 bytes to overflow pointer and save the pointers location
                        file.seekg(4, ios::cur);
                        int overflow_pointer_location = file.tellg();
                        int32_t overflow_pointer = 0;
                        file.read(reinterpret_cast<char*>(&overflow_pointer), sizeof(overflow_pointer));

                        if(overflow_pointer == -1) {

                            //move write pointer to the end to append, record the new appended offset
                            file.seekp(0, ios::end);
                            
                            appended_offset = file.tellp();
                            if(!this->write_overflow_pointer(file, offset_rb)) {
                                cout << "Failed to append to the dupe pointer" << endl;
                                return false;
                            }

                            //Go back and change -1 to the new location it pointes to
                            file.seekp(overflow_pointer_location, ios::beg);
                            file.write(reinterpret_cast<const char*>(&appended_offset), sizeof(appended_offset));
                            cout << "Found empty dupe pointer and appended it." << endl;
                            return file.good(); 

                        }
                        else if(overflow_pointer > 0) {

                            file.seekg(overflow_pointer, ios::beg);
                            continue;

                        }
                    }

                }
                //If item is lower then key
                else if(key > item) {
                    //jump 8 bytes to left pointer and save the pointers location
                    file.seekg(8, ios::cur);
                    int left_pointer_location = file.tellg();
                    int32_t left_pointer = 0;
                    file.read(reinterpret_cast<char*>(&left_pointer), sizeof(left_pointer));

                    if(left_pointer == -1) {

                        //move write pointer to the end to append, record the new appended offset
                        file.seekp(0, ios::end);
                        
                        appended_offset = file.tellp();
                        if(!this->append_new_key(file, item, offset_rb)) {
                            cout << "Failed to append to the left pointer" << endl;
                            return false;
                        }

                        //Go back and change -1 to the new location it pointes to
                        file.seekp(left_pointer_location, ios::beg);
                        file.write(reinterpret_cast<const char*>(&appended_offset), sizeof(appended_offset));
                        cout << "Found empty left pointer and appended it." << endl;
                        return file.good(); 

                    }
                    else if(left_pointer > 0) {

                        file.seekg(left_pointer, ios::beg);
                        continue;

                    }
                    
                }
                //If item is greater then key
                else if(key < item) {
                    //jump 12 bytes to left pointer and save the pointers location
                    file.seekg(12, ios::cur);
                    int right_pointer_location = file.tellg();
                    int32_t right_pointer = 0;
                    file.read(reinterpret_cast<char*>(&right_pointer), sizeof(right_pointer));

                    if(right_pointer == -1) {

                        //move write pointer to the end to append, record the new appended offset
                        file.seekp(0, ios::end);
                        appended_offset = file.tellp();

                        if(!this->append_new_key(file, item, offset_rb)) {
                            cout << "Failed to append to the right pointer" << endl;
                            return false;
                        }

                        //Go back and change -1 to the new location it pointes to
                        file.seekp(right_pointer_location, ios::beg);
                        file.write(reinterpret_cast<const char*>(&appended_offset), sizeof(appended_offset));
                        cout << "Found empty right pointer and appended it." << endl;
                        return file.good(); 

                    }
                    else if(right_pointer > 0) {

                        file.seekg(right_pointer, ios::beg);
                        continue;

                    }
                    
                }

                cycle++;
                    if(cycle > 2000) {
                        cerr << "Endless BST loop detected." << endl;
                        cerr << "Canseling run" << endl;
                        return false;
                }

            }   
        }

        template<typename T>
        bool fetch_from_BST(fstream& file, const T& item, vector<uint32_t>& result) {
            //capture end of file
            file.seekg(0, ios::end);
            int end_of_file = file.tellg();
        
            //skip header
            file.seekg(12, ios::beg);
            
            //if no root found
            if (file.tellg() == end_of_file ) {
                cout << "Index is empty" << endl;
                return file.good();
            }
        
            int cycle = 0;
        
            //Tree search traversal
            while(true) {
                
                BST_Node<T> node;

                cout << "Key in node: " << node.key << endl;

                if constexpr (is_same_v<T, std::string>) {
                    
                    file.read(reinterpret_cast<char*>(&node.len), sizeof(node.len));
                    node.key.resize(node.len);
                    file.read(node.key.data(), node.len);
                
                }
                else {

                    file.read(reinterpret_cast<char*>(&node.key), sizeof(node.key));
                    
                }

                T& key = node.key; 
                if(key == item) {
        
                    cout << "Match found" << endl;
                    uint32_t overflow_offset = 0;
                    uint32_t record_bank_offset = 0;
        
                    file.read(reinterpret_cast<char*>(&record_bank_offset), sizeof(record_bank_offset ));
        
                    file.read(reinterpret_cast<char*>(&overflow_offset), sizeof(overflow_offset));
                    if (int(overflow_offset) == -1) {
                        result.push_back(record_bank_offset);
                        return file.good();
                    }
                    else if (int(overflow_offset)> 0) {
                      
                        result.push_back(record_bank_offset);
                        while(int(overflow_offset)!= -1) {
                            int32_t duped_offset = 0;
                            file.seekg(overflow_offset, ios::beg);
        
                            file.read(reinterpret_cast<char*>(&duped_offset), sizeof(duped_offset));
                            result.push_back(duped_offset);
        
                            file.read(reinterpret_cast<char*>(&overflow_offset), sizeof(overflow_offset));   
                        }
                        return file.good();
                    }
        
                }
                //When its lesser
                //note that the sign is inverted because strings are like that >:(
                else if(key > item) {
                    //jump 4 bytes to left pointer and save the pointers location
                    file.seekg(8, ios::cur);
                    int32_t left_pointer = 0;
                    file.read(reinterpret_cast<char*>(&left_pointer), sizeof(left_pointer));
                    if(left_pointer == -1) {
        
                        cout << "No match Found." << endl;
                        return file.good();
        
                    }
                    else if(left_pointer > 0) {
        
                        file.seekg(left_pointer, ios::beg);
                        continue;
        
                    }
                }
                //If item is greater then key
                //note that the sign is inverted because strings are like that >:(
                else if(key < item) {
                    //jump 8 bytes to left pointer and save the pointers location
                    file.seekg(12, ios::cur);
                    int32_t right_pointer = 0;
                    file.read(reinterpret_cast<char*>(&right_pointer), sizeof(right_pointer));
        
                    if(right_pointer == -1) {
        
                        cout << "No match Found." << endl;
                        return file.good();
        
                    }
                    else if(right_pointer > 0) {
        
                        file.seekg(right_pointer, ios::beg);
                        continue;
        
                    }
                }
        
                cycle++;
                if(cycle > 2000) {
                    cerr << "Endless BST loop detected." << endl;
                    cerr << "Canceling STR fetch" << endl;
                    return false;
                }
        
            }
        }


        
};     