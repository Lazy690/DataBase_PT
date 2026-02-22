#include "indexer.h"
#include <iostream>
using namespace std;


Indexer::Indexer() {}

bool Indexer::write_offset_pointers(fstream& file, const uint32_t& offset_db) {
    //Save DataBank offset
    file.write(reinterpret_cast<const char*>(&offset_db), sizeof(offset_db));

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
bool Indexer::write_overflow_pointer(fstream& file, const uint32_t& offset_db) {
    //Save DataBank offset
    file.write(reinterpret_cast<const char*>(&offset_db), sizeof(offset_db));

    //Save the overflow pointer for dupe keys:
    uint32_t overflow_pointer = -1;
    file.write(reinterpret_cast<const char*>(&overflow_pointer), sizeof(overflow_pointer));
    return file.good();
}

bool Indexer::append_new_key_str(fstream& file, const string& item, const uint32_t& offset_db) {
    file.seekp(0, ios::end);
    //Write key len and value string
    uint32_t len = item.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(item.data(), len);

    if(!this->write_offset_pointers(file, offset_db)) {
        cerr << "Failed to save empty pointers." << endl;
    }
    return file.good(); 
}
bool Indexer::append_new_key_int(fstream& file, const int32_t& item, const uint32_t& offset_db) {
    file.seekp(0, ios::end);
    //Write key value
    file.write(reinterpret_cast<const char*>(&item), sizeof(item));

    if(!this->write_offset_pointers(file, offset_db)) {
        cerr << "Failed to save empty pointers." << endl;
    }
    return file.good();
}
bool Indexer::append_new_key_double(fstream& file, const double& item, const uint32_t& offset_db) {
    file.seekp(0, ios::end);
    //Write key value
    file.write(reinterpret_cast<const char*>(&item), sizeof(item));

    if(!this->write_offset_pointers(file, offset_db)) {
        cerr << "Failed to save empty pointers." << endl;
    }
    return file.good();
}

//================================================================================
//==========                                                            ==========
//================================================================================

bool Indexer::insert_into_BST_str(fstream& file, const string& item, const int32_t& offset_db) {
    //capture end of file
    file.seekg(0, ios::end);
    int end_of_file = file.tellg();
    
    //skip header
    file.seekg(12, ios::beg);
    
    //if no root found
    if (file.tellg() == end_of_file ) {
        cout << "No root found" << endl;
        cout << "Appending to root" << endl;
        if(!this->append_new_key_str(file, item, offset_db)) {
            cerr << "Failed to save root" << endl;
            return false;
        }
        
        return file.good();
    }
    //reset read and write cursors
    file.seekp(0, ios::beg);
    file.seekg(12, ios::beg);

    int cycle = 0;
    
    //Tree traversal
    while(true) {
        

        uint32_t appended_offset = 0;
        uint32_t key_len = 0;
        string key;
        //read the next key
        file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        key.resize(key_len);
        file.read(key.data(), key_len);

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
                    if(!this->write_overflow_pointer(file, offset_db)) {
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
                if(!this->append_new_key_str(file, item, offset_db)) {
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

                if(!this->append_new_key_str(file, item, offset_db)) {
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
bool Indexer::insert_into_BST_int(fstream& file, const int32_t& item, const int32_t& offset_db) {
    //capture end of file
    file.seekg(0, ios::end);
    int end_of_file = file.tellg();

    //skip header
    file.seekg(12, ios::beg);
    
    //if no root found
    if (file.tellg() == end_of_file ) {
        cout << "No root found" << endl;
        cout << "Appending to root" << endl;
        if(!this->append_new_key_int(file, item, offset_db)) {
            cerr << "Failed to save root" << endl;
            return false;
        }
        
        return file.good();
    }
    //reset read and write cursors
    file.seekp(0, ios::beg);
    file.seekg(12, ios::beg);

    //Tree traversal
    while(true) {
        uint32_t appended_offset = 0;
        int32_t key = 0;
        //read the next key
        file.read(reinterpret_cast<char*>(&key), sizeof(key));

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
                    if(!this->write_overflow_pointer(file, offset_db)) {
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
                if(!this->append_new_key_int(file, item, offset_db)) {
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
            //jump 12 bytes to right pointer and save the pointers location
            file.seekg(12, ios::cur);
            int right_pointer_location = file.tellg();
            int32_t right_pointer = 0;
            file.read(reinterpret_cast<char*>(&right_pointer), sizeof(right_pointer));

            if(right_pointer == -1) {

                //move write pointer to the end to append, record the new appended offset
                file.seekp(0, ios::end);
                appended_offset = file.tellp();

                if(!this->append_new_key_int(file, item, offset_db)) {
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

    }
}

bool Indexer::insert_into_BST_double(fstream& file, const double& item, const int32_t& offset_db) {

    //capture end of file
    file.seekg(0, ios::end);
    int end_of_file = file.tellg();
  
    //skip header
    file.seekg(12, ios::beg);
      
    //if no root found
    if (file.tellg() == end_of_file ) {
        cout << "No root found" << endl;
        cout << "Appending to root" << endl;
        if(!this->append_new_key_double(file, item, offset_db)) {
            cerr << "Failed to save root" << endl;
            return false;
        }
          
        return file.good();
    }
    //reset read and write cursors
    file.seekp(0, ios::beg);
    file.seekg(12, ios::beg);
  
    //Tree traversal
    while(true) {
        uint32_t appended_offset = 0;
        int32_t key = 0;
        //read the next key
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
  
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
                    if(!this->write_overflow_pointer(file, offset_db)) {
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
                if(!this->append_new_key_double(file, item, offset_db)) {
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
            //jump 12 bytes to right pointer and save the pointers location
            file.seekg(12, ios::cur);
            int right_pointer_location = file.tellg();
            int32_t right_pointer = 0;
            file.read(reinterpret_cast<char*>(&right_pointer), sizeof(right_pointer));
  
            if(right_pointer == -1) {
  
                //move write pointer to the end to append, record the new appended offset
                file.seekp(0, ios::end);
                appended_offset = file.tellp();
  
                if(!this->append_new_key_double(file, item, offset_db)) {
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
  
    }

}

bool Indexer::fetch_from_BST_str(fstream& file, const string& item, vector<uint32_t>& result) {
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
        
        int32_t key_len = 0;
        string key;

        //read the next key
        file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        key.resize(key_len);
        file.read(key.data(), key_len);
        
        cout << "Item to Search: " << item << endl; 
        cout << "Extracted item: " << key << endl; 
        //match found:
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
        if(cycle > 10) {
            cerr << "Endless BST loop detected." << endl;
            cerr << "Canceling STR fetch" << endl;
            return false;
        }

    }
}

bool Indexer::fetch_from_BST_int(fstream& file, const int32_t& item, vector<uint32_t>& result) {
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

    //Tree search traversal
    while(true) {
        int32_t key = 0;

        //read the next key
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        //match found:
        if(key == item) {

            uint32_t overflow_offset = 0;
            uint32_t record_bank_offset = 0;

            file.read(reinterpret_cast<char*>(&record_bank_offset), sizeof(record_bank_offset ));

            file.read(reinterpret_cast<char*>(&overflow_offset), sizeof(overflow_offset));
            if (int(overflow_offset)== -1) {
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
        else if(key > item) {
            //jump 8 bytes to left pointer and save the pointers location
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
        else if(key < item) {
            //jump 12 bytes to left pointer and save the pointers location
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

    }
}

bool Indexer::fetch_from_BST_double(fstream& file, const double& item, vector<uint32_t>& result) {
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

    //Tree search traversal
    while(true) {
        double key = 0;

        //read the next key
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        //match found:
        if(key == item) {

            uint32_t overflow_offset = 0;
            uint32_t record_bank_offset = 0;

            file.read(reinterpret_cast<char*>(&record_bank_offset), sizeof(record_bank_offset ));

            file.read(reinterpret_cast<char*>(&overflow_offset), sizeof(overflow_offset));
            if (int(overflow_offset)== -1) {
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
        else if(key > item) {
            //jump 8 bytes to left pointer and save the pointers location
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
        else if(key < item) {
            //jump 12 bytes to left pointer and save the pointers location
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

    }
}

bool Indexer::test(fstream& file, int32_t& item, uint32_t& offset_db) {
    cout << file.tellg() << endl;
    //Write key value
    file.read(reinterpret_cast<char*>(&item), sizeof(item));
    //Save DataBank offset
    file.read(reinterpret_cast<char*>(&offset_db), sizeof(offset_db));
    //Save left and right pointer, its -1 because theres no children yet
    int32_t left_pointer = 0;
    int32_t right_pointer = 0;

    file.read(reinterpret_cast<char*>(&left_pointer), sizeof(left_pointer));
    file.read(reinterpret_cast<char*>(&right_pointer), sizeof(right_pointer));  
    cout << "Right pointer: " << right_pointer << " | Left pointer: " << left_pointer << endl;  
    //file.seekg(0, ios::beg);
    return file.good();
}

void Indexer::test_storage_str() {
    fs::path file = fs::path("test_data") / "test.bin"; 
    {
    fstream in(file, ios::binary | ios::in | ios::out); 
    vector<string> keys {"Xamac", "Plan double Ds", "K-six", "Jordan", "Betty", "1", "K-six", "K-six", "Jordan"};
    vector<uint32_t> offsets = {500, 1000, 1500, 2000, 2500, 3000, 6000, 6500, 7000};
    
    int vector_size = keys.size();
    for (int i = 0; i < vector_size; i++) {
        cout << "Inserting: " << keys[i] << endl;
        if(!insert_into_BST_str(in, keys[i], offsets[i])) {
            cerr << "Failed to append key" << endl;
            return;
        }
    }
    in.close();
    }



    {
    fstream out(file, ios::binary | ios::in | ios::out);
    if(!out) {
        cerr << "Failed to load file" << endl;
        return;
    }
    
    
    vector<string> keys {"Xamac", "Plan double Ds", "K-six", "Jordan", "Betty", "1", "K-six", "K-six", "Jordan"};

    for (auto key: keys) {
        vector<uint32_t> results;
        if(!fetch_from_BST_str(out, key, results)) {
            cout << "Failed to fetch from BST" << endl;
            return;
        }

        for(auto result: results) {
            cout << "The target: " << key << " is in offset: " << result << " of the DataBank." << endl;
        }
    }

    }
}