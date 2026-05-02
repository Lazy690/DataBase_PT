
#include <iostream>
#include <cassert>
#include <algorithm>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <cstddef> 

namespace fs = std::filesystem;

struct DB_Header {
    uint32_t MAGIC;
    uint32_t VERSION;
};  

std::fstream file("DB_metadata.bin", std::ios::binary | std::ios::in | std::ios::out);
DB_Header DBHEADER{0x44415641, 1};

bool create_DB_metadata(fs::path DBfolder, DB_Header header, std::string dbName) {
    
    std::fstream file((fs::path(DBfolder) / "DBHeaderMeta.bin"), std::ios::binary | std::ios::out);  
    if(!file) {
        std::cerr << "Failed to create header" << std::endl;
        return false;
    }
    file.write(reinterpret_cast<const char*>(&header), sizeof(DB_Header));
    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }
    uint32_t len = dbName.size();
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(dbName.data(), len);
    if(!file) {
        std::cerr << "Failed to save header" << std::endl;
        return false;
    }

    return true;
}

bool validate_Database_header(std::fstream& file, std::string dbName) {
    DB_Header this_Header;
    std::string name_from_file;
    uint32_t name_len = 0;

    file.read(reinterpret_cast<char*>(&this_Header), sizeof(DB_Header));

    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name_from_file.resize(name_len);
    file.read(name_from_file.data(), name_len);

    if(!file) {
        std::cerr << "Failed to load database metadata" << std::endl;
        return false;
    }
    if(this_Header.MAGIC != DBHEADER.MAGIC) {
        std::cerr << "Database has invalid MAGIC" << std::endl;
        return false;
    }
    if(this_Header.VERSION != DBHEADER.VERSION) {
        std::cerr << "Database has invalid file VERSION" << std::endl;
        return false;
    }
    if(name_from_file != dbName) {
        std::cerr << "Database name is not the same as its header file" << std::endl;
        return false;
    }
    return true;
}

struct connection_files {
    std::fstream DB_metadata;
};
 //This function throws error
connection_files openFiles(fs::path db_path, std::string dbName) {
    connection_files files;
    files.DB_metadata.open((fs::path(db_path) / "DBHeaderMeta.bin"), std::ios::binary | std::ios::in | std::ios::out);
    if(!files.DB_metadata) {
        throw std::runtime_error("Failed to find or open DBHeaderMeta file");
    }
    if(!validate_Database_header(files.DB_metadata, dbName)) {
        throw std::runtime_error("Invalid Header");
    }

    return files;
}

bool CREATE_DATABASE(std::string input_DBname) {
    std::string dbName = input_DBname + "_DB";
    
    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;

    if(!fs::exists(db_path)) {
        fs::create_directory(db_path);
        create_DB_metadata(db_path, DBHEADER, input_DBname);
    }
    return true;
}

bool DROP_DATABASE(std::string input_DBname) {
    std::string dbName = input_DBname + "_DB";

    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;
    
    if(!fs::exists(db_path)) {
        std::cerr << "Database does not Exist" << std::endl;
        return false;
    }
    fs::remove_all(db_path);
    return true;
}

//this function throws errors
connection_files CONNECT(std::string input_DBname) {
  
    connection_files files;

    std::string dbName = input_DBname + "_DB";
    fs::path db_path = "..";
    db_path /= ".."; 
    db_path /= dbName;
    if(!fs::exists(db_path)) {
        throw std::runtime_error("Database does not exists");
    }

    try {
        files = std::move(openFiles(db_path, input_DBname));
    }
    catch (std::runtime_error e) {
        throw std::runtime_error(e.what());
    }

    return files;
}

int main() {
    std::string name = "WorkSpace";
    if(!CREATE_DATABASE(name)) {
        std::cerr << "CREATE DATABASE command failed to execute." << std::endl;
        return 1;
    }

    connection_files files;
    try {
        files = std::move(CONNECT(name));
    }
    catch (std::runtime_error e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    if(!DROP_DATABASE(name)) { 
        std::cerr << "DROP DATABASE command failed to execute." << std::endl;
        return 1;
    }
    return 0; 
}

