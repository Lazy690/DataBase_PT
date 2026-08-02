#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include "interpreter.hpp"
#include "src/filesys.hpp"
#include "src/storage.hpp"

DB_Header globalDBHEADER{0x44415641, 3};
TB_Header globalTBHEADER{0x44415441, 4};
RecordHeader globalRBHEADER{0x44415441, 5};
LoggerHeader globalLogHeader{0x44518449, 4};
BeforeImageHeader globalImageHeader{0x75314648, 1};

struct CacheManagement {

    DataBase database;
    Pager pager;
    Logger logger;
    FileManager manager;

};

std::vector<Column> ConvertToColumns(const std::vector<Column_AST>& col_asts) {
    std::vector<Column> results;
    for(auto& col_ast : col_asts) {
        Column column;
        column.type = col_ast.type;
        column.name = col_ast.name.value;
        column.constraints = col_ast.constraints;
        results.push_back(column);
    }
    return results;
}
Row ConvertToRow(const std::vector<Token>& tokens, const std::vector<DataType> types) {
    assert(tokens.size() == types.size());
    Row row;

    for (size_t i = 0; i < types.size(); i++) {
        Entry entry;
        switch(types[i]) {
            case DataType::INTEIRO:
                entry.type  = DataType::INTEIRO;
                entry.value = static_cast<int32_t>(std::stoi(tokens[i].value));
                break;
            case DataType::TEXTO:
                entry.type  = DataType::TEXTO;
                entry.value = tokens[i].value;
                break;
            case DataType::REAL:
                entry.type  = DataType::REAL;
                entry.value = std::stod(tokens[i].value);
                break;
        }
      row.add_entry(entry);
    }

    return row;
}

bool VerifyColumnName(const Row& row, const Constraints_list list) {

    return true;
}
bool VerifyTypeIntegrety(const Row& row, const Constraints_list list) {

    return true;
}

bool EXECUTE(CacheManagement& cache, std::string sql) {
    
    DataBase&     database = cache.database;
    Pager&        pager    = cache.pager;
    Logger&       logger   = cache.logger;
    FileManager&  manager  = cache.manager;

    AbstractSyntaxTree AST;
    if(!GENERATE_AST(AST, sql)) return false;

    switch (AST.action) {
        case Action::CREATE: 
        {
            CREATE_AST tree = std::get<CREATE_AST>(AST.tree);
            if(tree.type == CREATE_TYPE::CREATE_DATABASE) {
                if(!CREATE_DATABASE(tree.subject.value, globalDBHEADER)) {
                    return false;
                }
                else {
                    std::cout << "CREATE DATABASE\n";
                }
            }
            else if(tree.type == CREATE_TYPE::CREATE_TABLE) {
                if(!database.connected) {
                    std::cerr << "Not connected to any database\n";
                    return false;
                }

                std::string table_name = tree.subject.value;
                std::vector<Column> columns = ConvertToColumns(tree.columns);
                assert(!columns.empty());
                bool will_overrite = tree.is_overrite;

                if(!CREATE_TABLE(database, globalTBHEADER, table_name, columns, will_overrite)) {
                    return false;
                }
                else {
                    std::cout << "CREATE TABLE\n";
                }
            }
            break;
        }
        case Action::DROP: 
        {
            DROP_AST tree = std::get<DROP_AST>(AST.tree);
            if(tree.type == DROP_TYPE::DROP_DATABASE) {
                if(database.connected) {
                    std::cerr << "Cannot DROP DATABASE while still connected\n";
                    return false;
                }
                if(!DROP_DATABASE(tree.subject.value)) {
                    return false;
                }
                else {
                    std::cout << "DROP DATABASE\n";
                }
            }
            else if(tree.type == DROP_TYPE::DROP_TABLE) {
                if(!database.connected) {
                    std::cerr << "Not connected to any database\n";
                    return false;
                }
                std::string table_name = tree.subject.value;

                if(!DROP_TABLE(database, table_name)) {
                    return false;
                }
                else {
                    std::cout << "DROP TABLE\n";
                }
            }
            
            break;
        }
        case Action::INSERT: 
        {
            INSERT_AST tree = std::get<INSERT_AST>(AST.tree);
            if(!database.connected) {
                std::cerr << "Cannot INSERT INTO TABLE while not connected to any DATABASE\n";
                return false;
            }
            
            //Temporary: make this into a function
            auto it = database.id_lookup.find(tree.table.value);
            if(it == database.id_lookup.end()) {
                std::cerr << "Table does not Exist\n";
                return false;
            }
            uint32_t tableID = database.id_lookup.at(tree.table.value);

            if(!loadTableFile(manager, database.tables.at(tableID))) {
                return false;
            }

            //temporary
            Row row = ConvertToRow(tree.values, {DataType::INTEIRO, DataType::TEXTO, DataType::REAL});

            if(!INSERT(manager.files.at(tableID), tableID, pager, logger, row)) {
                return false;
            }
            else {
                std::cout << "INSERT TABLE\n";
            }

            break;
        }
        case Action::SELECT: 
        {
            
            break;
        }
        case Action::DELETE: 
        {
            
            break;
        }
        case Action::UPDATE: 
        {
            
            break;
        }
        default:
        break;
    }
    return true;
}

int main() {
    
    CacheManagement cache;

    CONNECTION_STATUS status = CONNECT("Dudes", cache.database, globalDBHEADER, globalTBHEADER);
    if(status == CONNECTION_STATUS::FAILED) {
        std::cout << "Connection failed\n";
        return 1;
    }
    if(status == CONNECTION_STATUS::NOT_EXISTS) {
        std::cout << "Connection failed\n";
        return 1;
    }

    if(!START(cache.logger, globalLogHeader, globalImageHeader)) {
        return 1;
    }
    if(!EXECUTE(cache, "INSERT (id, name, grade) INTO cool_dudes VALUES (1, 'Kirsche', 18.5)")) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
    if(!COMMIT(cache.manager, cache.logger, cache.pager)) {
        return 1;
    }

    std::cout << "Worked\n";
}
