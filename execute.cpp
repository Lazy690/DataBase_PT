#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include "interpreter.hpp"
#include "src/filesys.hpp"

DB_Header globalDBHEADER{0x44415641, 3};
TB_Header globalTBHEADER{0x44415441, 4};
RecordHeader globalRBHEADER{0x44415441, 5};

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

bool EXECUTE(DataBase& database, std::string sql) {
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
    
    DataBase database;
    if(!EXECUTE(database, "CREATE DATABASE 'Dudes'")) {
        return 1;
    }

    CONNECTION_STATUS status = CONNECT("Dudes", database, globalDBHEADER, globalTBHEADER);
    if(status == CONNECTION_STATUS::FAILED) {
        std::cout << "Connection failed\n";
        return 1;
    }
    if(status == CONNECTION_STATUS::NOT_EXISTS) {
        std::cout << "Connection failed\n";
        return 1;
    }


    if(!EXECUTE(database, "CREATE TABLE cool_dudes IF NOT EXISTS (id INT UNIQUE AUTO_INCRIMENT PRIMARY_KEY, name TEXT UNIQUE INDEXED, grade DOUBLE)")) {
        return 1;
    }
    if(!EXECUTE(database, "CREATE TABLE lame_dudes IF NOT EXISTS (id INT UNIQUE AUTO_INCRIMENT PRIMARY_KEY, name TEXT UNIQUE INDEXED, grade DOUBLE)")) {
        return 1;
    }
    /*
    if(!EXECUTE(database, "DROP TABLE lame_dudes")) {
        return 1;
    }
    */

    COMMIT_DATABASE_DATA(database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
    printDataBase(database);
    std::cout << "Worked\n";
}
