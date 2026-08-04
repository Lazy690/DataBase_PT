#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include "interpreter.hpp"
#include "src/filesys.hpp"
#include "src/storage.hpp"

DB_Header globalDBHEADER{0x44415641, 3};
TB_Header globalTBHEADER{0x44415441, 5};
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
Row ConvertToRow(const std::vector<Token>& tokens) {
    //assumes its sorted
    Row row;

    for (size_t i = 0; i < tokens.size(); i++) {
        Entry entry;
        switch(tokens[i].type) {
            case TokenType::INT:
                entry.type  = DataType::INTEIRO;
                entry.value = static_cast<int32_t>(std::stoi(tokens[i].value));
                break;
            case TokenType::STRING:
                entry.type  = DataType::TEXTO;
                entry.value = tokens[i].value;
                break;
            case TokenType::DOUBLE:
                entry.type  = DataType::REAL;
                entry.value = std::stod(tokens[i].value);
                break;
        }
      row.add_entry(entry);
    }

    return row;
}

void sortRowTokens(std::vector<Token>& entries, std::vector<Token>& attributes, const Table& table) {
    assert(entries.size()==attributes.size());
    std::vector<Token> sortedEntries;
    std::vector<Token> sortedAttributes;

    for (uint32_t i = 0; i < table.schema.size(); i++) {
        std::cout << "Iteration: " << i << "\n";
        uint32_t index = 0;
        for(auto& attribute : attributes) {
            std::cout << "Index: " << index << "\n";
            if(attribute.value == table.schema.at(i).name) {
                std::cout << "Found: " << attribute.value << "/" << table.schema.at(i).name << "\n";
                sortedAttributes.push_back(attribute);
                sortedEntries.push_back(entries[index]);
                break;
            }
            ++index;
        }
    }
    attributes = sortedAttributes;
    entries = sortedEntries;
}

bool VerifyColumnName(const Token& attribute, const Table& table) {
    auto it = table.id_lookup.find(attribute.value);
    if(it == table.id_lookup.end()) {
        std::cerr << "attribute: " << attribute.value << " Does not Exist\n";
        return false;
    }
    return true;
}
bool VerifyTypeIntegrety(const Token& attribute, const Token& value, const Table& table) {
    //assumes attribute names where Verified
    uint32_t ColumnID = table.id_lookup.at(attribute.value);
    Column column = table.schema.at(ColumnID);

    if(column.type == DataType::INTEIRO) {
        if(value.type != TokenType::INT) {
            throw std::runtime_error("Entry is not an INT\n");
            return false;
        }
    }
    else if(column.type == DataType::REAL) {
        if(value.type != TokenType::DOUBLE) {
            throw std::runtime_error("Entry is not a DOUBLE\n");
            return false;
        }
    }
    else if(column.type == DataType::TEXTO) {
        if(value.type != TokenType::STRING) {
            throw std::runtime_error("Entry is not of DataType TEXT\n");
            return false;
        }
    }
 
    return true;
}
bool VerifyUniqueness(std::fstream& file, uint32_t tableID, Pager& pager, size_t column_index, const std::variant<int32_t, std::string, double> value) {
    if (!ScanUniqueness(file, tableID, pager, column_index, value)) {
        throw std::runtime_error("Value is not UNIQUE");
    }
    return true;
}
bool EnforceColumnIntegrety(const std::vector<Token>& attributes, const std::vector<Token> values, const Table& table) {
    try {
        for(size_t i = 0; i < attributes.size(); i++) {
            if(!VerifyColumnName(attributes[i], table)) return false;
            if(!VerifyTypeIntegrety(attributes[i], values[i], table)) return false;
        }
    } catch(const std::runtime_error& e) {
        std::cerr << "Integrety Error: " << e.what() << "\n";
        return false;
    }
    return true;
}
bool EnforceIntegretyList(std::fstream& file, Pager& pager, const Table& table, 
                          std::vector<Token> attributes, std::vector<Token>& values) {
    try {
        
        for (uint32_t column_index : table.NotNullColumns) {
            Column column = table.schema.at(column_index);
            bool found = false;
            for (auto& attribute : attributes) {
                if (column.name == attribute.value) {
                    found = true;
                    break;
                }
            }
            if(!found) {
                throw std::runtime_error("Entry must not be Null");
            }
        }
        for(uint32_t i = 0; i < attributes.size(); i++) {

            uint32_t column_index = table.id_lookup.at(attributes[i].value);
            Column column = table.schema.at(column_index);

            if(column.constraints.unique) {
                switch(values[i].type) {
                    case::TokenType::INT:
                        {
                            int val = std::stoi(values[i].value);
                            if(!VerifyUniqueness(file, table.header.ID, pager, column_index, static_cast<int32_t>(val))) return false;
                        }
                        break;
                    case::TokenType::STRING:
                        if(!VerifyUniqueness(file, table.header.ID, pager, column_index, values[i].value)) return false;
                        break;
                    case::TokenType::DOUBLE:
                        if(!VerifyUniqueness(file, table.header.ID, pager, column_index, std::stod(values[i].value))) return false;
                        break;
                }
            }

        }
    } catch(const std::runtime_error& e) {
        std::cerr << "Integrety Error: " << e.what() << "\n";
        return false;
    }
    return true;
}

bool handleAutoInciment(Table& table, std::vector<Token>& attributes, std::vector<Token>& values) {
    //assumes rows entries are sorted
    if(!table.autoIncrimentedColumnIDptr) {
        std::cout << "No autoIncrimentedColumn\n";
        return true;

    }
    std::string autoIncrimented_name = table.schema.at(*table.autoIncrimentedColumnIDptr).name;

    for (size_t i = 0; i < attributes.size(); i++) {
        if (attributes[i].value == autoIncrimented_name) {
            if(std::stoi(values[i].value) > table.header.LatestAutoIncriment) {
                table.header.LatestAutoIncriment = std::stoi(values[i].value);
            }
            return true;
        }
    }

    int32_t incriment = ++table.header.LatestAutoIncriment;
    uint32_t column_index = *table.autoIncrimentedColumnIDptr;

    Token token{TokenType::INT, std::to_string(incriment)};
    values.insert(values.begin() + column_index, token);
    assert(values.size() != (values.size() - 1) );
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
            
            printDataBase(database);
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
            if(!EnforceColumnIntegrety(tree.attributes, tree.values, database.tables.at(tableID))) {
                return false;
            }

            if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                return false;
            }

            sortRowTokens(tree.values, tree.attributes, database.tables.at(tableID));

            std::cout << "its this:\n";
            if(!EnforceIntegretyList(manager.files.at(tableID), pager, database.tables.at(tableID), tree.attributes, tree.values)) {
                return false;
            }
            std::cout << "never mind\n";
            if(!handleAutoInciment(database.tables.at(tableID), tree.attributes, tree.values)) {
                return false;
            }

            Row row = ConvertToRow(tree.values);
            
            //Temporary
            printRow(row);

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

    /*
    if(!EXECUTE(cache, "DROP DATABASE Dudes")) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
    */

    if(!EXECUTE(cache, "CREATE DATABASE Dudes")) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);

    CONNECTION_STATUS status = CONNECT("Dudes", cache.database, globalDBHEADER, globalTBHEADER);
    if(status == CONNECTION_STATUS::FAILED) {
        std::cout << "Connection failed\n";
        return 1;
    }
    if(status == CONNECTION_STATUS::NOT_EXISTS) {
        std::cout << "Database does not Exist\n";
        return 1;
    }
    if(!EXECUTE(cache, "CREATE TABLE cool_dudes IF NOT EXISTS (id INT UNIQUE PRIMARY_KEY AUTO_INCRIMENT, name TEXT UNIQUE INDEXED NOT_NULL, grade DOUBLE)")) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);

    if(!START(cache.logger, globalLogHeader, globalImageHeader)) {
        return 1;
    }
    if(!EXECUTE(cache, "INSERT (grade, name) INTO cool_dudes VALUES (10.3, 'alleb')")) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
    if(!COMMIT(cache.manager, cache.logger, cache.pager)) {
        return 1;
    }

    std::cout << "Worked\n";
}
