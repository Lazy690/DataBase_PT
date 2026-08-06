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

bool (*IntComparison)(int32_t, Conditional, int32_t, bool) = compare;
bool (*StrComparison)(std::string, Conditional, std::string, bool) = compare;
bool (*DubComparison)(double, Conditional, double, bool)  = compare;

template<typename T>
struct QueryFilter {

    T ValueFromQuery;
    Comparison comparison;
    T ValueFromRow;
    bool is_negated = false;

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

void sortAttributeTokens(std::vector<Token>& attributes, const Table& table) {
    std::vector<Token> sortedAttributes;

    for (uint32_t i = 0; i < table.schema.size(); i++) {
        uint32_t index = 0;
        for(auto& attribute : attributes) {
            if(attribute.value == table.schema.at(i).name) {
                sortedAttributes.push_back(attribute);
                break;
            }
            ++index;
        }
    }
    attributes = sortedAttributes;
}
void sortRowTokens(std::vector<Token>& entries, std::vector<Token>& attributes, const Table& table) {
    std::vector<Token> sortedEntries;
    std::vector<Token> sortedAttributes;

    for (uint32_t i = 0; i < table.schema.size(); i++) {
        uint32_t index = 0;
        for(auto& attribute : attributes) {
            if(attribute.value == table.schema.at(i).name) {
                sortedEntries.push_back(entries[index]);
                sortedAttributes.push_back(attributes[index]);
                break;
            }
            ++index;
        }
    }
    entries = sortedEntries;
    attributes = sortedAttributes;
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

struct Result {
    std::vector<std::variant<int32_t, std::string, double>> values;
    template<typename T>
    T get(size_t index) {
        if constexpr (std::is_same_v<T, int32_t>) {
            return std::get<int32_t>(values[index]);
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            return std::get<std::string>(values[index]);
        }
        else if constexpr (std::is_same_v<T, double>) {
            return std::get<double>(values[index]);
        }
    }
};
struct ResultSet {
    std::vector<Result> values;
};

ResultSet ReturnResultSet(const std::vector<Row>& rows, const std::vector<Token>& attributes, const Table& table) {
    //Assumes columns are sorted and verified
    ResultSet set;
    if(attributes[0].value == "*") {
        for(auto& row : rows) {
            Result result;
            for(auto& entry : row.values) {
                result.values.push_back(entry.value);
            }
            set.values.push_back(result);
        }
        return set;
    }
    else {
        for (auto& row : rows) {
            Result result;
            for (auto& attribute : attributes) {
                uint32_t column_index = table.id_lookup.at(attribute.value);
                result.values.push_back(row.values[column_index].value);
            }
            set.values.push_back(result);
        }
        return set;
    }
}


bool EXECUTE(CacheManagement& cache, std::string sql, ResultSet& resultSet) {
    
    DataBase&     database = cache.database;
    Pager&        pager    = cache.pager;
    Logger&       logger   = cache.logger;
    FileManager&  manager  = cache.manager;

    AbstractSyntaxTree AST;
    if(!GENERATE_AST(AST, sql)) return false;

    switch (AST.action) {
        case Action::CONNECT:
        {
            CONNECT_AST tree = std::get<CONNECT_AST>(AST.tree);
            if(database.connected) {
                std::cerr << "cannot connect to database while already connected to one.\n";
                return false;
            }
            database = {};
            
            CONNECTION_STATUS status = CONNECT(tree.database.value, cache.database, globalDBHEADER, globalTBHEADER);
            if(status == CONNECTION_STATUS::FAILED) {
                std::cerr << "Connection failed\n";
                return false;
            }
            if(status == CONNECTION_STATUS::NOT_EXISTS) {
                std::cerr << "Database does not Exist\n";
                return false;
            }
            else if(status == CONNECTION_STATUS::CONNECTED) {
                std::cout << "CONNECT DATABASE.\n";
                return true;
            }
        }
        case Action::DISCONNECT:
        {
            DISCONNECT_AST tree = std::get<DISCONNECT_AST>(AST.tree);
            if(!database.connected) {
                std::cerr << "cannot disconnect from database when not connected to any.\n";
                return false;
            }
            if(database.name != tree.database.value) {
                std::cerr << "Not connected to this database\n";
                return false;
            }
            else {
                database = {};
                std::cout << "DISCONNECT DATABASE\n";
                return true;
            }
        }
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
            
            //printDataBase(database);
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
            if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                return false;
            }
            if(!EnforceColumnIntegrety(tree.attributes, tree.values, database.tables.at(tableID))) {
                return false;
            }

            sortRowTokens(tree.values, tree.attributes, database.tables.at(tableID));

            if(!EnforceIntegretyList(manager.files.at(tableID), pager, database.tables.at(tableID), tree.attributes, tree.values)) {
                return false;
            }
            if(!handleAutoInciment(database.tables.at(tableID), tree.attributes, tree.values)) {
                return false;
            }

            Row row = ConvertToRow(tree.values);
            
            //Temporary
            //printRow(row);

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
            SELECT_AST tree = std::move(std::get<SELECT_AST>(AST.tree));
            if(!database.connected) {
                std::cerr << "Cannot SELECT FROM TABLE while not connected to any DATABASE\n";
                return false;
            }
            uint32_t tableID = database.id_lookup.at(tree.table.value);

            if(!loadTableFile(manager, database.tables.at(tableID))) {
                return false;
            }
            if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                return false;
            }
            
            if(tree.attributes[0].value != "*") {
                for (auto& attribute : tree.attributes) {
                    if(!VerifyColumnName(attribute, database.tables.at(tableID))) {
                        std::cerr << "Column does not Exist\n";
                        return false;
                    }
                }
                sortAttributeTokens(tree.attributes, database.tables.at(tableID));
            }
            else assert(tree.attributes.size() == 1);

            std::vector<Row> queryResult;
            std::cout << "Loaded metadata pagecount: " << pager.tableMetadata[tableID].PAGECOUNT << "\n";
            if(!SELECT(manager.files.at(tableID), queryResult, tableID, logger, pager)) {
                return false;
            }
            else {
                resultSet = std::move(ReturnResultSet(queryResult, tree.attributes, database.tables.at(tableID)));
                std::cout << "SELECT FROM TABLE\n";
            }

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
    ResultSet result;

    if(!EXECUTE(cache, "CREATE DATABASE Dudes;", result)) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);

    if(!EXECUTE(cache, "CONNECT Dudes;", result)) {
        return 1;
    }

    if(!EXECUTE(cache, "CREATE TABLE cool_dudes IF NOT EXISTS (id INT UNIQUE PRIMARY_KEY AUTO_INCRIMENT, name TEXT UNIQUE INDEXED NOT_NULL, grade DOUBLE);", result)) {
        return 1;
    }
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);

    if(!START(cache.logger, globalLogHeader, globalImageHeader)) {
        return 1;
    }
    if(!EXECUTE(cache, "INSERT (grade, name) INTO cool_dudes VALUES (6.7, 'Very Cool Dude');", result)) {
        return 1;
    }
    if(!EXECUTE(cache, "SELECT * FROM cool_dudes;", result)) {
        return 1;
    }

    for(auto& result : result.values) {
        
        int id = result.get<int>(0);
        std::string name = result.get<std::string>(1);
        double grade = result.get<double>(2);
        std::cout << "------------------\n";
        std::cout << " id: " << id << "\n name: " << name << "\n grade: " << grade << "\n";
    }
    std::cout << "------------------\n";
    COMMIT_DATABASE_DATA(cache.database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
    if(!COMMIT(cache.manager, cache.logger, cache.pager)) {
        return 1;
    }

    std::cout << "Worked\n";
}
