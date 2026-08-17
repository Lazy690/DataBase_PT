#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <optional>
#include "interpreter.hpp"
#include "src/filesys.hpp"
#include "src/storage.hpp"
#include "headers.hpp"

DB_Header globalDBHEADER{0x44415641, 3};
TB_Header globalTBHEADER{0x44415441, 5};
RecordHeader globalRBHEADER{0x44415441, 5};
LoggerHeader globalLogHeader{0x44518449, 4};
BeforeImageHeader globalImageHeader{0x75314648, 1};
IndexHeader globalIndexHeader{0x87564464, 1};

struct CacheManagement {

    DataBase database;
    Pager pager;
    Logger logger;
    FileManager manager;

};

/*
bool (*IntComparison)(int32_t, Conditional, int32_t, bool) = compare;
bool (*StrComparison)(std::string, Conditional, std::string, bool) = compare;
bool (*DubComparison)(double, Conditional, double, bool)  = compare;
*/

template<typename T>
struct QueryFilter {

    T ValueFromQuery;
    Comparison comparison;
    T ValueFromRow;
    bool is_negated = false;

};

using column_name  = std::string;
using column_index = uint32_t;
bool EVALUATE_COMPARISON(ComparisonNode comparison, const std::unordered_map<column_name, column_index>& columns, Row& row) {
    
    auto it = columns.find(comparison.attribute.value);
    assert(it != columns.end());
    uint32_t index = it->second;
    Conditional condition;
    if (comparison.comparator.value == "=") {
        condition = Conditional::EQUAL;
    }
    if (comparison.comparator.value == ">") {
        condition = Conditional::GREATER;
    }
    if (comparison.comparator.value == "<") {
        condition = Conditional::LESSER;
    }
    if (comparison.comparator.value == ">=") {
        condition = Conditional::GREATERorEQUAL;
    }
    if (comparison.comparator.value == "<=") {
        condition = Conditional::LESSERorEQUAL;
    }
    if (comparison.comparator.value == "!=") {
        condition = Conditional::NotEQUAL;
    }
    Entry* entry_to_compare = &row.values[index];
    switch(entry_to_compare->type) {
        case DataType::INT:
            return compare(std::get<int32_t>(entry_to_compare->value), condition, static_cast<int32_t>(std::stoi(comparison.value.value)));
            break;
        case DataType::STRING:
            {
            bool result = compare(std::get<std::string>(entry_to_compare->value), condition, comparison.value.value);
            return result;
            }
            break;
        case DataType::DOUBLE:
            std::cout << "Val from row: " << std::get<double>(entry_to_compare->value);
            std::cout << " " << comparison.comparator.value;
            std::cout << " Val from imput: " << comparison.value.value << "\n";
            {
            bool result = compare(std::get<double>(entry_to_compare->value), condition, std::stod(comparison.value.value));
            return result;
            }
            break;
    }
    return false;
}

bool EnforceColumnIntegrety(const Token& attribute, const Token& value, const Table& table);
bool EVALUATE(Expression* node, const Table& table, Row& row) {
    if (auto* comparison = dynamic_cast<ComparisonNode*>(node)) {
        EnforceColumnIntegrety(comparison->attribute, comparison->value, table);
        return EVALUATE_COMPARISON(*comparison, table.id_lookup, row);
    }
    if (auto* andNode = dynamic_cast<AndNode*>(node)) {
        return EVALUATE(andNode->left.get(), table, row) &&
               EVALUATE(andNode->right.get(), table, row);
    }
    if (auto* orNode = dynamic_cast<OrNode*>(node)) {
        return EVALUATE(orNode->left.get(), table, row) ||
               EVALUATE(orNode->right.get(), table, row);
    }
    if (auto* notNode = dynamic_cast<NotNode*>(node)) {
        return EVALUATE(notNode->next.get(), table, row);
    }
    throw std::runtime_error("Unknown expression node");
}

std::optional<std::vector<ScanResult>> SCAN(std::fstream& file, Table& table, uint32_t tableID, Logger& logger, Pager& pager, Expression* expr = nullptr) {
    assert(file.is_open());
    std::vector<ScanResult> results;
    if(pager.tableMetadata[tableID].PAGECOUNT <= 0) { 
        std::cout << "Table has no pages\n";
        return std::nullopt;
    } 
    for (uint32_t pageID = 0; pageID < pager.tableMetadata[tableID].PAGECOUNT; pageID++) {
        std::vector<ScanResult> ScannedRows;
        if(!ScanRowsFromPage(file, ScannedRows, tableID, pageID, pager)) {
            std::cerr << "Failed to load rows from page\n";
            return std::nullopt;
        }
        for(auto& result : ScannedRows) {
            if(expr == nullptr) {
                results.push_back(result);
                continue;
            }
            try {
                if(EVALUATE(expr, table, result.row)) {
                    results.push_back(result);
                }
            } catch(const std::runtime_error& e) {
                std::cerr << "Integrety Error: " << e.what() << "\n";
                return std::nullopt;
            }
        }
    }
    return results;
}

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
                entry.type  = DataType::INT;
                entry.value = static_cast<int32_t>(std::stoi(tokens[i].value));
                break;
            case TokenType::STRING:
                entry.type  = DataType::STRING;
                entry.value = tokens[i].value;
                break;
            case TokenType::DOUBLE:
                entry.type  = DataType::DOUBLE;
                entry.value = std::stod(tokens[i].value);
                break;
        }
      row.add_entry(entry);
    }

    return row;
}
std::optional <std::vector<Set>> ConvertToSet(const std::vector<Comparison>& comp_asts, const Table& table) {
    std::vector<Set> results;
    for(auto& comp : comp_asts) {
        try {
            EnforceColumnIntegrety(comp.attribute, comp.value, table);
        }
        catch(const std::runtime_error& e) { 
                std::cerr << "Integrety Error: " << e.what() << "\n";
                return std::nullopt;
        }
        Set set;
        assert(comp.comparator.value == "=");
        auto it = table.id_lookup.find(comp.attribute.value);
        assert(it != table.id_lookup.end());
        set.column_index = it->second;
        switch(comp.value.type) {
            case TokenType::INT:
                set.value = static_cast<int32_t>(std::stoi(comp.value.value));
                break;
            case TokenType::STRING:
                set.value = comp.value.value;
                break;
            case TokenType::DOUBLE:
                set.value = std::stod(comp.value.value);
                break;
        }
        results.push_back(set);
    }
    return results;
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
        throw std::runtime_error("Attribute does not exist");
        return false;
    }
    return true;
}
bool VerifyTypeIntegrety(const Token& attribute, const Token& value, const Table& table) {
    //assumes attribute names where Verified
    uint32_t ColumnID = table.id_lookup.at(attribute.value);
    Column column = table.schema.at(ColumnID);

    /*
    if(column.type == DataType::INT) {
        if(value.type != TokenType::INT) {
            throw std::runtime_error("Entry is not an INT\n");
            return false;
        }
    }
    */
    if(column.type == DataType::DOUBLE || column.type == DataType::INT) {
        if(value.type != TokenType::DOUBLE && value.type != TokenType::INT) {
            std::cout << "value: " << value.value << "\n";
            std::cout << "value type: ";
            switch(value.type) {
                case TokenType::STRING:
                    std::cout << "STRING\n";
                    break;
                case TokenType::INT:
                    std::cout << "INT\n";
                    break;
                case TokenType::DOUBLE:
                    std::cout << "DOUBLE\n";
                    break;
            }
            throw std::runtime_error("Entry is not a NUMBER\n");
            return false;
        }
    }
    else if(column.type == DataType::STRING) {
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
        if(attributes.size() != values.size()) {
            throw std::runtime_error("Column count and Values count do not corelate");
        }
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
bool EnforceColumnIntegrety(const Token& attribute, const Token& value, const Table& table) {
        if(!VerifyColumnName(attribute, table)) return false;
        if(!VerifyTypeIntegrety(attribute, value, table)) return false;
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

ResultSet ReturnResultSet(const std::vector<ScanResult>& rows, const std::vector<Token>& attributes, const Table& table) {
    //Assumes columns are sorted and verified
    ResultSet set;
    if(attributes[0].value == "*") {
        for(auto& row : rows) {
            Result result;
            for(auto& entry : row.row.values) {
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
                result.values.push_back(row.row.values[column_index].value);
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
                //printDataBase(database);
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
                if(will_overrite) {
                    std::cout << "TABLE EXISTS\n";
                    return true;
                }

                if(!CREATE_TABLE(database, globalTBHEADER, globalRBHEADER, table_name, columns, will_overrite)) {
                    return false;
                }
                else {
                    COMMIT_DATABASE_DATA(database, globalTBHEADER, globalDBHEADER, globalRBHEADER);
                    std::cout << "CREATE TABLE\n";
                }
            }
            else if(tree.type == CREATE_TYPE::CREATE_INDEX) {
                if(!database.connected) {
                    std::cerr << "Not connected to any database\n";
                    return false;
                }
                auto it = database.id_lookup.find(tree.subject.value);
                if(it == database.id_lookup.end()) {
                    std::cerr << "Table does not Exist\n";
                    return false;
                }
                uint32_t tableID = database.id_lookup.at(tree.subject.value);

                try {
                    if(!VerifyColumnName(tree.attribute, database.tables.at(tableID))) {
                        std::cerr << "Column does not Exist\n";
                        return false;
                    }
                }
                catch (std::runtime_error& e) {
                    std::cerr << "Integrety error: " << e.what() << "\n";
                    return false;
                }
                uint32_t column_index = database.tables.at(tableID).id_lookup.at(tree.attribute.value);
                if (!CREATE_INDEX(database, globalIndexHeader, column_index, tableID)) {
                    return false;
                }
                else {
                    std::cout << "CREATE INDEX ON " << tree.attribute.value << "\n";
                    return true;
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
                auto it = pager.tableMetadata.find(database.id_lookup.at(table_name));
                if(it != pager.tableMetadata.end()) {
                    pager.tableMetadata.erase(database.id_lookup.at(table_name));
                }

                if(!DROP_TABLE(database, table_name)) {
                    return false;
                }
                std::cout << "DROP TABLE\n";
            }

            break;
        }
        case Action::INSERT: 
        {
            INSERT_AST tree = std::move(std::get<INSERT_AST>(AST.tree));
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

            auto it2 = pager.tableMetadata.find(tableID);
            if(it2 == pager.tableMetadata.end()) {
                if(!loadTableFile(manager, database.tables.at(tableID))) {
                    return false;
                }
                if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                    return false;
                }
            }

            INSERT_DATA* current = tree.root.get();
            
            while(current != nullptr) {

                std::vector<Token> values = std::move(current->tokens);
                try {
                    if(!EnforceColumnIntegrety(tree.attributes, values, database.tables.at(tableID))) {
                        return false;
                    }
                    sortRowTokens(values, tree.attributes, database.tables.at(tableID));
                    if(!EnforceIntegretyList(manager.files.at(tableID), pager, database.tables.at(tableID), tree.attributes, values)) {
                        return false;
                    }
                    if(!handleAutoInciment(database.tables.at(tableID), tree.attributes, values)) {
                        return false;
                    }
                } catch(const std::runtime_error& e) {
                    std::cerr << "Integrety Error: " << e.what() << "\n";
                    return false;
                }
                
                Row row = ConvertToRow(values);
                
                //Temporary
                //printRow(row);

                if(logger.transaction.start) {
                    if(!INSERT(manager.files.at(tableID), tableID, pager, logger, row)) {
                        return false;
                    }
                    INSERT_DATA* hold = current->next.get();
                    current = hold;
                } 
                else {
                    if(!INSERT(manager.files.at(tableID), tableID, pager, row)) {
                        return false;
                    }
                    INSERT_DATA* hold = current->next.get();
                    current = hold;
                }
            }

            std::cout << "INSERT TABLE\n";

            break;
        }
        case Action::SELECT: 
        {
            SELECT_AST tree = std::move(std::get<SELECT_AST>(AST.tree));
            if(!database.connected) {
                std::cerr << "Cannot SELECT FROM TABLE while not connected to any DATABASE\n";
                return false;
            }
            auto its = database.id_lookup.find(tree.table.value);
            if(its == database.id_lookup.end()) {
                std::cerr << "Table not found\n";
                return false;
            }
            uint32_t tableID = database.id_lookup.at(tree.table.value);
            std::cout << "Never mind\n";

            auto it = pager.tableMetadata.find(tableID);
            if(it == pager.tableMetadata.end()) {
                if(!loadTableFile(manager, database.tables.at(tableID))) {
                    return false;
                }
                if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                    return false;
                }
            }
            try {
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
            } catch(const std::runtime_error& e) {
                std::cerr << "Integrety Error: " << e.what() << "\n";
                return false;
            }

            Expression* expr = tree.WHERE_ROOT.get();
            auto scannedResults = SCAN(manager.files.at(tableID), database.tables.at(tableID), tableID, logger, pager, expr);
            if(!scannedResults) {
                return false;
            }
            /*
            std::cout << "-------RESULTS---------\n";
            for (auto result: *scannedResults) {
                printRow(result.row);
            }
            */
            if (scannedResults){
                resultSet = std::move(ReturnResultSet(*scannedResults, tree.attributes, database.tables.at(tableID)));
                std::cout << "SELECT FROM TABLE\n";
            }

            break;
        }
        case Action::DELETE: 
        {
            
            DELETE_AST tree = std::move(std::get<DELETE_AST>(AST.tree));
            if(!database.connected) {
                std::cerr << "Cannot DELETE FROM TABLE while not connected to any DATABASE\n";
                return false;
            }
            auto its = database.id_lookup.find(tree.table.value);
            if(its == database.id_lookup.end()) {
                std::cerr << "Table not found\n";
                return false;
            }
            uint32_t tableID = database.id_lookup.at(tree.table.value);

            auto it = pager.tableMetadata.find(tableID);
            if(it == pager.tableMetadata.end()) {
                if(!loadTableFile(manager, database.tables.at(tableID))) {
                    return false;
                }
                if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                    return false;
                }
            }
            Expression* expr = tree.WHERE_ROOT.get();
            auto scannedResults = SCAN(manager.files.at(tableID), database.tables.at(tableID), tableID, logger, pager, expr);
            if(!scannedResults) {
                return false;
            }
            if(logger.transaction.start) {
                if(!DELETE(manager.files.at(tableID), *scannedResults, tableID, pager, logger)) {
                    return false;
                }
            }
            else {
                if(!DELETE(manager.files.at(tableID), *scannedResults, tableID, pager)) {
                    return false;
                }
            }
            std::cout << "DELETE FROM TABLE\n";
            break;
        }
        case Action::UPDATE: 
        {
            UPDATE_AST tree = std::move(std::get<UPDATE_AST>(AST.tree));
            if(!database.connected) {
                std::cerr << "Cannot UPDATE FROM TABLE while not connected to any DATABASE\n";
                return false;
            }
            auto its = database.id_lookup.find(tree.table.value);
            if(its == database.id_lookup.end()) {
                std::cerr << "Table not found\n";
                return false;
            }
            uint32_t tableID = database.id_lookup.at(tree.table.value);

            auto it = pager.tableMetadata.find(tableID);
            if(it == pager.tableMetadata.end()) {
                if(!loadTableFile(manager, database.tables.at(tableID))) {
                    return false;
                }
                if(!loadTableMetadata(manager, pager, globalRBHEADER, database.tables.at(tableID))) {
                    return false;
                }
            }
            Expression* expr = tree.WHERE_ROOT.get();
            auto scannedResults = SCAN(manager.files.at(tableID), database.tables.at(tableID), tableID, logger, pager, expr);
            if(!scannedResults) {
                return false;
            }
            auto set = ConvertToSet(tree.set, database.tables.at(tableID));
            if(!set) {
                return false;
            }
            if(logger.transaction.start) {
                if(!UPDATE(manager.files.at(tableID), *scannedResults, tableID, pager, logger, *set)) {
                    return false;
                }
            }
            else {
                if(!UPDATE(manager.files.at(tableID), *scannedResults, tableID, pager, *set)) {
                    return false;
                }
            }
            std::cout << "UPDATE TABLE\n";
            
            break;
        }
        default:
        break;
    }
    return true;
}

int mainnn() {
    
    CacheManagement cache;
    ResultSet result;

    if(!EXECUTE(cache, "CREATE DATABASE Dudes;", result)) {
        return 1;
    }

    if(!EXECUTE(cache, "CONNECT Dudes;", result)) {
        return 1;
    }

    if(!EXECUTE(cache, "CREATE TABLE cool_dudes IF NOT EXISTS (id INT UNIQUE PRIMARY_KEY AUTO_INCRIMENT, name TEXT INDEXED NOT_NULL, grade DOUBLE);", result)) {
        return 1;
    }
    if(!START(cache.database, cache.manager, cache.logger, globalLogHeader, globalImageHeader)) {
        return 1;
    }
    if(!EXECUTE(cache, "INSERT (name, grade) INTO cool_dudes VALUES (('Alice', 95.0), ('Bob', 78.0), ('Charlie', 92.0), ('Diana', 85.0), ('Eve', 88.0), ('Frank', 72.0), ('Grace', 91.0), ('Henry', 76.0), ('Iris', 89.0), ('Jack', 84.0));", result)) {
        return 1;
    }
    if(!EXECUTE(cache, "INSERT (name, grade) INTO cool_dudes VALUES ('Kirche', 69.0);", result)) {
        return 1;
    }
    //std::cout << "Crashing now\n";
    //return 0;
    
    if(!EXECUTE(cache, "UPDATE cool_dudes SET name = 'Yohann the femboy', grade = 6.7 WHERE name = 'Kirche';", result)) {
        return 1;
    }
    if(!EXECUTE(cache, "SELECT * FROM cool_dudes WHERE name = 'Yohann the femboy';", result)) {
        return 1;
    }

    //assert(result.values.size() == 0);
    if(result.values.size() > 0) {
        std::cout << "---------RESULTS---------\n";
        for (size_t i = 0; i < result.values.size(); i++) {
            for (size_t j = 0; j < result.values[i].values.size(); j++) {
                if(j != 0) std::cout << ", ";
                std::visit([](const auto& x) {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, int32_t>) {
                        std::cout << x;
                    }
                    else if constexpr (std::is_same_v<T, std::string>) {
                        std::cout << x;
                    }
                    else if constexpr (std::is_same_v<T, double>) {
                        std::cout << x;
                    }
                }, result.values[i].values[j]);
            }
            std::cout << "\n";
        }


    }
    if(!COMMIT(cache.database, cache.manager, &cache.logger, cache.pager, globalTBHEADER, globalDBHEADER, globalRBHEADER)) {
        return 1;
    }

    std::cout << "Worked\n";
}
