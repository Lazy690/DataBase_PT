#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <memory>

#include "classes.h"

/* -------------------------------------------------
 * ------     INTERPRETER CLASSES              -----
 * -------------------------------------------------*/

enum class TokenType {
    IDENTIFIER,
    INT,
    DOUBLE,
    STRING,
    OPERATOR,
    END
};

enum class Action {
    
    CREATE,
    DROP,
    INSERT,
    SELECT,
    DELETE, 
    UPDATE 

};

enum class CREATE_TYPE {
    CREATE_DATABASE,
    CREATE_TABLE,
};
enum class DROP_TYPE {
    DROP_DATABASE,
    DROP_TABLE,
};

const std::unordered_map<std::string, Action> ACTION_MAP = {
    {"CREATE", Action::CREATE}, 
    {"DROP",   Action::DROP}, 
    {"INSERT", Action::INSERT}, 
    {"SELECT", Action::SELECT}, 
    {"DELETE", Action::DELETE}, 
    {"UPDATE", Action::UPDATE}
};

const std::unordered_map<std::string, DataType> DATATYPE_MAP = {
    {"INT", DataType::INTEIRO}, 
    {"TEXT", DataType::TEXTO}, 
    {"DOUBLE", DataType::REAL}
};

std::unordered_set<std::string> KeyWords {
      
      "CREATE",
      "DROP",
      "TABLE",
      "DATABASE",
      "IF",
      "NOT",
      "EXISTS",
      "INSERT",
      "SELECT",
      "DELETE",
      "UPDATE",
      "VALUES",
      "FROM",
      "INTO",
      "WHERE",
      "SET",
      "*",
      "UNIQUE",
      "AUTO_INCRIMENT",
      "NOT_NULL",
      "PRIMARY_KEY",
      "FOREIGN_KEY"
      "=",
      ">=",
      "<=",
      ">",
      "<"

};
std::unordered_set<std::string> Comparators {

      "=",
      ">=",
      "<=",
      ">",
      "<"
 
};
std::unordered_set<std::string> constraints_set {
    "UNIQUE",
    "AUTO_INCRIMENT",
    "INDEXED",
    "NOT_NULL",
    "PRIMARY_KEY",
    "FOREIGN_KEY"
};

enum class ConnType {

    AND,
    OR,
    NOT

};

struct Token {
    TokenType type;
    std::string value;
};
struct ValueToken : public Token {
    DataType datatype;
};

struct Comparison {
    Token attribute;
    Token comparator;
    Token value;
};

enum class Constraint {

    UNIQUE,
    AUTO_INCRIMENT,
    NOT_NULL,
    INDEXED,
    PRIMARY_KEY,
    FOREIGN_KEY

};


struct Clause;

struct Connector {
    ConnType type;
    std::unique_ptr<Clause> next;
};

struct Clause {

    bool is_negated = false;
    Comparison comparison;
    std::unique_ptr<Connector> connector;

};

//Linked list that alternates between Clause nodes and Connector nodes
class Where_clause {
    
    private:
        
        std::unique_ptr<Clause> clause_head;
        Clause *clause_tail;

        Connector *connector_hold;

    public:
        enum class NodeType {
            CLAUSE,
            CONNECTOR
        };

        void flip(NodeType& t) {
            if (t == NodeType::CLAUSE) t = NodeType::CONNECTOR;
            else t = NodeType::CLAUSE;
        }

        NodeType tail_type = NodeType::CLAUSE;

        Where_clause() : clause_head(nullptr), 
                         clause_tail(nullptr),
                         connector_hold(nullptr) {}

        void append_clause(const Comparison& c, const bool is_negated) {
            auto temp = std::make_unique<Clause>();
            temp->comparison = c;
            if(is_negated) {
                temp->is_negated = true;
            }
            if (clause_head == nullptr) {
                clause_head = std::move(temp);
                clause_tail = clause_head.get();
                flip(tail_type);
            }
            else {
                connector_hold->next = std::move(temp);
                clause_tail = connector_hold->next.get();
                flip(tail_type);
            }
        }
        void append_connector(const ConnType& t) {
            auto temp = std::make_unique<Connector>();
            temp->type = t;
            
            connector_hold = temp.get();
            clause_tail->connector = std::move(temp);
            flip(tail_type);
        }
        void print_clause() {
            
            NodeType current_type = NodeType::CLAUSE;
            Clause *current_clause = clause_head.get();

            while(true) {
                
                if(current_type == NodeType::CLAUSE) {
                    if(current_clause->is_negated) {
                        std::cout << "NOT ";
                    }
                    std::cout << current_clause->comparison.attribute.value << " " 
                      << current_clause->comparison.comparator.value << " " 
                      << current_clause->comparison.value.value << std::endl;
                    flip(current_type);
                    if(current_clause->connector == nullptr) {
                        break;
                    }

                }
                else if(current_type == NodeType::CONNECTOR) {
                    switch(current_clause->connector->type) {

                        case ConnType::AND:
                            std::cout << "AND" << std::endl;
                            break;
                        case ConnType::OR:
                            std::cout << "OR" << std::endl;
                            break;

                    }
                    
                    current_clause = current_clause->connector->next.get();
                    flip(current_type);
                }
            }
          
        }

};


struct Column_AST {

    Token name;
    DataType type;
    Constraints_list constraints;

};


struct CREATE_AST {

    CREATE_TYPE type;
    bool is_overrite = true;
    Token subject;
    std::vector<Column_AST> columns;  

};

struct DROP_AST {
    
    DROP_TYPE type;
    Token subject;

};

struct INSERT_AST {

    Token table;
    std::vector<Token> attributes;
    std::vector<Token> values;

};

struct SELECT_AST {

    Token table;
    std::vector<Token> attributes;
    std::unique_ptr<Where_clause> where_clauses;

};

struct DELETE_AST {

    Token table;
    std::unique_ptr<Where_clause> where_clauses;
    
};

struct UPDATE_AST {

    Token table;
    std::vector<Comparison> set;
    std::unique_ptr<Where_clause> where_clauses;

};

struct AbstractSyntaxTree {
    Action action;
    std::variant<CREATE_AST, 
                 DROP_AST,
                 INSERT_AST, 
                 SELECT_AST, 
                 DELETE_AST, 
                 UPDATE_AST> tree;
};

bool isInVector(const std::string& value, std::vector<std::string> v) {
    auto it = std::find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}

Action returnAction(std::string a) {
    auto it = ACTION_MAP.find(a);
    if(it == ACTION_MAP.end()) {
        throw std::runtime_error("Action not recognized as a valid command");
    }
    return ACTION_MAP.at(a);
}
DataType returnDataType(std::string t) {
    auto it = DATATYPE_MAP.find(t);
    if(it == DATATYPE_MAP.end()) {
        throw std::runtime_error("DataType not recognized as a valid");
    }
    return DATATYPE_MAP.at(t);
}

std::vector<Token> TOKENIZE(const std::string& input) {
    std::vector<Token> tokens;
    std::string current;

    for (size_t i = 0; i < input.size(); i++) {
        char c = input[i];

        // 1 Skip whitespace
        if (isspace(c)) {
            continue;
        }

          // 2 String literal
          if (c == '\'') {
              current = "";
              i++; // move past opening quote

              while (i < input.size() && input[i] != '\'') {
                  current += input[i];
                  i++;
              }

              tokens.push_back({TokenType::STRING, current});
            continue;
        }

        // 3 Two-character operators
        if ((c == '<' || c == '>' || c == '!' || c == '=') &&
            i + 1 < input.size() && input[i + 1] == '=') {
            
            std::string op;
            op += c;
            op += '=';

            tokens.push_back({TokenType::OPERATOR, op});
            i++; // skip second char
            continue;
        }

        // 4 Single-character operators
        if (c == '<' || c == '>' || c == '=' || c == '+' || c == '-' || c == '*' || c == '(' || c == ')' || c == ',') {
            tokens.push_back({TokenType::OPERATOR, std::string(1, c)});
            continue;
        }

        // 5 Number
        if (isdigit(c)) {
            bool is_double = false;
            current = "";

            while (i < input.size() && (isdigit(input[i]) || input[i] == '.')) {
                current += input[i];
                if(input[i] == '.') is_double = true;
                i++;
            }

            TokenType type;
            if(is_double){
                type = TokenType::DOUBLE;
            }
            else {
                type = TokenType::INT;
            }

            tokens.push_back({type, current});
            i--; // adjust because loop increments
            continue;
        }

        // 6 Identifier
        if (isalpha(c) || c == '_') {
            current = "";

            while (i < input.size() &&
                  (isalnum(input[i]) || input[i] == '_')) {
                current += input[i];
                i++;
            }

            tokens.push_back({TokenType::IDENTIFIER, current});
            i--;
            continue;
        }

        // 7 Unknown character
        throw std::runtime_error("Unknown character detected.");
    }
    

    //for (auto& t : tokens) {
    //    cout << static_cast<int>(t.type) << " : " << t.value << endl;
    //}
    
    tokens.push_back({TokenType::END, ""});
    return tokens;
}           

struct Cursor  {
    int index = 0;
    std::vector<Token> tokens;

    void skip() {
        index++;
        return;
    }
    std::string peek() {
        if(index >= tokens.size()) return "END";
        return tokens[index].value;
    };
    Token consume() {
        index++;
        return tokens[index - 1];
    }
    bool match(const std::string& check) {
        if (tokens[index].value == check) {
            index++;
            return true;
        }
        return false;
    }
    Token expect(const std::string& check) {
        if (tokens[index].value != check) {
            std::cerr << "Expected token: " << check << std::endl;
            throw std::runtime_error("Invalid token");
        }
        return consume();
    }
    bool is_END() {
        if(tokens[index].type == TokenType::END) return true;
        return false;
    }

    int getEndOfParenthesis() {
        for (int i = index; i < tokens.size(); i++) {
            if(tokens[i].value == ")") return i;
        }
        return 0;
    }
    int getEndOfSet() {
        for (int i = index; i < tokens.size(); i++) {
            if(tokens[i].value == "WHERE") return i;
        }
        return 0;
    }
};




bool isKeyWord(const std::string& word) {
    return KeyWords.contains(word);
}
bool isKeyComparator(const std::string& word) {
    return Comparators.contains(word);
}
bool isKeyConstraint(const std::string& word) {
    return constraints_set.contains(word);
}


Comparison return_comparison(Cursor& cursor) {
    Comparison comp;

    if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected attribute in 'WHERE' clause");
    comp.attribute = cursor.consume();
    if(!isKeyComparator(cursor.peek())) {
        std::cout << "Comparator: " << cursor.peek() << "\n";
        throw std::runtime_error("Expected comparator token after attribute declaration in 'WHERE' clause");
    }
    comp.comparator = cursor.consume();
    if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected value after comparator token in 'WHERE' clause");
    comp.value = cursor.consume();

    return comp;
}

std::vector<Token> handle_parenthesis(Cursor& cursor) {
      
      std::vector<Token> values;
      int endof_parenthesis = cursor.getEndOfParenthesis();
      if(endof_parenthesis == 0) throw std::runtime_error("Token '(' was not closed.");
      bool expect_value = true; 
      while(cursor.index != endof_parenthesis) {
          if(expect_value) {
              values.push_back(cursor.consume());
              expect_value = false;
          }
          else {
              if(cursor.peek() != ",") throw std::runtime_error("Expected ',' between values");
              cursor.skip();
              expect_value = true;
          }
      }
      cursor.skip();
      return values;
}

std::vector<Comparison> handle_set(Cursor& cursor) {

    std::vector<Comparison> comps;
    int endof_set = cursor.getEndOfSet();
    if(endof_set == 0) throw std::runtime_error("Expected 'WHERE' clasuse after 'SET'");
    bool expect_value = true; 
    while(cursor.index != endof_set) {
        
        Comparison comp;
        if(expect_value) {
            comp = return_comparison(cursor);
            if(comp.comparator.value != "=") throw std::runtime_error("Expected '=' as comparator in 'SET'");
            comps.push_back(comp);
            expect_value = false;
        }
        else {
            if(cursor.peek() != ",") throw std::runtime_error("Expected ',' between values");
            cursor.skip();
            expect_value = true;
        }
    }
    cursor.skip();
    return comps;

}

std::vector<Column_AST> handle_column_ast(Cursor& cursor) {
    std::vector<Column_AST> cols;
    int endof_parenthesis = cursor.getEndOfParenthesis();
    if(endof_parenthesis == 0) throw std::runtime_error("Token '(' was not closed.");
    bool expect_value = true;
    bool has_autoIncriment = false;

    while(cursor.index != endof_parenthesis) {
        
        Column_AST col;

        if(expect_value) {
            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected column name");
            col.name = cursor.consume();
            col.type = returnDataType(cursor.peek());
            cursor.skip();

            if(cursor.peek() == "," ) {
                expect_value = false;
                cols.push_back(col);
                continue;
            } 

            std::unordered_set<std::string> track_constraints;
            Constraints_list list;
            int count = 1;
            while(cursor.peek() != "," && cursor.index != endof_parenthesis) {
                
                if(!isKeyConstraint(cursor.peek())) throw std::runtime_error("Invalid column Constraint token");
                if(track_constraints.contains(cursor.peek())) throw std::runtime_error("Duplicate Constrate tokens not alowed.");
                

                if(cursor.peek() == "UNIQUE") {
                    list.unique = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    continue;
                }
                else if(cursor.peek() == "NOT_NULL") {
                    list.not_null = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    continue;
                }

                else if(cursor.peek() == "INDEXED") {
                    list.indexed = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    continue;
                }

                else if(cursor.peek() == "AUTO_INCRIMENT") {
                    if(has_autoIncriment) throw ("Only one column may be auto incremented");
                    if(col.type != DataType::INTEIRO) throw ("Non INT types cannot be assigned 'AUTO_INCIMENT' token.");

                    list.auto_incriment = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    has_autoIncriment = true;
                    continue;
                }
                else if(cursor.peek() == "PRIMARY_KEY") {
                    if(col.type != DataType::INTEIRO) throw ("Non INT types cannot be assigned 'PRIMARY_KEY' token.");
                    if(list.foreign_key != false) throw ("Column cannot be 'PRIMARY_KEY' and 'FOREIGN_KEY' at the same time");

                    list.primary_key = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    continue;
                }
                else if(cursor.peek() == "FOREIGN_KEY") {
                    if(col.type != DataType::INTEIRO) throw ("Non INT types cannot be assigned 'FOREIGN_KEY' token.");
                    if(list.primary_key != false) throw ("Column cannot be 'PRIMARY_KEY' and 'FOREIGN_KEY' at the same time");

                    list.primary_key = true;
                    track_constraints.insert(cursor.peek());
                    cursor.skip();
                    continue;
                }
                else {
                    throw std::runtime_error("Invalid constraint token");
                }
          
            }
            
            col.constraints = list;
            cols.push_back(col); 
            expect_value = false;
        }
        else {
            if(cursor.peek() != ",") throw std::runtime_error("Expected ',' between values");
            cursor.skip();
            expect_value = true;
        }
    }
    cursor.skip();
    return cols;
}

std::unique_ptr<Where_clause> handle_where_clauses(Cursor& cursor) {
    
    auto where_clause = std::make_unique<Where_clause>();

    while( cursor.index < cursor.tokens.size() && cursor.tokens[cursor.index].type != TokenType::END ) {
        
        if(where_clause->tail_type == Where_clause::NodeType::CLAUSE) {
            bool is_negated = false;
            if (cursor.match("NOT")) {
                is_negated = true;
            }
            Comparison comp = return_comparison(cursor);
            where_clause->append_clause(comp, is_negated);
        }
        else if(where_clause->tail_type == Where_clause::NodeType::CONNECTOR) {
            ConnType type;

            if (cursor.match("AND")) {
                type = ConnType::AND;
            }
            else if(cursor.match("OR")) {
                type = ConnType::OR;
            }
            else {
                throw std::runtime_error("Expected tokens 'AND' or 'OR' between clauses");
            }
            where_clause->append_connector(type);
        }
    }
    return where_clause;
}

AbstractSyntaxTree PARSE(const std::vector<Token>& tokens) {

    Cursor cursor;
    cursor.tokens = tokens;

    AbstractSyntaxTree AST;
    AST.action = returnAction(cursor.peek());
    cursor.skip();
    
    switch(AST.action) {

        case Action::CREATE:
            {
        
            CREATE_AST create;

            if(cursor.match("DATABASE")) {
                create.type = CREATE_TYPE::CREATE_DATABASE;
            }
            else if(cursor.match("TABLE")) {
                create.type = CREATE_TYPE::CREATE_TABLE;
            }
            else throw std::runtime_error("Expected target specification after 'CREATE' token.");

            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table or database name after token 'CREATE'");
            create.subject = cursor.consume();

            if(create.type == CREATE_TYPE::CREATE_DATABASE) {
                if (!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command"); 
                AST.tree = std::move(create);
                return AST;
            }
            
            if(cursor.match("IF")) {
                if(cursor.match("NOT")) {
                    if(cursor.match("EXISTS")) {
                        create.is_overrite = false;
                    }
                    else throw std::runtime_error("Invalid syntax after 'IF' token");
                    
                }
                else throw std::runtime_error("Invalid syntax after 'IF' token");
            }

            if(cursor.match("(")) {
                create.columns = handle_column_ast(cursor);
            }
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");
            AST.tree = std::move(create);
            }
            break;
            
        case Action::DROP:
            {
            
            DROP_AST drop;

            if(cursor.match("DATABASE")) {
                drop.type = DROP_TYPE::DROP_DATABASE;
            }
            else if(cursor.match("TABLE")) {
                drop.type = DROP_TYPE::DROP_TABLE;
            }

            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table name after token 'DROP'");
            drop.subject = cursor.consume();
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");

            AST.tree = std::move(drop);
            }
            break;
        case Action::INSERT: 
            { 

            INSERT_AST insert;

            cursor.expect("(");
            insert.attributes = handle_parenthesis(cursor);

            cursor.expect("INTO");

            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table name after token 'INTO'");
            insert.table = cursor.consume();

            cursor.expect("VALUES");
            cursor.expect("(");
            insert.values = handle_parenthesis(cursor);
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");
            if(insert.attributes.size() != insert.values.size()) throw std::runtime_error("Attributes and value counts do not corelate");
            AST.tree = std::move(insert);
            }
            break;
        case Action::SELECT: {

            SELECT_AST select;

            if ( cursor.peek() == "*" ) {
                select.attributes.push_back(cursor.consume());
            }
            else {
                cursor.expect("(");
                select.attributes = handle_parenthesis(cursor);
            }

            cursor.expect("FROM");
            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table name after token 'FROM'");
            select.table = cursor.consume();
            
            if ( cursor.match("WHERE") ) {
                select.where_clauses = handle_where_clauses(cursor);
            }                    
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");

            AST.tree = std::move(select);

            }
            break;
            
        case Action::DELETE:
            {

            DELETE_AST delete_;

            cursor.expect("FROM");

            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table name after token 'FROM'");
            delete_.table = cursor.consume();
            
            if ( cursor.match("WHERE") ) {
                delete_.where_clauses = handle_where_clauses(cursor);
            }                    
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");

            AST.tree = std::move(delete_);

            break;
            }

        case Action::UPDATE:
            {
            UPDATE_AST update;

            if(isKeyWord(cursor.peek())) throw std::runtime_error("Expected table name after token 'UPDATE'");
            update.table = cursor.consume();

            cursor.expect("SET");

            update.set = handle_set(cursor);

            update.where_clauses = handle_where_clauses(cursor);
            if(!cursor.is_END()) throw std::runtime_error("Invalid tokens at end of command");
            
            AST.tree = std::move(update);

            break;
            }
    }
    return AST;
}

void print_attributes(std::vector<Token> a) {
            std::cout << "Attributes: " << std::endl;
            for(auto att : a) {
                std::cout << att.value << std::endl;
            }
        }
        void print_set(std::vector<Comparison> s) {
            std::cout << "SET: " << std::endl;
            for(auto comp : s) {
                std::cout << comp.attribute.value << " "
                  << comp.comparator.value << " "
                  << comp.value.value << std::endl;
            }
        }
        void print_values(std::vector<Token> v) {
            std::cout << "Values: " << std::endl;
            for (auto val : v) {
                std::cout << val.value << std::endl; 
            }
        }
        void print_overrite(const bool c) { 
            if(c) {
                std::cout << "Will overrite table" << std::endl;
            }
        }
        void print_create_cols(const std::vector<Column_AST>& c) {
            std::cout << "Columns: " << std::endl;
            std::cout << "-------------------------" << std::endl;
            for(auto col : c) {
                std::cout << "Name: " << col.name.value << std::endl;
                std::cout << "Constraints: " << std::endl;
                if(col.constraints.unique) std::cout << "UNIQUE" << std::endl;
                if(col.constraints.auto_incriment) std::cout << "AUTO_INCRIMENT" << std::endl;
                if(col.constraints.not_null) std::cout << "NOT_NULL" << std::endl;
                if(col.constraints.indexed) std::cout << "INDEXED" << std::endl;
                if(col.constraints.primary_key) std::cout << "PRIMARY_KEY" << std::endl;
                if(col.constraints.foreign_key) std::cout << "FOREIGN_KEY" << std::endl;
                std::cout << "-------------------------" << std::endl;
            }
        }

template<typename T>
void print_AST(const T& ast) {
     
    
    
    if constexpr (std::is_same_v<T, CREATE_AST>) {
        if (ast.type == CREATE_TYPE::CREATE_DATABASE) {
            std::cout << "Creating Database: " << ast.subject.value << "\n";
            return;
        }
        else if (ast.type == CREATE_TYPE::CREATE_TABLE) {
            std::cout << "Creating Table: " << ast.subject.value << "\n";
        }
        print_overrite(ast.is_overrite);
        print_create_cols(ast.columns);
        return;
    }
    if constexpr (std::is_same_v<T, DROP_AST>) {
        if (ast.type == DROP_TYPE::DROP_DATABASE) {
            std::cout << "Dropping Database: " << ast.subject.value << "\n";
        }
        else if (ast.type == DROP_TYPE::DROP_TABLE) {
            std::cout << "Dropping Table: " << ast.subject.value << "\n";
        }
        return;
    }
    if constexpr (std::is_same_v<T, INSERT_AST>) {
        std::cout << "Table: " << ast.table.value << std::endl;
        print_attributes(ast.attributes);
        print_values(ast.values);
        return;
    }
    else if constexpr (std::is_same_v<T, SELECT_AST>) {
        std::cout << "Table: " << ast.table.value << std::endl;
        print_attributes(ast.attributes);
    }
    else if constexpr (std::is_same_v<T, UPDATE_AST>) {
        std::cout << "Table: " << ast.table.value << std::endl;
        print_set(ast.set);
    }
    
    if constexpr (std::is_same_v<T, UPDATE_AST> || std::is_same_v<T, SELECT_AST>) {
        std::cout << "Table: " << ast.table.value << std::endl;
        if(ast.where_clauses == nullptr) return;
        std::cout << "WHERE clauses: " << std::endl;
        ast.where_clauses->print_clause();         
    }
}

bool GENERATE_AST(AbstractSyntaxTree& AST, std::string sql) {
    try {
        std::vector<Token> tokens = TOKENIZE(sql);
        AST = std::move(PARSE(tokens));
    }
    catch (const std::runtime_error& e) {
        std::cerr << "Syntax Error: " << e.what() << "\n";
        return false;
    }
    return true;
}

int test_interpreter(std::string sql) {
    
    AbstractSyntaxTree AST;
    try {
        std::vector<Token> tokens = TOKENIZE(sql);
        AST = PARSE(tokens);
    }
    catch (const std::runtime_error& e) {
        std::cerr << "Syntax Error: " << e.what() << "\n";
        return 1;
    }

    print_AST(std::get<CREATE_AST>(AST.tree));
    std::cout << "compiles!\n";
    return 0;
}
