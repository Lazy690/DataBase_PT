#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <variant>
#include <memory>

#include <classes.h>
using namespace std;

vector<string> KeyWords {
      
      "CREATE",
      "DROP",
      "TABLE",
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

};
vector<string> Comparators {
      
      "=",
      ">=",
      "<=",
      ">",
      "<"
  
};

bool isInVector(const string& value, vector<string> v) {
    auto it = find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}

Action returnAction(string a) {
    map<string, Action> m = {{"CREATE", Action::CREATE}, {"DROP", Action::DROP}, {"INSERT", Action::INSERT}, {"SELECT", Action::SELECT}, {"DELETE", Action::DELETE}, {"UPDATE", Action::UPDATE}};
    try
    {
        return m.at(a);
    }
    catch(const out_of_range& ex)
    {
        throw runtime_error("Action not recognized as a valid command");
    }
}
DataType returnDataType(string t) {
    map<string, DataType> m = {{"INT", DataType::INT}, {"TEXT", DataType::TEXT}, {"DOUBLE", DataType::DOUBLE}};
    try
    {
        return m.at(t);
    }
    catch(const out_of_range& ex)
    {
        throw runtime_error("Invalied data type token in column initialization");
    }
    
}

class Lexer {
    public:
        vector<Token> run(const string& input) {
            vector<Token> tokens;
            string current;

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
                    
                    string op;
                    op += c;
                    op += '=';

                    tokens.push_back({TokenType::OPERATOR, op});
                    i++; // skip second char
                    continue;
                }

                // 4 Single-character operators
                if (c == '<' || c == '>' || c == '=' || c == '+' || c == '-' || c == '*' || c == '(' || c == ')' || c == ',') {
                    tokens.push_back({TokenType::OPERATOR, string(1, c)});
                    continue;
                }

                // 5 Number
                if (isdigit(c)) {
                    current = "";

                    while (i < input.size() && (isdigit(input[i]) || input[i] == '.')) {
                        current += input[i];
                        i++;
                    }

                    tokens.push_back({TokenType::NUMBER, current});
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
                throw runtime_error("Unknown character detected.");
            }
           

            //for (auto& t : tokens) {
            //    cout << static_cast<int>(t.type) << " : " << t.value << endl;
            //}
            
            tokens.push_back({TokenType::END, ""});
            return tokens;
        }           
};

class Parser {

    private:
        int cursor = 0;
        vector<Token> tokens;
        
        void skip() {
            cursor++;
            return;
        }
        string peek() {
            if(cursor >= tokens.size()) return "END";
            return tokens[cursor].value;
        };
        Token consume() {
            cursor++;
            return tokens[cursor - 1];
        };
        bool match(const string& check) {
            if (tokens[cursor].value == check) {
                cursor++;
                return true;
            }
            return false;
        };
        Token expect(const string& check) {
            if (tokens[cursor].value != check) {
                cerr << "Expected token: " << check << endl;
                throw runtime_error("Invalid token");
            }
            return consume();
        };
        bool is_END() {
            if(tokens[cursor].type == TokenType::END) return true;
            return false;
        }



        bool isKeyWord(const string& word) {
            return isInVector(word, KeyWords);
        }
        bool isKeyComparator(const string& word) {
            return isInVector(word, Comparators);
        }
        bool isKeyConstraint(const string& word) {
            return isInVector(word, constraints_vec);
        }
        int getEndOfParenthesis() {
            for (int i = cursor; i < tokens.size(); i++) {
                if(tokens[i].value == ")") return i;
            }
            return 0;
        }
        int getEndOfSet() {
            for (int i = cursor; i < tokens.size(); i++) {
                if(tokens[i].value == "WHERE") return i;
            }
            return 0;
        }



        Comparison return_comparison() {
            Comparison comp;

            if(isKeyWord(peek())) throw runtime_error("Expected attribute in 'WHERE' clause");
            comp.attribute = consume();
            if(!isKeyComparator(peek())) throw runtime_error("Expected comparator token after attribute declaration in 'WHERE' clause");
            comp.comparator = consume();
            if(isKeyWord(peek())) throw runtime_error("Expected value after comparator token in 'WHERE' clause");
            comp.value = consume();

            return comp;
        }

        vector<Token> handle_parenthesis() {
              
              vector<Token> values;
              int endof_parenthesis = getEndOfParenthesis();
              if(endof_parenthesis == 0) throw runtime_error("Token '(' was not closed.");
              bool expect_value = true; 
              while(cursor != endof_parenthesis) {
                  if(expect_value) {
                      values.push_back(consume());
                      expect_value = false;
                  }
                  else {
                      if(peek() != ",") throw runtime_error("Expected ',' between values");
                    skip();
                      expect_value = true;
                  }
              }
              skip();
              return values;
        }

        vector<Comparison> handle_set() {

            vector<Comparison> comps;
            int endof_set = getEndOfSet();
            if(endof_set == 0) throw runtime_error("Expected 'WHERE' clasuse after 'SET'");
            bool expect_value = true; 
            while(cursor != endof_set) {
                
                Comparison comp;
                if(expect_value) {
                    comp = return_comparison();
                    if(comp.comparator.value != "=") throw runtime_error("Expected '=' as comparator in 'SET'");
                    comps.push_back(comp);
                    expect_value = false;
                }
                else {
                    if(peek() != ",") throw runtime_error("Expected ',' between values");
                    skip();
                    expect_value = true;
                }
            }
            skip();
            return comps;


        }

        vector<Column_AST> handle_column_ast() {
            vector<Column_AST> cols;
            int endof_parenthesis = getEndOfParenthesis();
            if(endof_parenthesis == 0) throw runtime_error("Token '(' was not closed.");
            bool expect_value = true;

            while(cursor != endof_parenthesis) {
                
                Column_AST col;

                if(expect_value) {
                    if(isKeyWord(peek())) throw runtime_error("Expected column name");
                    col.name = consume();
                    col.type = returnDataType(peek());
                    skip();

                    if(peek() == "," ) {
                        expect_value = false;
                        cols.push_back(col);
                        continue;
                    } 

                    vector<string> track_constraints;
                    Constraints_list list;
                    int count = 1;
                    while(peek() != "," && cursor != endof_parenthesis) {
                        
                        if(!isKeyConstraint(peek())) throw runtime_error("Invalid column Constraint token");
                        if(isInVector(peek(), track_constraints)) throw runtime_error("Duplicate Constrate tokens not alowed.");
                        

                        if(peek() == "UNIQUE") {
                            list.unique = true;
                            track_constraints.push_back(peek());
                            skip();
                            continue;
                        }
                        else if(peek() == "NOT_NULL") {
                            list.not_null = true;
                            track_constraints.push_back(peek());
                            skip();
                            continue;
                        }

                        else if(peek() == "AUTO_INCRIMENT") {
                            if(col.type != DataType::INT) throw ("Non INT types cannot be assigned 'AUTO_INCIMENT' token.");

                            list.auto_incriment = true;
                            track_constraints.push_back(peek());
                            skip();
                            continue;
                        }
                        else if(peek() == "PRIMARY_KEY") {
                            if(col.type != DataType::INT) throw ("Non INT types cannot be assigned 'PRIMARY_KEY' token.");
                            if(list.foreign_key != false) throw ("Column cannot be 'PRIMARY_KEY' and 'FOREIGN_KEY' at the same time");

                            list.primary_key = true;
                            track_constraints.push_back(peek());
                            skip();
                            continue;
                        }
                        else if(peek() == "FOREIGN_KEY") {
                            if(col.type != DataType::INT) throw ("Non INT types cannot be assigned 'FOREIGN_KEY' token.");
                            if(list.primary_key != false) throw ("Column cannot be 'PRIMARY_KEY' and 'FOREIGN_KEY' at the same time");

                            list.primary_key = true;
                            track_constraints.push_back(peek());
                            skip();
                            continue;
                        }
                        else {
                            throw runtime_error("Invalid constraint token");
                        }
                  
                    }
                    
                    col.constraints = list;
                    cols.push_back(col); 
                    expect_value = false;
                }
                else {
                    if(peek() != ",") throw runtime_error("Expected ',' between values");
                  skip();
                    expect_value = true;
                }
            }
            skip();
            return cols;
        }

        unique_ptr<Where_clause> handle_where_clauses() {
            
            auto where_clause = make_unique<Where_clause>();

            while( cursor < tokens.size() && tokens[cursor].type != TokenType::END ) {
                
                if(where_clause->tail_type == Where_clause::NodeType::CLAUSE) {
                    bool is_negated = false;
                    if (match("NOT")) {
                        is_negated = true;
                    }
                    Comparison comp = return_comparison();
                    where_clause->append_clause(comp, is_negated);
                }
                else if(where_clause->tail_type == Where_clause::NodeType::CONNECTOR) {
                    ConnType type;

                    if (match("AND")) {
                        type = ConnType::AND;
                    }
                    else if(match("OR")) {
                        type = ConnType::OR;
                    }
                    else {
                        throw runtime_error("Expected tokens 'AND' or 'OR' between clauses");
                    }
                    skip();
                    where_clause->append_connector(type);
                }
            }
            return where_clause;
        }

        
        
    public:
    
        explicit Parser(const vector<Token>& t) {
            
            tokens = t;

        }
    
        AST run() {
    
            AST ast;
            Action action = returnAction(peek());
            skip();
            
            switch(action) {

                case Action::CREATE:
                    {
                
                    CREATE_AST create;

                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'CREATE'");
                    create.table = consume();
                    
                    if(match("IF")) {
                        if(match("NOT")) {
                            if(match("EXISTS")) {
                                create.is_overrite = false;
                            }
                            else throw runtime_error("Invalid syntax after 'IF' token");
                            
                        }
                        else throw runtime_error("Invalid syntax after 'IF' token");
                    }

                    if(match("(")) {
                        create.columns = handle_column_ast();
                    }
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");
                    ast = create;
                    }
                    break;
                    
                case Action::DROP:
                    {
                    
                    DROP_AST drop;

                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'DROP'");
                    drop.table = consume();
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");

                    ast = drop;
                    }
                    break;
                case Action::INSERT: 
                    { 

                    INSERT_AST insert;
                    expect("INTO");

                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'INTO'");
                    insert.table = consume();

                    expect("(");
                    insert.attributes = handle_parenthesis();

                    expect("VALUES");
                    expect("(");
                    insert.values = handle_parenthesis();
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");
                    ast = insert;
                    }
                    break;
                case Action::SELECT: {

                    SELECT_AST select;

                    if ( peek() == "*" ) {
                        select.attributes.push_back(consume());
                    }
                    else {
                        expect("(");
                        select.attributes = handle_parenthesis();
                    }

                    expect("FROM");
                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'FROM'");
                    select.table = consume();
                    
                    if ( match("WHERE") ) {
                        select.where_clauses = handle_where_clauses();
                    }                    
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");

                    ast = move(select);

                    }
                    break;
                    
                case Action::DELETE:
                    {

                    DELETE_AST delete_;

                    expect("FROM");

                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'FROM'");
                    delete_.table = consume();
                    
                    if ( match("WHERE") ) {
                        delete_.where_clauses = handle_where_clauses();
                    }                    
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");

                    ast = move(delete_);

                    break;
                    }
        
                case Action::UPDATE:
                    {
                    UPDATE_AST update;

                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'UPDATE'");
                    update.table = consume();

                    expect("SET");

                    update.set = handle_set();

                    update.where_clauses = handle_where_clauses();
                    if(!is_END()) throw runtime_error("Invalid tokens at end of command");
                    
                    ast = move(update);
      
                    break;
                    }
            }
            return ast;
        }
           
        template<typename T>
        void print_AST(const T& ast) {
            
            
            cout << "Table: " << ast.table.value << endl;
            
            if constexpr (is_same_v<T, CREATE_AST>) {
                print_overrite(ast.is_overrite);
                print_create_cols(ast.columns);
                return;
            }
            if constexpr (is_same_v<T, DROP_AST>) {
                cout << "Table dropped." << endl;
                return;
            }
            if constexpr (is_same_v<T, INSERT_AST>) {
                print_attributes(ast.attributes);
                print_values(ast.values);
                return;
            }
            else if constexpr (is_same_v<T, SELECT_AST>) {
                print_attributes(ast.attibutes);
            }
            else if constexpr (is_same_v<T, UPDATE_AST>) {
                print_set(ast.set);
            }
            
            if constexpr (is_same_v<T, UPDATE_AST> || is_same_v<T, SELECT_AST>) {
                if(ast.where_clauses == nullptr) return;
                cout << "WHERE clauses: " << endl;
                ast.where_clauses->print_clause();         
            }
        }
};

AST Interpreter::run(const string& input) {
    try {
        Lexer lexer;
        Parser parser(lexer.run(input));
        AST ast = parser.run();
        return ast;
    } catch (const runtime_error& e)
    {
        cerr << "Syntax Error: " << e.what() << endl;
        throw;
    } 
};

void print_attributes(vector<Token> a) {
            cout << "Attributes: " << endl;
            for(auto att : a) {
                cout << att.value << endl;
            }
        }
        void print_set(vector<Comparison> s) {
            cout << "SET: " << endl;
            for(auto comp : s) {
                cout << comp.attribute.value << " "
                  << comp.comparator.value << " "
                  << comp.value.value << endl;
            }
        }
        void print_values(vector<Token> v) {
            cout << "Values: " << endl;
            for (auto val : v) {
                cout << val.value << endl; 
            }
        }
        void print_overrite(const bool c) { 
            if(c) {
                cout << "Will overrite table" << endl;
            }
        }
        void print_create_cols(const vector<Column_AST>& c) {
            cout << "Columns: " << endl;
            cout << "-------------------------" << endl;
            for(auto col : c) {
                cout << "Name: " << col.name.value << endl;
                cout << "Constraints: " << endl;
                if(col.constraints.unique) cout << "UNIQUE" << endl;
                if(col.constraints.auto_incriment) cout << "AUTO_INCRIMENT" << endl;
                if(col.constraints.not_null) cout << "NOT_NULL" << endl;
                if(col.constraints.primary_key) cout << "PRIMARY_KEY" << endl;
                if(col.constraints.foreign_key) cout << "FOREIGN_KEY" << endl;
                cout << "-------------------------" << endl;
            }
        }

template<typename T>
void print_AST(const T& ast) {
     
    cout << "Table: " << ast.table.value << endl;
    
    if constexpr (is_same_v<T, CREATE_AST>) {
        print_overrite(ast.is_overrite);
        print_create_cols(ast.columns);
        return;
    }
    if constexpr (is_same_v<T, DROP_AST>) {
        cout << "Table dropped." << endl;
        return;
    }
    if constexpr (is_same_v<T, INSERT_AST>) {
        print_attributes(ast.attributes);
        print_values(ast.values);
        return;
    }
    else if constexpr (is_same_v<T, SELECT_AST>) {
        print_attributes(ast.attibutes);
    }
    else if constexpr (is_same_v<T, UPDATE_AST>) {
        print_set(ast.set);
    }
    
    if constexpr (is_same_v<T, UPDATE_AST> || is_same_v<T, SELECT_AST>) {
        if(ast.where_clauses == nullptr) return;
        cout << "WHERE clauses: " << endl;
        ast.where_clauses->print_clause();         
    }
}

void test_interpreter(const string& input) {
    try {
        Interpreter interpreter;
        auto ast = interpreter.run(input);
        print_AST(get<CREATE_AST>(ast));
    }
    catch (const runtime_error& e) {
        cerr << "Syntax Error: " << e.what() << endl;
        return;
    }
}
