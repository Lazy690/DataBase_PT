#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <variant>
#include <memory>
using namespace std;

void insert(string table, string value) {cout << "Inserting " << value << " Into " << table << endl;}
void select(string table, string value) {cout << "Selecting: " << value << " From " << table << endl;}
void delete_(string table, string value) {cout << "Deleting From table: " << table << endl;}
void update(string table,string value) {cout << "Updating: " << value << " From " << table << endl;}


map<string, void(*)(string, string)> action_map = {{"INSERT", &insert}, {"SELECT", &select}, {"DELETE", &delete_}, {"UPDATE", &update}};

vector<string> KeyWords {

      "INSERT",
      "SELECT",
      "DELETE",
      "UPDATE",
      "VALUES",
      "FROM",
      "INTO",
      "WHERE",
      "SET",
      "*"

};
vector<string> Comparators {
      
      "=",
      ">=",
      "<=",
      ">",
      "<"
  
};

bool isInVector(string value, vector<string> v) {
    auto it = find(v.begin(), v.end(), value);
    if(it == v.end()) return false;
    return true;
}

enum class TokenType {
    IDENTIFIER,
    NUMBER,
    STRING,
    OPERATOR,
    END
};

enum class Action {
    INSERT,
    SELECT,
    DELETE,
    UPDATE
};

enum class ConnType {

    AND,
    OR,
    NOT

};

struct Token {
    TokenType type;
    string value;
};

struct Comparison {
    Token attribute;
    Token comparator;
    Token value;
};

struct Clause;

struct Connector {
    ConnType type;
    unique_ptr<Clause> next;
};

struct Clause {
    
    bool is_negated = false;
    Comparison comparison;
    unique_ptr<Connector> connector;
    
};

//Linked list that alternates between Clause nodes and Connector nodes
class Where_clause {
    
    private:
        
        unique_ptr<Clause> clause_head;
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
            auto temp = make_unique<Clause>();
            temp->comparison = c;
            if(is_negated) {
                temp->is_negated = true;
            }
            if (clause_head == nullptr) {
                clause_head = move(temp);
                clause_tail = clause_head.get();
                flip(tail_type);
            }
            else {
                connector_hold->next = move(temp);
                clause_tail = connector_hold->next.get();
                flip(tail_type);
            }
        }
        void append_connector(const ConnType& t) {
            auto temp = make_unique<Connector>();
            temp->type = t;
            
            connector_hold = temp.get();
            clause_tail->connector = move(temp);
            flip(tail_type);
        }
        void print_clause() {
            
            NodeType current_type = NodeType::CLAUSE;
            Clause *current_clause = clause_head.get();

            while(true) {
                
                if(current_type == NodeType::CLAUSE) {
                    if(current_clause->is_negated) {
                        cout << "NOT ";
                    }
                    cout << current_clause->comparison.attribute.value << " " 
                      << current_clause->comparison.comparator.value << " " 
                      << current_clause->comparison.value.value << endl;
                    flip(current_type);
                    if(current_clause->connector == nullptr) {
                        break;
                    }

                }
                else if(current_type == NodeType::CONNECTOR) {
                    switch(current_clause->connector->type) {

                        case ConnType::AND:
                            cout << "AND" << endl;
                            break;
                        case ConnType::OR:
                            cout << "OR" << endl;
                            break;

                    }
                    
                    current_clause = current_clause->connector->next.get();
                    flip(current_type);
                }
            }
          
        }

};

struct INSERT_AST {

    Token table;
    vector<Token> attributes;
    vector<Token> values;

};

struct SELECT_AST {

    Token table;
    vector<Token> attributes;
    unique_ptr<Where_clause> where_clauses;

};

struct DELETE_AST {

    Token table;
    unique_ptr<Where_clause> where_clauses;
    
};

struct UPDATE_AST {

    Token table;
    vector<Comparison> set;
    unique_ptr<Where_clause> where_clauses;

};

struct COMMAND{
    
    Action ACTION;
    variant<INSERT_AST, SELECT_AST, DELETE_AST, UPDATE_AST> AST;

};

Action returnAction(string a) {
    map<string, Action> m = {{"INSERT", Action::INSERT}, {"SELECT", Action::SELECT}, {"DELETE", Action::DELETE}, {"UPDATE", Action::UPDATE}};
    return m[a];
}

string collect_set(int& cursor, vector<Token> tokens)  {
      
      string set;    
      while(tokens[cursor].value != "WHERE") {
          set += tokens[cursor].value;
          set += " ";
          cursor++;
      } 
      return set;
}
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
                throw runtime_error("Invalid syntax");
            }
            return consume();
        };



        bool isKeyWord(const string& word) {
            return isInVector(word, KeyWords);
        }
        bool isKeyComparator(const string& word) {
            return isInVector(word, Comparators);
        }
        int getEndOfParenthesis() {
            for (int i = cursor; i < tokens.size(); i++) {
                if(tokens[i].value == ")") return i;
            }
            return 0;
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

        vector<Token> tokenize(const string& input) {
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
           

            for (auto& t : tokens) {
                cout << static_cast<int>(t.type) << " : " << t.value << endl;
            }
            
            tokens.push_back({TokenType::END, ""});
            return tokens;
        }    
    public:
    
        Parser(string input) {

            tokens = tokenize(input);

        }
    
        COMMAND parse() {
    
            COMMAND command;
            cout << peek() << endl;
            command.ACTION = returnAction(peek());
            skip();
            
            switch(command.ACTION) {
                
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
                    command.AST = insert;
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

                    command.AST = move(select);

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

                    command.AST = move(delete_);


                    break;
                    }
        
                case Action::UPDATE:
                    break;

            }

            return command;
        }
        void execute(COMMAND& comm) {
            cout << "Attributes: " << endl;
            if (holds_alternative<INSERT_AST>(comm.AST)) {
                for(auto att : get<INSERT_AST>(comm.AST).attributes) {
                    cout << att.value << endl;
                }
                cout << "Values: " << endl;
                for (auto val : get<INSERT_AST>(comm.AST).values) {
                    cout << val.value << endl; 
                }
            }
        }
        void execute_in(INSERT_AST& conn) {
            cout << "Attributes: " << endl;
            for(auto att : conn.attributes) {
                cout << att.value << endl;
            }
            cout << "Values: " << endl;
            for (auto val : conn.values) {
                cout << val.value << endl; 
            }
        }
        void execute_se(SELECT_AST& conn) {
            cout << "Table: " << conn.table.value << endl;
            cout << "Attributes: " << endl;
            for(auto att : conn.attributes) {
                cout << att.value << endl;
            }
            if(conn.where_clauses == nullptr) return;
            cout << "WHERE clauses: " << endl;
            conn.where_clauses->print_clause();         
        }
        void execute_de(DELETE_AST& conn) {
            cout << "Table: " << conn.table.value << endl;
            if(conn.where_clauses == nullptr) return;
            cout << "WHERE clauses: " << endl;
            conn.where_clauses->print_clause();         
        }
      

};

int main() {
    string input = "DELETE FROM dudes "; 
    
    try {
        COMMAND command;
        Parser parser(input);
        command = parser.parse();
        parser.execute_de(get<DELETE_AST>(command.AST));
    }
    catch (const runtime_error& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
}
