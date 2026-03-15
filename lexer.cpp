#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <variant>
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

struct comparison {
    Token attribute;
    Token comparator;
    Token value;
};

struct Where_clause;

struct Connector {
    ConnType type;
    Where_clause* next;
};

struct Where_clause {
    
    comparison expression;
    Connector* connector;
    
};

struct INSERT_AST {

    Token table;
    vector<Token> attributes;
    vector<Token> values;

};

struct SELECT_AST {

    string table;
    vector<Token> attributes;
    Where_clause clause;

};

struct DELETE_AST {

    string table;
    Where_clause clause;
    
};

struct UPDATE_AST {

    string table;
    vector<comparison> set;
    Where_clause clause;

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

        comparison return_comparison() {
            comparison comp;
            if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'INTO'");
            comp.attribute = consume();

        }

        Where_clause handle_where_clauses() {
            
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
                                cout << peek() << endl;

                    expect("VALUES");
                    expect("(");
                    insert.values = handle_parenthesis();
                    command.AST = insert;
                    }
                    break;
                case Action::SELECT: {

                    SELECT_AST select;

                    if ( peek() == "*" ) {
                        select.attributes.push_back(comsume());
                    }
                    else {
                        expect("(");
                        select.attributes = handle_parenthesis(); 
                    }

                    expect("FROM");
                    if(isKeyWord(peek())) throw runtime_error("Expected table name after token 'FROM'");
                    insert.table = consume();
                    
                    if ( match("WHERE") ) {
                        
                    }                    

                    }
                    break;
                    
                case Action::DELETE:
                    break;
                    
        
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


};

int main() {
    string input = "INSERT INTO dudes (name, age, grade) VALUES ('Kirche', 28, 12.5)"; 
    
    try {
        COMMAND command;
        Parser parser(input);
        command = parser.parse();
        parser.execute_in(get<INSERT_AST>(command.AST));
    }
    catch (const runtime_error& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
}
