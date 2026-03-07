#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
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
      "SET"

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

    AND;
    OR;
    NOT;

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

struct Connector {
    ConnType type;
    where_clause* next = nullptr;
};

struct Where_clause{
    
    comparison expression;
    Connector* connector = nullptr;
    
};

struct INSERT_AST {

    string table;
    vector<Token> attributes
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
    INSERT_AST INSERT;
    SELECT_AST SELECT;
    DELETE_AST DELETE;
    UPDATE_AST UPDATE;
    
};

Action returnAction(string a) {
    map<string, Action> m = {{"INSERT", Action::INSERT}, {"SELECT", Action::SELECT}, {"DELETE", Action::DELETE}, {"UPDATE", Action::UPDATE}};
    return m[a];
}


Where_clause collect_clauses(int& cursor, vector<Token>& tokens) {

      Where_clause clause;
      if(isInVector(tokens[cursor].value, KeyWords)) throw runtime_error("Invalid token Type");
      clause.left_item = tokens[cursor].value; 
      cursor++;
      if(tokens[cursor].type != TokenType::OPERATOR && !isInVector(tokens[cursor].value, Comparators))
              throw runtime_error("Invalid Comparison Operator in 'WHERE' clause");
      clause.comparator = tokens[cursor].value;
      cursor++;
     

      if(tokens[cursor].type != TokenType::STRING && tokens[cursor].type != TokenType::NUMBER 
              && tokens[cursor].value != "," ) throw runtime_error("Invalid token Type");
      clause.right_item = tokens[cursor].value;

      return clause;
}


string collect_parenthesis(int& cursor, vector<Token>& tokens) {
      cursor++;
      if(tokens[cursor].value != "(") throw runtime_error("Expected token '(' for value instantiation.");
      int endof_parenthesis = 0;
      for (int i = cursor; i < tokens.size(); i++) {
          if(tokens[i].value == ")") endof_parenthesis = i;
      }
      if(endof_parenthesis == 0) throw runtime_error("Token '(' was not closed.");
      
      string value;
      
      //exclude parenthesis
      cursor++;
      
      while(cursor != endof_parenthesis) {
          //temporary for testing!!
          if(tokens[cursor].type != TokenType::STRING && tokens[cursor].type != TokenType::NUMBER 
              && tokens[cursor].value != "," ) throw runtime_error("Invalid token Type");
          if(((cursor + 1) != endof_parenthesis) && tokens[cursor + 1].value != ",") throw runtime_error("Expected ',' token");
          else if(tokens[cursor + 1].value == ",") {
              value += tokens[cursor].value;
              value += " ";
              cursor += 2;
              continue;
          } 
          value += tokens[cursor].value;
          value += " ";
          cursor += 1;
  
      }
      return value;
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
        int cursor;
        vector<Token> tokens;

        string peek() { 

        };
        void consume() {

        };
        void match() {

        };
        void expect {

        };
        
    public:
    
        Parser(vector<Token> t) : tokens(t) {}

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

            tokens.push_back({TokenType::END, ""});
            return tokens;
        }

        Command parser(vector<Token> tokens) {
    
            
            
            
            switch(returnAction(tokens[0].value)) {
                
                case Action::INSERT:

                

                case Action::SELECT:
                    
                    
                case Action::DELETE:

                    
        
                case Action::UPDATE:
                

            }

            return command;
        }
};

int main() {
    string input = "UPDATE dudes SET name = 'kirche', age = 18, grade = 16.5 WHERE name = 'Kirsche'"; 

    vector<Token> tokens = tokenize(input);

    for (auto& t : tokens) {
        cout << static_cast<int>(t.type) << " : " << t.value << endl;
    }
    
    try {
        Command command;

        command = parser(tokens);

        command.action(command.table, command.value);
        cout << "WHERE clause: " << command.clauses[0].left_item << " " << command.clauses[0].comparator << " " << command.clauses[0].right_item << endl;
    }
    catch (const runtime_error& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
}
