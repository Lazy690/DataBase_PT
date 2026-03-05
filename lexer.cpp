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

Action returnAction(string a) {
    map<string, Action> m = {{"INSERT", Action::INSERT}, {"SELECT", Action::SELECT}, {"DELETE", Action::DELETE}, {"UPDATE", Action::UPDATE}};
    return m[a];
}

struct Token {
    TokenType type;
    string value;
};

struct Where_clause{
    
    string right_item;
    string comparator;
    string left_item;

};

struct Command {
    //function pointer named 'action'.
    void (*action)(string, string);
    string table;
    string attribute;
    string value;
    vector<Where_clause> clauses;
};

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


vector<Token> tokenize(const string& input) {
    vector<Token> tokens;
    string current;

    for (size_t i = 0; i < input.size(); i++) {
        char c = input[i];

        // 1️⃣ Skip whitespace
        if (isspace(c)) {
            continue;
        }

        // 2️⃣ String literal
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

        // 3️⃣ Two-character operators
        if ((c == '<' || c == '>' || c == '!' || c == '=') &&
            i + 1 < input.size() && input[i + 1] == '=') {
            
            string op;
            op += c;
            op += '=';

            tokens.push_back({TokenType::OPERATOR, op});
            i++; // skip second char
            continue;
        }

        // 4️⃣ Single-character operators
        if (c == '<' || c == '>' || c == '=' || c == '+' || c == '-' || c == '*' || c == '(' || c == ')' || c == ',') {
            tokens.push_back({TokenType::OPERATOR, string(1, c)});
            continue;
        }

        // 5️⃣ Number
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

        // 6️⃣ Identifier
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

        // 7️⃣ Unknown character
        throw runtime_error("Unknown character detected.");
    }

    tokens.push_back({TokenType::END, ""});
    return tokens;
}

bool isValidGrammar(vector<Token>& tokens) {

    if(tokens[0].type != TokenType::IDENTIFIER) {
        cerr << "Syntax error: " << tokens[0].value << " is Invalid.";  
        return false; 
    }
    
    return true;
}

Command parser(vector<Token> tokens) {
    
    Command command;

    if(tokens[0].type != TokenType::IDENTIFIER) {
        throw runtime_error("Missing Action keyword the the beggining of the command.");
    }
    {
    auto it = action_map.find(tokens[0].value);
    if(it == action_map.end()) {
        throw runtime_error("This Action not a valid command.");
    }
    }
//check to see if command string has an end character (just in case)
    if(tokens[-1].type != TokenType::END) {
        //throw runtime_error("Missing 'END' token at the end of the command string");
    }
    
    command.action = action_map[tokens[0].value]; 
    string values;
    int cursor = 1;
    
    
    switch(returnAction(tokens[0].value)) {
      
      case Action::INSERT:

        if(tokens[cursor].value != "INTO") 
            throw runtime_error("Missing token after token INSERT, Did you mean 'INTO'?");
        
        cursor++;

        if(isInVector(tokens[cursor].value, KeyWords))
            throw runtime_error("Expected a table name after token INTO");

        command.table = tokens[cursor].value;
        
        cursor++;

        if(tokens[cursor].value != "VALUES") {
            throw runtime_error("Expected token after table name, Did you mean 'VALUES'?");
        }


        values = collect_parenthesis(cursor, tokens);
        
        command.value = values;
        
        break;
    

      case Action::SELECT:
          
          //Temporary: For testing the only option avalable will be *;
          if(tokens[cursor].value != "*")
              throw runtime_error("Expected token '*' after 'SELECT'");

          command.value = tokens[cursor].value;
          
          cursor++;

          if(tokens[cursor].value != "FROM") 
              throw runtime_error("Missing token after token INSERT, Did you mean 'FROM'?");
          
          cursor++;

          if(isInVector(tokens[cursor].value, KeyWords))
              throw runtime_error("Expected a table name after token 'FROM'");
          
          command.table = tokens[cursor].value;

          cursor++;
          cout << "Token: " << tokens[cursor].value << endl;
          if(tokens[cursor].type == TokenType::END)
              break;

          
          cout << "Token: " << tokens[cursor].value << endl;
 
          if(tokens[cursor].value != "WHERE")
              throw runtime_error("Expected 'WHERE' clause. If not intended then end the command at the Table name.");
          
          //Expand later for more sufisticated WHERE clauses when you add AND
          //gather all WHERE clauses:
          
          cursor++;

          //vector<Where_clause> clauses;
          command.clauses.push_back(collect_clauses(cursor, tokens));
          break;
      case Action::DELETE:

          if(tokens[cursor].value != "FROM") 
              throw runtime_error("Missing token after token 'DELETE', Did you mean 'FROM'?");
          
          cursor++;

          if(isInVector(tokens[cursor].value, KeyWords))
              throw runtime_error("Expected a table name after token 'FROM'");
          
          command.table = tokens[cursor].value;

          cursor++;

          if(tokens[cursor].value != "WHERE")
              throw runtime_error("Expected 'WHERE' clause after table decleration.");
          
          cursor++;

          command.clauses.push_back(collect_clauses(cursor, tokens));
          break;
 
      case Action::UPDATE:
        cout << "Token: " << tokens[cursor].value << endl;        
        if(isInVector(tokens[cursor].value, KeyWords))
            throw runtime_error("Expected a table name after token 'UPDATE'");
        
        command.table = tokens[cursor].value;
        
        cursor++;

        if(tokens[cursor].value != "SET")
            throw runtime_error("Expected 'SET' clause after table decleration.");
        
        {
        bool where_exists = false;   
        for(int i = cursor; i < tokens.size(); i++) {
           if(tokens[i].value == "WHERE") where_exists = true;
        }
           if(!where_exists) throw runtime_error("Expected a 'WHERE' clause for 'UPDATE' commands.");
        }
        string set = collect_set(cursor, tokens);
        command.value = set;
        Where_clause clause;
        if(tokens[cursor].value != "WHERE")
            throw runtime_error("Expected 'WHERE' clause after table decleration.");
        cursor++;

        command.clauses.push_back(collect_clauses(cursor, tokens));

    }

    return command;
}

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
