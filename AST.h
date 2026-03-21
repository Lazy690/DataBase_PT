#pragma once
#include <string>
#include <vector>
#include <variant>
#include <memory>
enum class TokenType {
    IDENTIFIER,
    NUMBER,
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

enum class Constraint {

    UNIQUE,
    AUTO_INCRIMENT,
    NOT_NULL,
    PRIMARY_KEY,
    FOREIGN_KEY

};

enum class DataType {

    INT,
    DOUBLE,
    TEXT

};

vector<string> constraints_vec {
    "UNIQUE",
    "AUTO_INCRIMENT",
    "NOT_NULL",
    "PRIMARY_KEY",
    "FOREIGN_KEY"
};

struct Constraints_list {

    bool unique = false;
    bool auto_incriment = false;
    bool not_null = false;
    bool primary_key = false;
    bool foreign_key = false;

};

struct Column_AST {

    Token name;
    DataType type;
    Constraints_list constraints;

};

struct CREATE_AST {

    bool is_overrite = true;
    Token table;
    vector<Column_AST> columns;  

};

struct DROP_AST {

    Token table;

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

using AST = variant<CREATE_AST, DROP_AST, INSERT_AST, SELECT_AST, DELETE_AST, UPDATE_AST>;



