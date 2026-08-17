#include <memory>
#include <variant>
#include <vector>
#include <string>

#include "classes.h"
#pragma once

enum class TokenType {
    IDENTIFIER,
    INT,
    DOUBLE,
    STRING,
    OPERATOR,
    END
};
enum class Action {
    
    CONNECT,
    DISCONNECT,
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
    CREATE_INDEX
};
enum class DROP_TYPE {
    DROP_DATABASE,
    DROP_TABLE,
    DROP_INDEX
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
    PRIMARY_KEY,
    FOREIGN_KEY

};

enum class LOGICAL {

    AND,
    OR,
    NOT

};

struct Expression {
    virtual ~Expression() = default;
};


struct ComparisonNode : Expression {
    Token attribute;
    Token comparator;
    Token value;
};

struct OrNode : Expression {
    const LOGICAL logical = LOGICAL::OR;
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;
};

struct AndNode : Expression {
    const LOGICAL logical = LOGICAL::AND;
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;
};

struct NotNode : Expression {
    const LOGICAL logical = LOGICAL::NOT;
    std::unique_ptr<Expression> next;
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

struct CONNECT_AST {
    Token database;
};

struct DISCONNECT_AST {
    Token database;
};

struct CREATE_AST {

    CREATE_TYPE type;
    bool is_overrite = true;
    Token subject;
    std::vector<Column_AST> columns;  
    Token attribute;

};

struct DROP_AST {
    
    DROP_TYPE type;
    Token subject;

};

struct INSERT_DATA {
    bool is_root = false;
    std::vector<Token> tokens;
    std::unique_ptr<INSERT_DATA> next;
};
struct INSERT_AST {

    Token table;
    std::vector<Token> attributes;
    std::unique_ptr<INSERT_DATA> root;

};

struct SELECT_AST {

    Token table;
    std::vector<Token> attributes;
    std::unique_ptr<Expression> WHERE_ROOT;

};

struct DELETE_AST {

    Token table;
    std::unique_ptr<Expression> WHERE_ROOT;
    
};

struct UPDATE_AST {

    Token table;
    std::vector<Comparison> set;
    std::unique_ptr<Expression> WHERE_ROOT;

};

struct AbstractSyntaxTree {
    Action action;
    std::variant<CONNECT_AST,
                 DISCONNECT_AST,
                 CREATE_AST, 
                 DROP_AST, 
                 INSERT_AST, 
                 SELECT_AST, 
                 DELETE_AST, 
                 UPDATE_AST> tree;
};

bool GENERATE_AST(AbstractSyntaxTree& AST, std::string sql);
int test_interpreter(std::string sql);
