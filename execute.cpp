#include <iostream>
#include <string>
#include "interpreter.hpp"
#include "src/filesys.hpp"

bool EXECUTE(AbstractSyntaxTree& AST) {
    switch (AST.action) {
        
        default:
        break;
    }
    return true;
}

int main() {
    std::string sql = "CREATE TABLE Dudes IF NOT EXISTS (id INT UNIQUE AUTO_INCRIMENT PRIMARY_KEY INDEXED, name TEXT UNIQUE, grade DOUBLE)";
    test_interpreter(sql);
    std::cout << "Worked\n";
}
