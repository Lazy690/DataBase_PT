#pragma once
#include <string>
#include "AST.h"

class Interpreter {
    public:
        AST run(const string& input);
};
