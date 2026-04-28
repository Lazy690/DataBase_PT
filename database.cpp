#include <iostream>
#include <filesystem>
namespace fs = std::filesystem;
#include <memory>
#include <variant>
#include <type_traits>

#include "executer.h"
#include "interpreter.h"


class Planner {
    private:
        Executer execute;
        Paths paths;
    public:
        Planner(fs::path table) {
            paths.record_bank = fs::path(table) / "RecordBank.bin";
            paths.index = fs::path(table) / "index";
        }

        void run(const AST& ast) {
          std::visit([this](const auto& tree) {
                  
                  using T = std::decay_t<decltype(tree)>;

                  if constexpr(std::is_same_v<T, INSERT_AST>) {
                      std::vector<Token> values = tree.values;
                      Row row;
                      row.values.push_back(tree.values[0].value);
                      row.values.push_back(stoi(tree.values[1].value));
                      row.values.push_back(stod(tree.values[2].value));

                      execute.INSERT(this->paths, row);
                  }

            }, ast);
        }


};

int main() {

    Interpreter interpreter;
    Planner planner(fs::path("Dudes"));
    Executer execute;

    std::string input = "INSERT (name, age, grade) INTO Dudes ('Sawa', 30, 15.6)";
    AST ast = interpreter.run(input);
    planner.run(ast);

    Paths path;
    path.record_bank = fs::path("Dudes") / "RecordBank.bin";
    path.index = fs::path("Dudes") / "index";

    std::vector<Row> results;
    std::string column = "name";
    std::variant<int, std::string, double> value = "Sawa";
    execute.SELECT_byValue(path, column, value, results);

    execute.print_row_test(results);

    return 0;
}


