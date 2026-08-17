#include <iostream>
#include <filesystem>
#include <string>
#include <fstream>
#include "execute.hpp"
#include "src/storage.hpp"

namespace fs = std::filesystem;

DB_Header nglobalDBHEADER{0x44415641, 3};
TB_Header nglobalTBHEADER{0x44415441, 5};
RecordHeader nglobalRBHEADER{0x44415441, 5};
LoggerHeader nglobalLogHeader{0x44518449, 4};
BeforeImageHeader nglobalImageHeader{0x75314648, 1};

std::string trimLine(const std::string& line) {

      std::string trimmed = "";
      size_t beg;
      for (beg = 0; beg < line.size(); beg++) {
          if (!isspace(line[beg])) {
              break;
          }
      }
      if(beg == line.size()) return trimmed;
      size_t end;
      for(end = line.size(); end-- > 0;) {
          if(!isspace(line[end])) {
              end++;
              break;
          }
      }
      trimmed.insert(trimmed.begin(), line.begin() + beg, line.begin() + end);
      return trimmed;

}

void printResults(const ResultSet& result) {
    if(result.values.size() > 0) {
        std::cout << "---------RESULTS---------\n";
        for (size_t i = 0; i < result.values.size(); i++) {
            for (size_t j = 0; j < result.values[i].values.size(); j++) {
                if(j != 0) std::cout << ", ";
                std::visit([](const auto& x) {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, int32_t>) {
                        std::cout << x;
                    }
                    else if constexpr (std::is_same_v<T, std::string>) {
                        std::cout << x;
                    }
                    else if constexpr (std::is_same_v<T, double>) {
                        std::cout << x;
                    }
                }, result.values[i].values[j]);
            }
            std::cout << "\n";
        }
    }
}

void EXECUTE_SCRIPT(const std::string& filename, fs::path path) {
 
    ResultSet resultSet;
    CacheManagement cache;

    fs::path file_name(filename);
    if (file_name.extension() != ".sql") throw std::runtime_error("Invalid file type");
    path /= filename;

    std::ifstream file(path);
    if(!file) {
        std::cerr << "No such file in directory.\n";
        return;
    }

    std::string command;
    std::string line;
    while (std::getline(file, line)) {
        std::string trimmed = trimLine(line);

        if(trimmed.empty()) continue;
        char first = trimmed[0];
        //comments
        if(first == '#') continue;

        char last = trimmed[trimmed.size() - 1];
        if(last == ';') {
            command += " ";
            command += trimmed;
            if(!EXECUTE(cache, command, resultSet)) {
                return;
            }
            printResults(resultSet);
            if(!COMMIT(cache.database, cache.manager, nullptr, cache.pager, nglobalTBHEADER, nglobalDBHEADER, nglobalRBHEADER)) {
                return;
            }
            std::cout << command << "\n";
            command = "";
            continue;
        }
        else {
            command += " ";
            command += trimmed;
        }
    }
}

int maim() {
    EXECUTE_SCRIPT("script.sql", "..");
    return 0;
}
