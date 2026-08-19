#include <iostream>
#include <string>
#include "execute.hpp"
#include "src/storage.hpp"
#include "src/filesys.hpp"

DB_Header oglobalDBHEADER{0x44415641, 3};
TB_Header oglobalTBHEADER{0x44415441, 5};
RecordHeader oglobalRBHEADER{0x44415441, 5};
LoggerHeader oglobalLogHeader{0x44518449, 4};
BeforeImageHeader oglobalImageHeader{0x75314648, 1};

int mainmn() {
    CacheManagement cache;

    std::string input = "";

    std::cout << "------ SQLang ------\n";
    while(true) {
        ResultSet result;
        std::cout << "<" << cache.database.name << "> SQLang >> ";
        std::getline(std::cin, input);
        
        if(input == "q") break;

        std::string sql = input;

        if(!EXECUTE(cache, sql, result)) {
            std::cout << "FAILED\n\n";
        }
        else {
         
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

        if(!COMMIT(cache.database, cache.manager, nullptr, cache.pager, oglobalTBHEADER, oglobalDBHEADER, oglobalRBHEADER)) {
            return 1;
        }
        std::cout << "\n";
        input = "";
    }
    return 0;
}
