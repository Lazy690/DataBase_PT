
#include "interpreter.hpp"
#include "src/filesys.hpp"
#include "src/storage.hpp"
#include "headers.hpp"

struct CacheManagement {

    DataBase database;
    Pager pager;
    Logger logger;
    FileManager manager;

};
struct Result {
    std::vector<std::variant<int32_t, std::string, double>> values;
    template<typename T>
    T get(size_t index) {
        if constexpr (std::is_same_v<T, int32_t>) {
            return std::get<int32_t>(values[index]);
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            return std::get<std::string>(values[index]);
        }
        else if constexpr (std::is_same_v<T, double>) {
            return std::get<double>(values[index]);
        }
    }
};
struct ResultSet {
    std::vector<Result> values;
};

bool EXECUTE(CacheManagement& cache, std::string sql, ResultSet& resultSet);
