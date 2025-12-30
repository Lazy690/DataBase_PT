#include <iostream>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>

using namespace std;

enum class DataType : uint8_t {
    INTEIRO = 1, //int
    TEXTO = 2 //string
};

struct Column {
    string name;
    DataType type;
};
struct Row {
    vector<variant<int32_t,  string>> row;
    //Row(std::initializer_list<variant<int, string>> init) : row(init) {}
};

class Table {

    public:
        string name;
        vector<Column> schema;
        vector<Row> data;
    
        Table(string n, vector<Column> s, vector<Row> d) : name(n), schema(s), data(d) {
            cout << "Table: " << this->name << " Created." << endl;
        }
        string get_name() {
            return name;
        }
};

void evaluate(const variant<int, string>& v, DataType& t) {
    std::visit([&t](const auto& x) {
        using T = std::decay_t<decltype(x)>;

        if constexpr (std::is_same_v<T, int>) {
            t = DataType::INTEIRO;
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            t = DataType::TEXTO;
        }
        else if constexpr (std::is_same_v<T, double>) {
            //Empty for now
        }
    }, v);
}

bool save_table_name(ostream& out, const Table& t) {
    
    uint32_t name_len = t.name.size();

    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(t.name.data(), name_len);

    return out.good();
}

bool save_column(ostream&out, const Column& c) {
    
    uint32_t name_len = c.name.size();

    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(c.name.data(), name_len);

    uint8_t type = static_cast<uint8_t>(c.type);

    out.write(reinterpret_cast<const char*>(type), 1);

    return out.good();
}

void save_row(ostream& out,const Row& r) {
    for (auto item: r.row) {
        //Make this into a function that returns the type instead of changing the variable by refrence!!!
        //evaluate(item, type);

        std::visit([](const auto& x) {
            using T = std::decay_t<decltype(x)>;
    
            if constexpr (std::is_same_v<T, int32_t>) {
                uint8_t type = 1;
                out.write(reinterpret_cast<const char*>(&type), 1);

                int32_t val = &x;
                out.write(reinterpret_cast<const char*>(&val), sizeof(&val));
                
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                uint8_t type = 2;
                out.write(reinterpret_cast<const char*>(&type), 1);

                int32_t len = x.size();
                out.write(reinterpret_cast<const char*>(&len), sizeof(&len));
                out.write(x.data(), &len);
            }
            
        }, item);
    }
    return out.good();
}

bool load_table_name(istream& in, string& name) {

    uint32_t name_len;

    in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    
    name.resize(name_len);

    in.read(name.data(), name_len);

    return in.good();
}

int main() {

    vector<Column> columns = {
        {"name", DataType::TEXTO},
        {"grade", DataType::INTEIRO}
    };

    vector<Row> rows = {
        { {"dog", 69} },
        { {"Pippa", 12} },
        { {"Dude", 16} }
    };
    
    Table table("Dudes", columns, rows);

    
    save_row(table.data[0]);
    
    return 0;
}
