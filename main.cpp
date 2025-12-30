#include <iostream>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
using namespace std;

struct Column {
    string name;
    string type;
};
struct Row {
    vector<variant<int,  string>> row;
    Row(std::initializer_list<variant<int, string>> init) : row(init) {}
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

bool save_table_name(ostream& out, const Table& t) {
    
    uint32_t name_len = t.name.size();

    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(t.name.data(), name_len);

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
        {"name", "TEXTO"},
        {"grade", "INTEIRO"}
    };

    vector<Row> rows = {
        { {"dog", 69} },
        { {"Pippa", 12} },
        { {"Dude", 16} }
    };
    
    Table table("example", columns, rows);

    ofstream out("example.bin", ios::binary);
    if(!save_table_name(out, table)) {
        cout << "Failed to save" << endl;
        return 1;
    }
    out.close();

    string loaded_name;

    ifstream in("example.bin", ios::binary);
    if(!load_table_name(in, loaded_name)) {
        cout << "Failed to load" << endl;
        return 1;
    }
    in.close();
    cout << loaded_name << " Loaded Successfully." << endl;

    return 0;
}
