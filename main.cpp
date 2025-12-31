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

bool save_table_name(ostream& out, const Table& t) {
    
    uint32_t name_len = t.name.size();

    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(t.name.data(), name_len);

    return out.good();
}

bool save_schema(ostream& out, const vector<Column>& schema) {
    
    uint64_t size = schema.size();    
    out.write(reinterpret_cast<const char*>(&size), sizeof(&size));
    
    for (auto column: schema) {
        //cout << "saving column: " << column.name << endl;
        uint32_t name_len = column.name.size();

        out.write(reinterpret_cast<const char*>(&name_len), sizeof(&name_len));
        //cout << "String size saved: " << column.name << endl;
        out.write(column.name.data(), name_len);
        //cout << "String saved: " << column.name << endl;
        switch(column.type) {
            case DataType::TEXTO:
                //cout << "Column.type is TEXTO" << endl;
                break;
        }       
        uint8_t type = static_cast<uint8_t>(column.type);

        out.write(reinterpret_cast<const char*>(&type), 1);
        //cout << "Type saved: " << column.name << endl;
        cout << "column saved: " << column.name << endl;
        
    }
    return out.good();
}

bool save_row(ostream& out,const Row& r) {
    for (auto item: r.row) {
        //Make this into a function that returns the type instead of changing the variable by refrence!!!
        //evaluate(item, type);

        std::visit([&out](const auto& x) {
            using T = std::decay_t<decltype(x)>;
    
            if constexpr (std::is_same_v<T, int32_t>) {
                uint8_t type = 1;
                out.write(reinterpret_cast<const char*>(&type), 1);

                
                out.write(reinterpret_cast<const char*>(&x), sizeof(&x));
                
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                uint8_t type = 2;
                out.write(reinterpret_cast<const char*>(&type), 1);

                int32_t len = x.size();
                out.write(reinterpret_cast<const char*>(&len), sizeof(&len));
                out.write(x.data(), len);
            }
            
        }, item);
    }
    return out.good();
}

//###################################################################################
//######################## -LOADING SECTION- ########################################
//###################################################################################

bool load_table_name(istream& in, string& name) {

    uint32_t name_len;

    in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    
    name.resize(name_len);

    in.read(name.data(), name_len);

    return in.good();
}

bool load_schema(istream& in, vector<Column>& schema) {
    
    uint64_t size;
    in.read(reinterpret_cast<char*>(&size), sizeof(&size));
    schema.resize(size);

    for (auto column: schema) {

        uint32_t name_len;

        in.read(reinterpret_cast<char*>(&name_len), sizeof(&name_len));
        
        column.name.resize(name_len);

        in.read(reinterpret_cast<char*>(column.name.data()), name_len);

        uint8_t type;

        in.read(reinterpret_cast<char*>(&type), 1);

        column.type = static_cast<DataType>(type);

    }
    return in.good();
}

bool load_row(istream& in, Row& r) {
    for (auto item: r.row) {
        //Make this into a function that returns the type instead of changing the variable by refrence!!!
        //evaluate(item, type);

        std::visit([&in](auto& x) {
            using T = std::decay_t<decltype(x)>;
    
            if constexpr (std::is_same_v<T, int32_t>) {
                uint8_t type = 1;
                in.read(reinterpret_cast<char*>(&type), 1);

                
                in.read(reinterpret_cast<char*>(&x), sizeof(&x));
                
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                uint8_t type = 2;
                in.read(reinterpret_cast<char*>(&type), 1);

                int32_t len = x.size();
                in.read(reinterpret_cast<char*>(&len), sizeof(&len));
                in.read(x.data(), len);
            }
            
        }, item);
    }
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

    ofstream out("schema.bin", ios::binary);

    if(!save_schema(out, table.schema)) {
        cout << "Could not save schema." << endl;
    }
    else {
        cout << "Worked!" << endl;
    }
    out.close();

    vector<Column> schema;
    ifstream in("schema.bin", ios::binary);
    if(!load_schema(in, schema)) {
        cout << "Could not save schema." << endl;
    }
    else {
        cout << "Columns Loaded!" << endl;
        for (auto col: schema) {
            
            cout << "Name: " << col.name << endl;
            switch(col.type) {
                case DataType::TEXTO:
                    cout << "Type: TEXT" << endl;
                    break;
                case DataType::INTEIRO:
                    cout << "Type: INTEIRO" << endl;
                    break;
            }
        }
    }
    in.close();

    return 0;
}
