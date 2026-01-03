#include <iostream>
#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
     
using namespace std;
namespace fs = std::filesystem;

struct DataBankHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint32_t ID_INDEX = 0;
};
struct IndexHeader {
    uint32_t MAGIC;
    uint32_t VERSION;
    uint8_t DATATYPE;
};

enum class DataType : uint8_t {
    INTEIRO = 1, //int
    TEXTO = 2 //string
};

struct Column {
    DataType type;
    string name;  
};
struct Row {
    vector<variant<int32_t,  string>> values;
};

class Table {
    private:
        DataBankHeader DBheader{0x44415441, 1};
        
        template <typename T>
        bool save_header(ostream& out, const T& header) {
            out.write(reinterpret_cast<const char*>(&header), sizeof(T));
            return out.good();
        }
        
        
        bool validate_databank_header(DataBankHeader& header) {
            
            if(header.MAGIC != this->DBheader.MAGIC) {
                cerr << "Invalid Magic" << endl;
                return false;
            }
            else if(header.VERSION != this->DBheader.VERSION) {
                cerr << "Invalid Version" << endl;
                return false;
            }
            return true;
        }

        bool append(std::fstream& out, const Row& row) {
            
            // 2 is the index of id index offcet in the header struct.
            // 4 is the size of each value in the header struct, each are 4 bytes wide.
            int id_index_offset = 2 * 4;
            uint32_t index = 0;
            out.seekg(id_index_offset, ios::beg);
            out.read(reinterpret_cast<char*>(&index), sizeof(index));
            //reset read cursor:
            out.seekg(0, ios::beg);

            index += 1;
            //Move the write cursor to the end of the file to append:
            out.seekp(0, ios::end);

            uint32_t row_count = row.values.size() + 1;
            out.write(reinterpret_cast<const char*>(&row_count), sizeof(row_count));

            uint8_t id_type = 1;
            out.write(reinterpret_cast<const char*>(&id_type), sizeof(id_type));
            out.write(reinterpret_cast<const char*>(&index), sizeof(index));

            for (const auto& value : row.values) {
                std::visit([&out](const auto& payload) {
                    using T = std::decay_t<decltype(payload)>;
                        if constexpr (std::is_same_v<T, int32_t>) {

                            uint8_t type = 1;
                            out.write(reinterpret_cast<const char*>(&type), sizeof(type));
                            out.write(reinterpret_cast<const char*>(&payload), sizeof(payload));  // now 4 bytes
                            

                        } else if constexpr (std::is_same_v<T, std::string>) {

                            uint8_t type = 2;
                            out.write(reinterpret_cast<const char*>(&type), sizeof(type));
                            uint32_t len = static_cast<uint32_t>(payload.size());
                            out.write(reinterpret_cast<const char*>(&len), sizeof(len));
                            out.write(payload.data(), len);
                    

                        }
                }, value);
            }

            //Update index counter:
            out.seekp(id_index_offset, ios::beg);
            out.write(reinterpret_cast<const char*>(&index), sizeof(index));
            out.seekp(0, ios::beg);
            return out.good();
        }

        bool fetch(istream& in, Row& row) {

            uint32_t row_count;
            in.read(reinterpret_cast<char*>(&row_count), sizeof(row_count));
            if (!in) return false;
            row.values.resize(row_count);

            for(size_t i = 0; i < row_count; i++) {
                uint8_t type_int;
                in.read(reinterpret_cast<char*>(&type_int), sizeof(type_int));
                if (!in) return false;
                DataType type = static_cast<DataType>(type_int);

                switch (type) {
                    case DataType::INTEIRO: {
                        int32_t payload_int;
                        in.read(reinterpret_cast<char*>(&payload_int), sizeof(payload_int));
                        if (!in) return false;
                        row.values[i] = payload_int;
                        break;
                    }
                    case DataType::TEXTO: {
                        uint32_t len;
                        in.read(reinterpret_cast<char*>(&len), sizeof(len));
                        if (!in) return false;
                        string payload_str;
                        payload_str.resize(len);
                        in.read(payload_str.data(), len);
                        if (!in) return false;
                        row.values[i] = payload_str;
                        break;
                    }
                    default:
                        return false; // Unknown type
                }
            }
            return true;
        }

    public:
        
        
        
        string name;
        vector<Column> schema;
        vector<Row> data;

        fs::path folder_path;
        fs::path databank_path;
        fs::path index_path;

        Table(string n, const vector<Column>& s) : name(n), schema(s){

            folder_path = fs::path(this->name);
            fs::create_directories(folder_path);
            
            databank_path = fs::path(folder_path) / "DataBank.bin";
            if(!fs::exists(databank_path)) {
                ofstream ofs(databank_path, ios::binary);
                if(!this->save_header(ofs, this->DBheader)) {
                    cout << "Failed to initiate DataBank." << endl;
                }
            }

            index_path = fs::path(folder_path) / "index";
            if(!fs::exists(index_path)) {
                fs::create_directories(index_path);
                ofstream(fs::path(index_path) / "index-id.idx").close();
                for(auto& column: this->schema) {
                    string file_name = "index-" + column.name + ".idx";
                    ofstream(fs::path(index_path) / file_name, ios::binary);
                }
            }

            cout << "Table: " << this->name << " Created." << endl;
        }

        void appendRow(const Row& row) {
            
            fstream out(this->databank_path, ios::binary | ios::in | ios::out);
            if (!out) {
                cout << "Failed to Open file when Appending." << endl;
                return;
            }
            if (!this->append(out, row)) {
                cout << "Failed to append row." << endl;
                return;
            }
        }
        void fetchAllRows() {  // Renamed/updated to read all
            ifstream in(this->databank_path, ios::binary);
            if (!in) {
                cout << "Failed to Open file" << endl;
                return;
            }
            
            DataBankHeader fetched_header;

            in.read(reinterpret_cast<char*>(&fetched_header), sizeof(DataBankHeader));
            
            if(!this->validate_databank_header(fetched_header)) {
                cout << "Header not validated." << endl;
                return;
            }


            data.clear();  // Reset internal data
            while (in.peek() != EOF) {
                Row row;
                if (!this->fetch(in, row)) {
                    if (in.eof()) break;  // Normal end
                    cout << "Failed to fetch row." << endl;
                    return;
                }
                data.push_back(row);
            }
        }

        void print_id() {
            fstream out(this->databank_path, ios::binary | ios::in | ios::out);
            out.seekg(2 * 4, ios::beg);
            uint32_t index = 0;
            out.read(reinterpret_cast<char*>(&index), sizeof(index));
            cout << "Index: " << index << endl;
        }

};

int main() {

    vector<Column> columns = {
        {DataType::TEXTO, "name"},
        {DataType::INTEIRO, "grade"}
    };

    vector<Row> rows = {
        { {"dog", 69} },
        { {"Pippa", 12} },
        { {"Dude", 16} }
    };
    
    Table table("Dudes", columns);

    Row add;
    add.values.push_back("Pippa");
    add.values.push_back(100);
    Row add2;
    add2.values.push_back("Dude");
    add2.values.push_back(50);
    Row add3;
    add2.values.push_back("Yohan the Butcher");
    add2.values.push_back(25);
    Row add4;
    add4.values.push_back("Kirche");
    add4.values.push_back(99);
    add4.values.push_back("Fox God");

    vector<Row> rows2 = { {add, add2, add3, add4} };

    for (auto i: rows2) {
        table.appendRow(i);
    }
    table.fetchAllRows();  // Updated call
    int row_num = 1;
    for (const auto& row : table.data) {  // Loop over all fetched rows
        cout << "Row " << row_num++ << ":" << endl;
        int count = 1;
        for (auto item: row.values) {
            //cout << "item number: " << count << endl;
            count ++;
            visit([](const auto& x) {
                using T = std::decay_t<decltype(x)>;
    
                if constexpr (std::is_same_v<T, int32_t>) {
                    //cout << "Type: int" << endl;
                    cout << "|| " << x << " ||" << endl;
                }
                else if constexpr (std::is_same_v<T, std::string>) {
                    //cout << "Type: str" << endl;
                    cout << "|| " << x << " ||" << endl;
                }
            }, item);
        }
        cout << "SIZE: " << row.values.size() << endl;
    }

    table.print_id();

    return 0;
}