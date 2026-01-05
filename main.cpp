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
    //This also has a dataType assiciated with it.
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
        IndexHeader Iheader{0x44415441, 1};

        uint32_t hold_row_id = 0;
        Row hold_for_indexing;
        uint32_t Databank_offset_recording = 0;
        
        bool save_DBheader(ostream& out, const DataBankHeader& header) {
            out.write(reinterpret_cast<const char*>(&header), sizeof(DataBankHeader));
            return out.good();
        }
        bool save_Iheader(ostream& out, const IndexHeader& header, const uint8_t& type) {
            out.write(reinterpret_cast<const char*>(&header), sizeof(IndexHeader));
            out.write(reinterpret_cast<const char*>(&type), sizeof(type));
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

        //==================================================================
        //====== Index operations                                     ======
        //==================================================================


        bool save_ID_record(const uint32_t& id, const uint32_t& offset) {

            fstream index(fs::path(this->index_path) / "id.idx", ios::binary | ios::in | ios::out);
            if(!index) {
                cout << "Missing Id index file." << endl;
                return false;
            }
            index.seekp(0, ios::end);
            int first_pos = index.tellp();
            
            index.write(reinterpret_cast<const char*>(&id), sizeof(id));
            
            index.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
            int second_pos = index.tellp();
            
            return index.good();
        }
        bool fetch_ID_offset(istream& index, const int ID, int& fetched_offset) {

            //8 is because each entry is always 8 bytes wide: [value][offset in bank]
            int header_size = 9;
            index.seekg(header_size, ios::beg);
            
            int id_location_offset = (ID - 1) * 8;
            index.seekg(id_location_offset, ios::cur);
            

            if(index.eof()) {
                cerr << "Id out of Bounds." << endl;
                return false;
            }

            int32_t value = 0;

            index.read(reinterpret_cast<char*>(&value), sizeof(value));
           
            if(value != ID) {
                cerr << "Id not found" << endl;
                return false;
            }

            index.read(reinterpret_cast<char*>(&fetched_offset), sizeof(fetched_offset));
           
            return index.good();

        };


        //==================================================================
        //====== DataBank operations                                  ======
        //==================================================================

        bool append(std::fstream& out, const Row& row, uint32_t& hold_id, uint32_t& hold_offset) {
            
            // 2 is the index of id index offset in the header struct.
            // 4 is the size of each value in the header struct, each are 4 bytes wide.
            int id_index_offset = 2 * 4;
            uint32_t index = 0;
            out.seekg(id_index_offset, ios::beg);
            out.read(reinterpret_cast<char*>(&index), sizeof(index));
            //reset read cursor:
            out.seekg(0, ios::beg);

            index += 1;
            hold_id = index;
            //Move the write cursor to the end of the file to append:
            out.seekp(0, ios::end);

            //Capture the offset of the start of the row were going to save to use in the indexer:

            hold_offset = out.tellp();

            //write an empty space to later store the offset of the end of the row:
            int32_t end_of_row_offset = -1;
            int32_t end_of_row_offset_location = out.tellp();
            out.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));

            //Write row ID:
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

            //store the offset of the end of the row in the empty space we saved earlier:
            end_of_row_offset = out.tellp();
            out.seekp(end_of_row_offset_location, ios::beg);
            out.write(reinterpret_cast<const char*>(&end_of_row_offset), sizeof(end_of_row_offset));
           
            //Update index counter:
            out.seekp(id_index_offset, ios::beg);
            out.write(reinterpret_cast<const char*>(&index), sizeof(index));
            out.seekp(0, ios::beg);
            return out.good();
        }

        bool fetch(istream& in, Row& row) {

            //Add check so that it always skips the header here!!!

            
            //get the offset of end of the row:
            int32_t end_of_row_offset = 0;
            in.read(reinterpret_cast<char*>(&end_of_row_offset), sizeof(end_of_row_offset));
            
            while(true) {

                if(in.tellg() == end_of_row_offset) break;   
                
                uint8_t type_int;
                in.read(reinterpret_cast<char*>(&type_int), sizeof(type_int));
                if (!in) return false;
                DataType type = static_cast<DataType>(type_int);

                switch (type) {
                    case DataType::INTEIRO: {
                        int32_t payload_int;
                        in.read(reinterpret_cast<char*>(&payload_int), sizeof(payload_int));
                        if (!in) return false;
                        row.values.push_back(payload_int);
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
                        row.values.push_back(payload_str);
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
                if(!this->save_DBheader(ofs, this->DBheader)) {
                    cout << "Failed to initiate DataBank." << endl;
                }
            }

            index_path = fs::path(folder_path) / "index";
            if(!fs::exists(index_path)) {
                fs::create_directories(index_path);
                ofstream id_out(fs::path(index_path) / "id.idx");
                
                uint32_t ID_index_file_type = 1;

                if(!this->save_Iheader(id_out, this->Iheader, ID_index_file_type)) {

                    cout << "Failed to initiate ID index." << endl;

                }
                id_out.close();
                for(auto& column: this->schema) {

                    uint8_t index_file_type = static_cast<uint8_t>(column.type);

                    string file_name = column.name + ".idx";
                    ofstream index_out(fs::path(index_path) / file_name, ios::binary);
                    if(!this->save_Iheader(index_out, this->Iheader, index_file_type)) {

                        cout << "Failed to initiate " << column.name << " index." << endl;

                    }
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

            uint32_t hold_id = 0;
            uint32_t hold_offset = 0;

            if (!this->append(out, row, hold_id, hold_offset)) {
                cout << "Failed to append row." << endl;
                return;
            }
            out.close();

            if(!this->save_ID_record(hold_id, hold_offset)) {
                cout << "Failed to Index ID." << endl;
                return;
            }

        }

        void fetchRow_byID(int id, Row& row) {  
           
            ifstream index(fs::path(this->index_path) / "id.idx", ios::binary);
            if(!index) {
                cerr << "Failed to find ID index file." << endl;
                return;
            }

            int fetched_offset = 0;

            if(!fetch_ID_offset(index, id, fetched_offset)) {
                cout << "Failed to get row location from ID index" << endl;
                return;
            }
            index.close();
            
            ifstream databank(this->databank_path, ios::binary);
            if (!databank) {
                cout << "Failed to Find Databank file." << endl;
                return;
            }
            
            DataBankHeader fetched_header;

            databank.read(reinterpret_cast<char*>(&fetched_header), sizeof(DataBankHeader));
            
            if(!this->validate_databank_header(fetched_header)) {
                cout << "Header not validated." << endl;
                return;
            }

            //Now move the read cursor to the offset and get that row.
            databank.seekg(fetched_offset, ios::beg);
            if(!this->fetch(databank, row)) {
                cout << "Failed ot fetch row from Databank" << endl;
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

        void print_ID(const int ID, int& fetched_offset) {
            
            ifstream index(fs::path(this->index_path) / "id.idx", ios::binary);
            if(!index) {
                cout << "Missing Id index file." << endl;
                return;
            }
            if(!this->fetch_ID_offset(index, ID, fetched_offset)) {
                cout << "Failed to fetch ID" << endl;
                return;
            }
            
        }

};

int main() {

    vector<Column> columns = {
        {DataType::TEXTO, "student_id"},
        {DataType::INTEIRO, "grade"}
    };

    vector<Row> rows = {
        { {"dog", 69} },
        { {"Pippa", 12} },
        { {"Dude", 16} }
    };
    
    Table table("Dudes", columns);
    
        Row row;
        table.fetchRow_byID(4, row);
        
        int count = 0;
        for (auto item: row.values) {
                
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
    
    return 0;
}