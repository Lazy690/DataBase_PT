#include <vector>
#include <cstdint>
#include <string>
#include <variant>
#include <fstream>
#include <filesystem>
#include <map>

#include "indexer.h"
using namespace std;

class Executer {
    private:
        RecordBankHeader DBheader{0x44415441, 2};
        IndexHeader Iheader{0x44415441, 2};
        struct RecordBankHeader; 
        struct IndexHeader ;
        struct MetaDataHeader; 
        enum class DataType : uint8_t ;
        struct Column;
        Executer::struct Row {
            vector<variant<int32_t,  string, double>> values;
        };
        struct Header_vals;
        struct Paths;

        uint32_t hold_row_id;
        Row hold_for_indexing;
        uint32_t RecordBank_offset_recording;
        
        bool save_DBheader(ostream& file, const RecordBankHeader& header);
        bool save_Iheader(ostream& file, const IndexHeader& header, const uint32_t& type);
        bool save_MDheader(ostream& file, const MetaDataHeader& header);
        
        bool validate_RecordBank_header(fstream& file);
        bool validate_Index_header(fstream& file);
        bool validate_Metadata_header(istream& file);
          
        bool save_table_metadata(ostream& file, const std::vector<Column>& schema, const std::string& table_name);
        bool load_table_metadata(istream& file, std::vector<Column>& schema, std::string& table_name);

        //==================================================================
        //====== Index operations                                     ======
        //==================================================================

        bool save_ID_record(const uint32_t& id, const uint32_t& offset, const int32_t& overrite_id_offset = -1); 
        bool save_value_index_record(const Row& row, uint32_t& hold_offset);

        bool fetch_ID_offset(fstream& file, const int ID, int& fetched_offset);
        bool fetch_Value_offset(fstream& file, variant<int32_t, string, double>& value, vector<uint32_t>& fetched_offsets);
        //==================================================================
        //====== RecordBank operations                                  ====
        //==================================================================

        bool append(std::fstream& file, const Row& row, uint32_t& hold_id, uint32_t& hold_offser);
        bool fetch(istream& file, Row& row, int fetched_offset = -1);
        bool deleteRow(fstream& file, int fetched_offset = -1);
        bool append_updated_row(std::fstream& file, const Row& row, int& hold_new_offset, int old_index);

    public:

        void INSERT(Paths path, const Row& row);
        void SELECT_byID(Paths path, int id, Row& row, bool returns_offset = false, int* returned_offset = 0);
        void SELECT_byValue(Paths path, string& column, variant<int32_t, string, double>& value, vector<Row>& results, bool returns_offset = false, vector<uint32_t>* returned_offsets = {});
        void DELETE_byID(Paths path, const int id);
        void DELETE_byID(Paths path, int id, map<string, variant<int32_t, string, double>>& values_map);
        void UPDATE_byValue(Paths path, string column_to_search, variant<int32_t, string, double>& value_to_search, map<string, variant<int32_t, string, double>>& values_map); 
        void UPDATE_byValue(Paths path, string column_to_search, variant<int32_t, string, double>& value_to_search, map<string, variant<int32_t, string, double>>& values_map); 
        void print_row_test(vector<Row> results);
};

