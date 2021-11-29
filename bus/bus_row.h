#ifndef BUS_ROW_H
#define BUS_ROW_H

#include <assert.h>
#include <vector>
#include <map>
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <exception>
#include "bus_util.h"
#include "bus_position.h"

using namespace std;

namespace bus {
    enum action_t {INSERT, UPDATE, DEL};
    class row_t {
    public:
        row_t() {};
        explicit row_t(uint32_t sz) {// explicit:防止强制类型转换
            _cols.reserve(sz);
            _oldcols.reserve(sz);
        }
        row_t(const row_t& other);

        void push_back(const char* p, bool is_old);
        void push_back(const char* p, int sz, bool is_old);

        bool get_value(uint32_t index, char** val_ptr);

        bool get_value_byindex(uint32_t index, char** val_ptr);

        bool get_value_byindex(uint32_t index, char** val_ptr, bus_log_t* solog);

        bool get_value_byname(char* name, char** val_ptr);

        bool get_value_byname(char* name, char **val_ptr, bus_log_t *solog);

        bool get_old_value(uint32_t index, char **val_ptr);

        bool get_old_value_byindex(uint32_t index, char **val_ptr);

        bool get_old_value_byindex(uint32_t index, char **val_ptr, bus_log_t *solog);

        bool get_old_value_byname(char* name, char **val_ptr);

        bool get_old_value_byname(char* name, char **val_ptr, bus_log_t *solog);

        void set_db(const char* db_name) {
            snprintf(_dbname, sizeof(_dbname), "%s", db_name);
        }

        const char* get_db_name() {
            return _dbname;
        }

        void set_table(const char* table_name) {
            snprintf(_tablename, sizeof(_tablename), "%s", table_name);
        }

        const char* get_table() const {
            return _tablename;
        }

        void set_action(action_t action) {
            _action = action;
        }

        action_t get_action() const {
            return _action;
        }

        uint32_t size() const {
            return _cols.size();
        }

        void print(string& info);// info是传出参数
        ~row_t();

    private:
        // 行数据--每一个字符串代表行中的一列
        vector<char*> _cols;
        vector<char*> _oldcols;
        map<string, int>* _pnamemap;
        // 表名
        char _dbname[64];
        char _tablename[64];
        // 行的动作
        action_t _action;
    };
}// namespace bus




#endif