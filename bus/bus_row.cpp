#include <sys/time.h>
#include <sys/types.h>
#include "bus_row.h"

namespace bus {
    void row_t::push_back(const char *p, bool is_old) {
        if (nullptr == p) {
            if (is_old)
                _oldcols.push_back(nullptr);
            else
                _cols.push_back(nullptr);
        }

        uint32_t sz = strlen(p);
        char* str = new (std::nothrow)char[sz + 1];
        if (nullptr == str) {
            g_logger.error("allocate memory fail");
            return;
        }
        // p复制到str
        if (sz != 0) memcpy(str, p, sz);
        str[sz] = '\0';// str字符串要加尾0
        if (is_old)
            _oldcols.push_back(str);
        else
            _cols.push_back(str);
    }

    void row_t::push_back(const char* p, int sz, bool is_old) {
        char* str = new (std::nothrow)char[sz + 1];
        if (nullptr == str) {
            g_logger.error("mallocate memory fail");
            return;
        }
        if (sz != 0) memcpy(str, p, sz);
        str[sz] = '\0';
        if (is_old)
            _oldcols.push_back(str);
        else
            _cols.push_back(str);
    }

    bool row_t::get_value(uint32_t index, char **val_ptr) {
        uint32_t sz = _cols.size();
        if (index >= sz) {
            g_logger.error("get value fail, index = %lu, size = %lu", index, sz);
            return false;
        }

        *val_ptr = _cols[index];
        return true;
    }

    bool row_t::get_value_byindex(uint32_t index, char **val_ptr) {
        uint32_t sz = _cols.size();
        if (index >= sz) {
            g_logger.error("get value fail, index = %lu, size = %lu", index, sz);
            return false;
        }

        *val_ptr = _cols[index];
        return true;
    }

    bool row_t::get_value_byindex(uint32_t index, char **val_ptr, bus_log_t *solog) {
        uint32_t sz = _cols.size();
        if (index >= sz) {
            string info;
            this->print(info);// info是传出参数
            solog->error("get value fail, index = %lu, size = %lu, data = %s",
                         index, sz, info.c_str());
            return false;
        }
        *val_ptr = _cols[index];
        return true;
    }

    bool row_t::get_value_byname(char *name, char **val_ptr, bus_log_t *solog) {
        //*solog for _src_schema->get_column_index() need #include"bus_config.h"
        //then bad to bus_process.cc
        assert(NULL != _pnamemap);
        std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
        if (it == (*_pnamemap).end()) {
            solog->error("namemap can not find %s", name);
            return false;
        }

        uint32_t index = it->second;
        uint32_t sz = _cols.size();
        if (index >= sz) {
            std::string info;
            this->print(info);
            solog->error("get value fail, index=%u,size=%u, data=%s",
                         index, sz, info.c_str());
            return false;
        }
        *val_ptr = _cols[index];
        return true;
    }

    bool row_t::get_old_value(uint32_t index, char **val_ptr) {
        uint32_t sz = _oldcols.size();
        if (index >= sz) {
            std::string info;
            this->print(info);
            g_logger.error("get old value fail, index=%u,size=%u, data=%s", index, sz, info.c_str());
            return false;
        }
        *val_ptr = _oldcols[index];
        return true;
    }

    bool row_t::get_old_value_byindex(uint32_t index, char **val_ptr) {
        uint32_t sz = _oldcols.size();
        if (index >= sz) {
            return false;
        }
        *val_ptr = _oldcols[index];
        return true;
    }

    bool row_t::get_old_value_byindex(uint32_t index, char **val_ptr, bus_log_t *solog) {
        uint32_t sz = _oldcols.size();
        if (index >= sz) {
            std::string info;
            this->print(info);
            solog->error("get old value fail, index=%u,size=%u, data=%s",
                         index, sz, info.c_str());
            return false;
        }
        *val_ptr = _oldcols[index];
        return true;
    }

    bool row_t::get_old_value_byname(char *name, char **val_ptr) {
        assert(NULL != _pnamemap);
        std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
        if (it == (*_pnamemap).end()) {
            return false;
        }

        uint32_t index = it->second;
        uint32_t sz = _oldcols.size();
        if (index >= sz) {
            return false;
        }
        *val_ptr = _oldcols[index];
        return true;
    }

    bool row_t::get_old_value_byname(char *name, char **val_ptr, bus_log_t *solog) {
        assert(NULL != _pnamemap);
        std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
        if (it == (*_pnamemap).end()) {
            solog->error("namemap can not find %s", name);
            return false;
        }

        uint32_t index = it->second;
        uint32_t sz = _oldcols.size();
        if (index >= sz) {
            std::string info;
            this->print(info);
            solog->error("get old value fail, index=%u,size=%u, data=%s",
                         index, sz, info.c_str());
            return false;
        }
        *val_ptr = _oldcols[index];
        return true;
    }

    void row_t::print(string &info) {
        info.reserve(10240);// 为容器预留足够的空间--避免重复分配
        info.append("cols = ");
        std::size_t sz = _cols.size();
        for (std::size_t i = 0; i < sz; ++i) {
            if (_cols[i] != nullptr) info.append(_cols[i]);
            info.append(" ");
        }

        info.append("oldcols = ");
        sz = _oldcols.size();
        for (std::size_t i = 0; i < sz; ++i) {
            if (_oldcols[i] != nullptr) info.append(_oldcols[i]);
            info.append(" ");
        }
    }

    row_t::~row_t() {
        for (auto it = _cols.begin(); it != _cols.end(); ++it) {
            if (*it != nullptr) {
                delete[] *it;// *it为字符串首地址
                *it = nullptr;
            }
        }

        for (auto it = _oldcols.begin(); it != _oldcols.end(); ++it) {
            if (*it != nullptr) {
                delete[] *it;
                *it = nullptr;
            }
        }
    }
}// namespace bus