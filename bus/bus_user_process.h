#ifndef USER_PROCESS_H
#define USER_PROCESS_H

#include "bus_row.h"

namespace bus {
    // 业务操作接口ABC--抽象接口
    class bus_user_process {
    public:
        // 全量拉取
        virtual int FullPull() = 0;
        // 增量拉取
        virtual int IncrProcess(row_t* row) = 0;
        // 保存当前binlog filename和pos--调用bus_interface.h中的binlog filename pos接口
        virtual int SaveNextreqPos(const char* szBinlogFileName, uint32_t uBinlogPos) = 0;
        // 读取上一次解析到的binlog filename和pos
        virtual int ReadNextreqPos(char* szBinlogFileName, uint32_t uFileNameLen, uint32_t* uBinlogPos) = 0;
    };
}
#endif