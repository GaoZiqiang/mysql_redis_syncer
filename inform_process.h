// 用户业务实现接口
#ifndef INFORM_PROCESS_H
#define INFORM_PROCESS_H

#include <unistd.h>
#include <netdb.h>
#include <mysql/mysql.h>
#include "myredis.h"
#include "bus/bus_user_process.h"
#include "bus/bus_config.h"

namespace bus {
class bus_inform_process : public bus_user_process {
    public:
        bus_inform_process();
        virtual ~bus_inform_process();

        // mysql redis数据库连接初始化
        // 该Init()接口实际调用下面的Init()接口-->Init(pConfig->_mysql_ip,...)
        // 为什么不直接调用Init(pconfig->_mysql_ip,...)?
        // 先使用Init(bus_config_t* pConfig)使接口整洁简化
        int Init(bus_config_t* pConfig);
        int Init(const char* szMysqlIp, int nMysqlPort, const char* szMysqlUserName,
                 const char* szMysqlPasswd, bool bPasswdNeedDecode, const char* szRedisIp,
                 int nRedisPort, bool bRedisNeedPasswd, const char* szRedisPasswd);

        // 全量拉取
        virtual int FullPull();
        // 增量拉取--拉取到增量数据后的回调函数--将mysql中的增量数据同步到redis
        virtual int IncrProcess(row_t* row);
        // 将mysql当前解析到的binlog位置存储到redis(两个参数：szBinlogFileName和uBinlogPos)--对于redis而言就是nextreqPos下一次要解析到的地方
        virtual int SaveNextreqPos(const char* szBinlogFileName, uint32_t uBinlogPos);
        // 读出redis中存放的上一次解析到的binlog位置，用于重新继续解析--本次解析失败
        virtual int ReadNextreqPos(char* szBinlogFileName, uint32_t uFileNameLen, uint32_t* puBinlogPos);

        int FullPullFriended();
        int FullPullStaff();
        int FullPullOnlineCs();
        int FullPullOnlineCsLeader();

        int IncrProcessFriendedAndStaff(row_t* row);
        int IncrProcessOnlineCs(row_t* row);
        int IncrProcessCsLeader(row_t* row);

        int GetCorpid(uint32_t* puCorpid, uint32_t* puCorpidCount, uint32_t uLimitOffset, uint32_t uLimitCout);
        int GetCSManagerAndCreater(uint32_t uCorpid);
        int GetCorpidByUserid(int nUserId, int* pnCorpid);

        int ConnectMysql();
        int DisConnectMysql();
        int KeepMysqlAlive();

    private:
        MYSQL m_mysql;// mysql连接
        char m_szMysqlIp[16];// mysqlip
        int m_nMysqlPort;// mysql port
        char m_szMysqlUserName[24];// mysql username
        char m_szMysqlPasswd[48];// mysql passwd
        bool m_bIsMysqlConnected;// 数据库是否正常连接
        time_t m_uLastKeepAliveTime;// 上次执行mysql ping的时间

        MyRedis* m_pRedis;// MyRedis类对象
        char* m_pBuf;

    };
}
#endif