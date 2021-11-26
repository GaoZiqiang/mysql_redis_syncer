#ifndef BUS_INTERFACE_H
#define BUS_INTERFACE_H

#include <mysql/mysql.h>
#include "bus_config.h"
#include "bus_row.h"
#include "bus_user_process.h"

namespace bus {
    // 类声明--在Init中初始化bus_packet_t类对象实例m_pPacket
    class bus_packet_t;

    class bus_interface {
    public:
        bus_interface();
        virtual ~bus_interface();

        /*
         * @todo 读取配置文件，初始化相关参数，并保存到bus空间的全局变量中
         * @param nServerid mysql服务器的serverid
         * @param bPasswdNeedDecode mysql密码是否需要解码
         * */
        int Init(const char* szIp, int nPort, const char* szUserName, const char* szPasswd, int nServerid, bool bPasswdNeedDecode, bus_user_process* pUserProcess);

        int Init(bus_config_t* pConfig, bus_user_process* pUserProcess);

        // 连接到数据库
        int Connect();

        // 断开数据库
        int DisConnect();

        // 报纸数据库连接
        int KeepAlive();

        // 向数据库服务器发送数据报，请求服务器将binlog file发送过来
        // IO进程
        int ReqBinlog();

        // 判断与mysql交互的协议是否启用了checksum(mysql5.6)
        bool CheckIsChecksumEnable();

        // 读取binlog包，并进行解析
        int ReadAndParse(bus_user_process* pUserProcess);

        // 获取mysql binlog中的最新位置
        int GetNowBinlogPos(char* szBinlogFileName, uint32_t* puBinlogPos);

        // 类中m_szBinlogFileName和m_uBinlogPos成员变量，并同步保存到redis中
        int SetIncrUpdatePos(const char* szBinlogFileName, uint32_t uBinlogPos);

        // 读取redis中存储的mysql binlog pos
        int UpdateToNextPos();

    private:
        MYSQL             m_mysql;                // mysql连接
        char              m_szIp[16];             // mysql ip
        int               m_nPort;                // mysql端口
        char              m_szUserName[24];       // mysql用户名
        char              m_szPasswd[48];         // mysql密码
        int               m_nServerid;            // 数据库的serverid,mysql用它防止解析自身数据库产生的binlog，防止binlog循环解析
        char              m_szConfigFile[100];    // 配置文件路径
        char              m_szLogFilePrefix[100]; // binlog file文件名前缀
        char              m_szBinlogFileName[100];// binlog文件名
        uint32_t          m_uBinlogPos;           // binlog偏移位置--偏移量
        bus_packet_t*     m_pPacket;              // binlog包对象，用于解析拉取到的包
        time_t            m_uLastKeepAliveTime;   // 上次执行mysql ping的时间
        bool              m_bIsConnected;         // mysql是否连接正常
        bool              m_bIsChecksumEnable;    // mysql是否启动checksum机制
        time_t            m_uLastSetPosTime;      // 上次将pos记录到redis的时间
        bus_user_process* m_pUserProcess;
    };// class bus_interface
}// namespace bus

#endif