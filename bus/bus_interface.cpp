#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <mysql/mysql.h>
#include "bus_event.h"
#include "bus_config.h"
#include "bus_util.h"
#include "bus_log.h"
//#include "bus_interface.h"
#include <string>

namespace bus {
    extern bus_log_t g_logger;

    bus_interface::bus_interface() {
//        m_mysql = nullptr;// ???
        memset(m_szIp, 0, sizeof(m_szIp));
        m_nPort = 0;
        memset(m_szUserName, 0, sizeof(m_szUserName));
        memset(m_szPasswd, 0, sizeof(m_szPasswd));
        m_nServerid = 0;
        memset(m_szBinlogFileName, 0, sizeof(m_szBinlogFileName));
        m_uBinlogPos = 4;// 每一个biblog文件的最小有效位置是4

        m_pPacket = nullptr;

        m_uLastKeepAliveTime = 0;
        m_bIsConnected = false;
        m_bIsChecksumEnable = false;

        m_pUserProcess = nullptr;
    }

    bus_interface::~bus_interface() {
        DisConnect();// 断开mysql连接
    }

    int bus_interface::Init(bus_config_t* pConfig, bus_user_process* pUserProcess) {
        int nRet = 0;
        nRet = Init(pConfig->_mysql_ip, pConfig->_mysql_port, pConfig->_mysql_username, pConfig->_redis_passwd,
                    pConfig->_mysql_serverid, pConfig->_password_need_decode, pUserProcess);
        return nRet;
    }

    int bus_interface::Init(const char *szIp, int nPort, const char *szUserName, const char *szPasswd, int nServerid,
                            bool bPasswdNeedDecode, bus_user_process *pUserProcess) {
        int nRet = 0;

        // mysql ip
        struct hostent* pHostEnt = gethostbyname(szIp);// 域名解析到ip
        struct in_addr addr;
        addr.s_addr = *(unsigned long*)pHostEnt->h_addr_list[0];
        char* szRealIp = inet_ntoa(addr);// 网络地址转换成“.”点隔的字符串格式
        // szRealIp写法二
        // char* szRealIP = inet_ntoa(*(struct in_addr*)pHostEnt->h_addr_list[0]);
        strncpy(m_szIp, szRealIp, sizeof(m_szIp));
        // mysql port
        m_nPort = nPort;
        // username
        strncpy(m_szUserName, szUserName, sizeof(m_szUserName));
        // 解密passwd
        //解密password
        if (bPasswdNeedDecode)
        {
            int iLen = 64;
            unsigned char pSwap[64] = {0};
            unsigned char szPassword[512] = {0};
            Base64Decode((unsigned char *)szPasswd,strlen(szPasswd),pSwap,&iLen);
            u_int pOutlen = 0;
            if(!DesEcDncrypt(pSwap,iLen,szPassword,pOutlen,(unsigned char *)"WorkECJol"))
            {
                g_logger.error("Get mysql password error %s\n",szPasswd);
                return false;
            }
            strncpy(m_szPasswd, (const char *)szPassword, sizeof(m_szPasswd));
        }
        else
        {
            strncpy(m_szPasswd, szPasswd, sizeof(m_szPasswd));
        }
        // serverid
        m_nServerid = nServerid;
        m_pUserProcess = pUserProcess;

        // 进行mysql连接
        nRet = Connect();
        if (nRet == -1) {
            g_logger.error("connect mysql error");
            return -1;
        }

        // checksum
        CheckIsChecksumEnable();

        // 初始化/实例化bus_packet_t类对象m_pPacket
        m_pPacket = new bus_packet_t(m_bIsChecksumEnable);
        if (m_pPacket == nullptr) {
            g_logger.error("new bus_packet_t error");
            return -1;
        }

        return 0;
    }

    // 连接mysql
    int bus_interface::Connect() {
        // mysql实例m_mysql初始化
        if (mysql_init(&m_mysql) == nullptr) {
            g_logger.error("fail to init mysql connection");
            return -1;
        }

        // mysql参数设置
        char value = 1;// int value = 1;
        // 设置mysql自动连接--若MYSQL_OPT_RECONNECT参数的value=0，则mysql ping发现连接断开后只返回error，不会重新连接
        mysql_options(&m_mysql, MYSQL_OPT_RECONNECT, (char*)&value);
        // 进行连接
        if (!mysql_real_connect(&m_mysql, m_szIp, m_szUserName, m_szPasswd, nullptr, m_nPort, nullptr, 0)) {
            g_logger.error("fail to connect to mysql, error: %s, ip: %s", mysql_error(&m_mysql), m_szIp);
            return -1;
        }
        g_logger.debug("connected to mysql succeed, fd: %d, ip: %s", m_mysql.net.fd, m_szIp);// mysql也是用socket进行通信

        // 修改m_bIsConnected参数
        m_bIsConnected = true;

        return 0;
    }

    int bus_interface::DisConnect() {
        mysql_close(&m_mysql);
        m_bIsConnected = false;
        return 0;
    }

    // mysql ping
    int bus_interface::KeepAlive() {
        if (m_bIsConnected) {
            time_t uNow = time(0);
            // 每10秒进行mysql ping
            if (uNow - m_uLastKeepAliveTime > 10) {
                mysql_ping(&m_mysql);
                m_uLastKeepAliveTime = uNow;
            }
        } else {
            Connect();// 重新连接
        }

        return 0;
    }

    // IO进程
    // 向数据库服务器发送数据报,请求服务器将binlog文件发过来--是整个文件内容，而不是binlog filename和pos
    int bus_interface::ReqBinlog() {
        int nRet = -1;
        int fd = -1;
        int flags = 0;
        fd = m_mysql.net.fd;// mysql socket fd

        // 关闭mysql checksum
        if (m_bIsChecksumEnable) {
            // 设置mysql binlog checksum
            // mysql5.6.5之前的版本binlog_checksum默认值是none,5.6.5以后的版本中binlog_checksum默认值是crc32
            mysql_query(&m_mysql, "SET @master_binlog_checksum='NONE'");
//            mysql_query(&m_mysql, "SET GLOBAL BINLOG_CHECKSUM = 'NONE'");// mysql5.6.26
        }

        // 获取并修改mysql socket fd文件描述符的状态标志并修改
        if ((flags = fcntl(fd, F_GETFL)) == -1) {
            g_logger.error("fcntl(fd, F_GETFL): %s", strerror(errno));
            return -1;
        }
        flags &= ~O_NONBLOCK;// 修改文件描述符标志
        if (fcntl(fd, F_SETFL, flags) == -1) {
            g_logger.error("fcntl(fd, F_SETFL, flags): %s", strerror(errno));
            return -1;
        }

        // 向数据库服务器发送数据报,请求服务器将binlog发过来
        // binlog file name, binlog pos, server id
        bus::bus_dump_cmd_t dump_cmd(m_szBinlogFileName, m_uBinlogPos, m_nServerid);
        nRet = dump_cmd.write_packet(fd);// 发送数据至mysql socket fd
        if (nRet != 0) {
            g_logger.error("write dump packet fail");
            return -1;
        }

        g_logger.notice("reqbinlog, file: %s, logpos: %lu", m_szBinlogFileName, m_uBinlogPos);

        return 0;
    }

    // 检查mysql checksum
    bool bus_interface::CheckIsChecksumEnable() {
        mysql_query(&m_mysql, "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'");
        MYSQL_RES* pRes = mysql_store_result(&m_mysql);
        if (pRes == nullptr) {
            g_logger.error("fail to mysql_store_result, error: %s", mysql_error(&m_mysql));
            return -1;
        }
        MYSQL_ROW row = mysql_fetch_row(pRes);
        if (row == nullptr) {
            g_logger.error("fail to mysql_fetch_row, error: %s", mysql_error(&m_mysql));
            return -1;
        }
        if (strcmp(row[1], "NONE") == 0) {// checksum == 0 表示不进行checksum一致性检测
            m_bIsChecksumEnable = false;
            return false;
        }

        // 否则，m_bIsChecksumEnable置为true
        m_bIsChecksumEnable = true;
        return true;
    }

    // 读取并解析binlog file内容--内容--包含事件类型和事件内容
    int bus_interface::ReadAndParse(bus_user_process *pUserProcess) {
        int nRet = 0;
        int fd = m_mysql.net.fd;

        // 读取packet
        nRet = m_pPacket->read_packet(fd);
        if (nRet != 0) {
            g_logger.error("m_pPacket->read_packet() error");
            return nRet;
        }

        // 解析packet
        nRet = m_pPacket->parse_packet(pUserProcess);
        if (nRet == -1) {
            g_logger.debug("m_pPacket->parse_packet() error");
            printf("m_pPacket->parse_packet() error\n");
            return nRet;
        } else if (nRet == 1) {
            g_logger.debug("stop signal");
            printf("stop signal\n");
            return nRet;
        } else if (nRet == 2) {
            g_logger.debug("read eof");
            printf("read eof\n");
            return nRet;
        }

        return 0;
    }

    // 获取binlog filename和pos
    // szBinlogFileName和puBinlogPos是传出参数
    int bus_interface::GetNowBinlogPos(char *szBinlogFileName, uint32_t *puBinlogPos) {
        mysql_query(&m_mysql, "show master status");
        MYSQL_RES* pRes = mysql_store_result(&m_mysql);
        if (pRes == nullptr) {
            g_logger.error("fail to mysql_store_result, error: %s", mysql_error(&m_mysql));
            return -1;
        }
        MYSQL_ROW row = mysql_fetch_row(pRes);
        if (row == nullptr) {
            g_logger.error("mysql_fetch_row get empty row");
            return -1;
        }

        // binlog filename
        strncpy(szBinlogFileName, row[0], sizeof(szBinlogFileName));
        // pos
        *puBinlogPos = atol(row[1]);// Convert a string to a long integer

        mysql_free_result(pRes);

        g_logger.notice("get now binlog filename: %s, and pos: %d", szBinlogFileName, *puBinlogPos);

        return 0;
    }

    // 将获取到的binlog filename和pos保存到类成员变量中
    // szBinlogFileName和puBinlogPos是传入参数
    int bus_interface::SetIncrUpdatePos(const char *szBinlogFileName, uint32_t uBinlogPos) {
        // binlog filename
        memset(m_szBinlogFileName, 0, sizeof(m_szBinlogFileName));
        strncpy(m_szBinlogFileName, szBinlogFileName, sizeof(m_szBinlogFileName));
        // pos
        m_uBinlogPos = uBinlogPos;

        // 同步/保存到redis中
        m_pUserProcess->SaveNextreqPos(m_szBinlogFileName, m_uBinlogPos);

        return 0;
    }

#if 0
    int bus_interface::SetBinlogFileName(char *szBinlogFileName)
    {
        memset(m_szBinlogFileName, 0, sizeof(m_szBinlogFileName));
        strncpy(m_szBinlogFileName, szBinlogFileName, sizeof(m_szBinlogFileName));
        return 0;
    }

    int bus_interface::SetBinlogPos(uint32_t uBinlogPos)
    {
        m_uBinlogPos = uBinlogPos;
        return 0;
    }
#endif

    // 读取redis中存储的mysql binlog pos
    int bus_interface::UpdateToNextPos() {
        m_pUserProcess->ReadNextreqPos(m_szBinlogFileName, sizeof(m_szBinlogFileName), &m_uBinlogPos);
    }

}// namespace bus