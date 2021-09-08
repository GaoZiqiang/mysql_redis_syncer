#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <signal.h>
#include <mysql/mysql.h>
#include "bus/bus_event.h"
#include "bus/bus_config.h"
#include "bus/bus_util.h"
#include "bus/bus_log.h"

#include "bus/bus_interface.h"
#include "bus/bus_user_process.h"
#include "inform_process.h"

extern bus::bus_log_t g_logger;
extern bus::bus_config_t g_config;

int Daemon()
{
    pid_t pid = 0;
    pid = fork();
    if (pid > 0)
        exit(0);

    setsid();
    umask(0);
    for (int i = 0; i < 3; i++)
        close(i);

    FILE *p = NULL;
    p = fopen("inform_mysqlrep.pid", "w+");
    if (p == NULL)
    {
        return -1;
    }
    char szBuf[128] = {0};
    snprintf(szBuf, sizeof(szBuf), "%d", getpid());
    fwrite(szBuf, 1, strlen(szBuf), p);
    fclose(p);

    return 0;
}

void SigAction(int sig, siginfo_t *pInfo, void *pContext)
{
    bus::g_logger.notice("%s, got sig %d", __FUNCTION__, sig);
    if (sig == SIGINT || sig == SIGTERM)
    {
        remove("inform_mysqlrep.pid");
        exit(0);
    }
}

int RegisterSignal(int nSignal)
{
    struct sigaction sa;
    sa.sa_sigaction = SigAction;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    if (sigaction(nSignal, &sa, NULL) == -1)
    {
        bus::g_logger.error("%s, register signal error", __FUNCTION__);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
#if 0
    // 这里的0来自哪？
    char szIp[16] = "127.0.0.1";
    int nPort = 3306;
    char szUserName[] = "root";
    char szPasswd[] = "root";
    int nMysqlServerid = 1;
#endif

    int nRet = 0;
    bool bDaemon = false;
    uint32_t uLastFullPullTime = time(0);
    uint32_t uNowTime = 0;
//    uint32_t uFullPullInternal = 604800;  //3600 * 24 * 7 ，一周全量拉取一次，保证数据同步
    uint32_t uFullPullInternal = 10;  //每隔5s全量拉取一次，保证数据同步

    if (access("inform_mysqlrep.pid", 0) != -1)
    {
        printf("pid file exist, process already exist\n");
        return 0;
    } else {
        printf("pid file not exist\n");
    }

    for (int i = 0; i < argc; i++)
    {
        if (strstr(argv[i], "-daemon"))
            bDaemon = true;
    }

    if (bDaemon)
    {
        Daemon();
    }
    
    RegisterSignal(SIGINT);
    RegisterSignal(SIGTERM);


    bus::g_logger.init();
    //bus::g_logger.set_loglevel(LOG_DEBUG);
    bus::g_logger.set_loglevel(LOG_NOTICE);

    // 读入配置文件
    bus::g_config.init_conffile("inform_mysqlrep.ini");
    nRet = bus::g_config.parase_config_file();
    if (nRet == -1)
    {
        bus::g_logger.error("parase config file error");
        return -1;
    }


    bus::bus_interface *pInterface = new bus::bus_interface();
    bus::bus_inform_process *pUserProcess = new bus::bus_inform_process();

    // 初始化g_config和新进程
    nRet = pInterface->Init(&bus::g_config, pUserProcess);
    if (nRet != 0)
        return nRet;

    // 初始化mysql信息
    nRet = pUserProcess->Init(&bus::g_config);
    if (nRet != 0)
        return nRet;
    printf("已经完成nRet = pUserProcess->Init(&bus::g_config);\n");
    //printf("bus::g_config._mysql_ip: %s,bus::g_config._mysql_username: %s\n",bus::g_config._mysql_ip,bus::g_config._mysql_username);

    char szBinlogFileName[100] = {0};
    uint32_t uBinlogPos = 0;

    // 先读取nextreqPos
    pUserProcess->ReadNextreqPos(szBinlogFileName, sizeof(szBinlogFileName), &uBinlogPos);

    // 进行判断
    // 没有--就代表没变化？
    if (szBinlogFileName[0] == 0 || uBinlogPos == 0)
    {
        pInterface->GetNowBinlogPos(szBinlogFileName, &uBinlogPos);
        // 更新
        pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
        bus::g_logger.notice("not find last binlog pos in redis, set now pos and full pull to keep cache new.");
        printf("szBinlogFileName读取失败!\n");
        // 全量更新
        printf("not find last binlog pos in redis, set now pos and full pull to keep cache new.\n");
        pUserProcess->FullPull();
    }
    // 有就代表有变化？
    else
    {
        pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
        bus::g_logger.notice("find last binlog pos in redis, start here incr pull. binlog:%s, pos:%u", szBinlogFileName, uBinlogPos);
        printf("szBinlogFileName读取成功!\n");
        printf("find last binlog pos in redis, start here incr pull. binlog:%s, pos:%u\n", szBinlogFileName, uBinlogPos);
    }

    //pInterface->SetIncrUpdatePos("mysql-bin.000011", 4);

    uint32_t uReconnectCount = 0;

    while (true)
    {
        uNowTime = time(0);
        // 一周全量拉取一次
        if (uNowTime - uLastFullPullTime >= uFullPullInternal)// 一周全量拉取一次
        {
            pUserProcess->FullPull();
            uLastFullPullTime = uNowTime;
        }

        pInterface->KeepAlive();

        // 拉取Binlog--没问题
        nRet = pInterface->ReqBinlog();
        if (nRet != 0)
        {
            continue;
        } else {
            printf("Request拉取Binlog成功!\n");
        }

        int i = 0;
        while (true)
        {
//            if (++i > 2) {
//                printf("退出增量拉取的循环\n");
//                //printf("读取Binlog多于3次!\n");
//                pInterface->GetNowBinlogPos(szBinlogFileName, &uBinlogPos);
//                pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
//
//                break;
//            }
            // 读取&解析Binlog
            nRet = pInterface->ReadAndParse(pUserProcess);
            printf("-------- main函数中第 %d 次读取ReadAndParse\n",++i);
            if (nRet != 0)
            {
//                printf("main主函数中--读取&解析Binlog失败!\n");
                if (uReconnectCount++ < 3)
                {
                    printf("main主函数中--读取&解析Binlog失败,并进行UpdateToNextPos!\n");
                    pInterface->UpdateToNextPos();
                }
                else// reconnect多于三次
                {
                    // 更新当前BinlogPos-为什么？
                    printf("main主函数中--读取&解析Binlog失败,且读取Binlog多于3次!\n");
                    pInterface->GetNowBinlogPos(szBinlogFileName, &uBinlogPos);
                    pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
                }

                pInterface->DisConnect();
                //bus::g_logger.notice("reconnect, count:%d", uReconnectCount);
                sleep(1);
                break;
            }
            else
            {
                printf("主函数中pInterface->ReadAndParse()成功!\n");
                uReconnectCount = 0;
            }
        }
    }
    return 0;
}

