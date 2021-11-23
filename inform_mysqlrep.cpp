#include <unistd.h>
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

// 守护进程--记录当前程序运行的进程号
int Daemon() {
    pid_t pid = 0;
    pid = fork();// 创建子进程
    if (pid > 0)
        exit(0);// 父进程退出

    // 父子进程分离
    setsid();// 父子进程分离，父进程退出时不影响子进程

    // 给予进程创建文件的最大权限
    umask(0);// (~0) & mode-->0777 & mode
    // ???
    for (int i = 0; i < 3; i++)
        close(i);

    FILE *p = NULL;
    p = fopen("inform_mysqlrep.pid", "w+");
    if (p == NULL)
        return -1;
    char szBuf[128] = {0};

    // 获取进程号
    snprintf(szBuf, sizeof(szBuf), "%d", getpid());
    // 将进程号写入文件
    fwrite(szBuf, 1, strlen(szBuf), p);
    fclose(p);

    return 0;
}

// 信号处理函数handler
void SigAction(int sig, siginfo_t* pInfo, void* pContext) {
    bus::g_logger.notice("%s, got sig %d", __FUNCTION__, sig);
    if (sig == SIGINT || sig == SIGTERM) {// 进程终止信号
        remove("inform_mysqlrep.pid");
        exit(0);// 异常退出
    }
}

// 信号注册
int RegisterSignal(int nSignal) {
    struct sigaction sa;// 使用sigaction信号处理器
    // 绑定信号处理函数
    sa.sa_sigaction = SigAction;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    // 注册信号nSignal
    if (sigaction(nSignal, &sa, NULL) == -1) {
        bus::g_logger.error("%s, register signal error", __FUNCTION__);
        return -1;
    }

    return 0;
}

int main(int argc, char** argv) {
    // 调试程序
#if 0
    char szIp[16] = "127.0.0.1";
    int nPort = 3306;
    char szUserName = "root";
    chat szPasswd[] = "root";
    int nMysqlServerid = 1;
#endif

    int nRet = 0;
    bool bDaemon = false;// 不记录进程
    uint32_t uLastFullPullTime = time(0);// 上一次全量同步时间
    uint32_t uNowTime = 0;
    uint32_t uFullPullInternal = 20;// 一周全量同步一次，保证数据同步

    // 检查文件是否存在 mode == 0
    if (access("inform_mysqlrep.pid", 0) != -1) {
        printf("pid file exit, process already exist\n");
        return 0;
    } else {
        printf("pid file not exist\n");
    }

    // 读取终端参数
    for (int i = 0; i < argc; i++) {
        if (strstr(argv[i], "-daemon"))
            bDaemon = true;// 打开守护进程
    }

    if (bDaemon)
        Daemon();// 开启守护进程

    // 注册信号
    RegisterSignal(SIGINT);
    RegisterSignal(SIGTERM);

    // 初始化logger
    bus::g_logger.init();
    bus::g_logger.set_loglevel(LOG_NOTICE);

    // 读取配置文件--获取数据库配置信息
    bus::g_config.init_conffile("inform_mysqlrep.ini");
    // 解析配置文件
    nRet = bus::g_config.parase_config_file();
    if (nRet == -1) {
        bus::g_logger.error("parase config file inform_mysqlrep.ini error");
        return -1;
    }

    // 数据库管理接口
    bus::bus_interface *pInterface = new bus::bus_interface();
    // 用户业务接口
    bus::bus_inform_process *pUserProcess = new bus::bus_inform_process();

    // 数据库配置信息初始化
    nRet= pInterface->Init(&bus::g_config, pUserProcess);
    if (nRet != 0)
        return nRet;
    printf("完成数据库管理层配置信息初始化\n");

    // 用户业务配置信息初始化
    nRet = pUserProcess->Init(&bus::g_config);
    if (nRet != 0)
        return nRet;
    printf("完成用户业务层配置信息初始化\n");

    char szBinlogFileName[100] = {0};
    uint32_t uBinlogPos = 0;

    // 读取redis中存储的mysql biglog pos--szBinlogFileName和uBinlogPos为传出参数
    pUserProcess->ReadNextreqPos(szBinlogFileName, sizeof(szBinlogFileName), &uBinlogPos);

    // 首次同步-->初始化redis中存储的mysql binlog pos
    if (szBinlogFileName[0] == 0 || uBinlogPos ==0) {
        // 获取mysql数据的binlog file name和binlog pos
        pInterface->GetNowBinlogPos(szBinlogFileName, &uBinlogPos);
        // 更新/同步/保存到redis数据库
        pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
        bus::g_logger.notice("not find last binlog pos in redis, set now pos and full pull to keep cache new.");
        printf("redis中没有mysql中last binlog pos-->更新redis中的binlog pos并做一个全量拉取同步两数据库\n");
        // 做一个全量拉取--第一次同步两数据库
        pUserProcess->FullPull();
    } else {
        pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
        bus::g_logger.notice("find last binlog pos in redis, start here incr pull. binlog:%s, pos:%u", szBinlogFileName, uBinlogPos);
        printf("查询到redis中的binlog pos,将从此处进行增量拉取\n");
    }

    uint32_t uReconnectCount = 0;


    while (true) {
        uNowTime = time(0);
        // 一周全量拉取一次
        if (uNowTime - uLastFullPullTime >= uFullPullInternal) {
            printf("进行全量拉取\n");
            pUserProcess->FullPull();// 全量拉取
            printf("全量拉取结束\n");
            uLastFullPullTime = uNowTime;
        }

        // 心跳检测--每10s一次
        pInterface->KeepAlive();

        nRet = pInterface->ReqBinlog();
        if (nRet != 0)
            continue;

        int i = 0;
        while (true) {
            // 读取&解析binlog,并将binlog变化同步到redis
            nRet = pInterface->ReadAndParse(pUserProcess);

            if (nRet != 0) {
                // 解析mysql binlog失败少于三次--重新解析一次
                if (uReconnectCount++ < 3) {// reconnect少于三次--
                    printf("pInterface->ReadAndParse失败少于3次\n");
                    // 重新解析一次mysql binlog，因此需要回到上一次解析到的mysql binlog位置--从redis数据库中查找
                    pInterface->UpdateToNextPos();// 更新pUserProcess的szBinlogFileName和uBinlogPos这两个参数
                } else {// reconnect多于三次--重新同步mysql binlog pos和redis中存储的binlog pos
                    printf("pInterface->ReadAndParse失败多于3次\n");
                    // 同步mysql和redis
                    pInterface->GetNowBinlogPos(szBinlogFileName, &uBinlogPos);
                    pInterface->SetIncrUpdatePos(szBinlogFileName, uBinlogPos);
                }
                pInterface->DisConnect();
                sleep(1);
                break;
            } else {
                uReconnectCount = 0;
            }
        }
    }
    return 0;
}