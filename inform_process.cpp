#include "inform_process.h"

namespace bus {
    extern bus_log_t g_logger;// 多个文件共享该g_logger对象

    bus_inform_process::bus_inform_process() {
        // 类成员变量中指针变量在创建类对象时一定要初始化
        m_pRedis = nullptr;
        m_pBuf = nullptr;
    }

    bus_inform_process::~bus_inform_process() {
        // 释放内存
        if (m_pRedis != nullptr) {
            delete m_pRedis;
            m_pRedis = nullptr;
        }

        if (m_pBuf != nullptr) {
            delete m_pBuf;
            m_pBuf = nullptr;
        }

        // 断开mysql连接
        DisConnectMysql();
    }


    int bus_inform_process::Init(bus_config_t* pConfig) {
        int nRet = 0;
        nRet = Init(pConfig->_mysql_ip, pConfig->_mysql_port, pConfig->_mysql_username,
                    pConfig->_mysql_userpasswd, pConfig->_password_need_decode, pConfig->_redis_ip,
                    pConfig->_redis_port, pConfig->_redis_need_passwd, pConfig->_redis_passwd);
    }

    int bus_inform_process::Init(const char* szMysqlIp, int nMysqlPort, const char* szMysqlUserName,
                                 const char* szMysqlPasswd, bool bPasswdNeedDecode, const char* szRedisIp,
                                 int nRedisPort, bool bRedisNeedPasswd, const char* szRedisPasswd) {
        int nRet = 0;

        m_pBuf = new char[1024 * 100];
        if (m_pBuf == nullptr) {
            g_logger.error("new m_pBuf error");
            printf("new m_pBuf error\n");
            return -1;
        }

        m_pRedis = new MyRedis();
        if (m_pRedis == nullptr) {
            g_logger.error("new MyRedis error");
            printf("new MyRedis error\n");
            return -1;
        }

        // 连接redis
        nRet = m_pRedis->Init(szRedisIp, nRedisPort, bRedisNeedPasswd, szRedisPasswd);
        if (nRet != 0) {
            g_logger.error('redis连接失败');
            printf("redis连接失败\n");
            return nRet;
        }

        // 初始化mysql相关参数
        struct hostent* pHostEnt = gethostbyname(szMysqlIp);
        struct in_addr addr;
        addr.s_addr = *(unsigned long*)pHostEnt->h_addr_list[0];
        char* szRealIp = inet_ntoa(addr);
        strncpy(m_szMysqlIp, szRealIp, sizeof(m_szMysqlIp));// mysql ip

        m_nMysqlPort = nMysqlPort;// mysql port
        strncpy(m_szMysqlUserName, szMysqlUserName, sizeof(m_szMysqlUserName));// mysql username

        //解密password
        if (bPasswdNeedDecode)
        {
            int iLen = 64;
            unsigned char pSwap[64] = {0};
            unsigned char szPassword[512] = {0};
            Base64Decode((unsigned char *)szMysqlPasswd, strlen(szMysqlPasswd), pSwap, &iLen);
            u_int pOutlen = 0;
            if (!DesEcDncrypt(pSwap, iLen, szPassword, pOutlen, (unsigned char *)"WorkECJol"))
            {
                g_logger.error("Get mysql password error %s", szMysqlPasswd);
                return false;
            }
            strncpy(m_szMysqlPasswd, (const char *)szPassword, sizeof(m_szMysqlPasswd));
        }
        else
        {
            strncpy(m_szMysqlPasswd, szMysqlPasswd, sizeof(m_szMysqlPasswd));
        }

        // 连接mysql
        nRet = ConnectMysql();
        if (nRet == -1) {
            g_logger.error("mysql连接失败");
            printf("mysql连接失败");
            return -1;
        }

        return nRet;
    }

    // 全量拉取
    int bus_inform_process::FullPull() {
        g_logger.notice("%s", __FUNCTION__);
        FullPullFriended();
        FullPullStaff();
        FullPullOnlineCs();
        FullPullOnlineCsLeader();

        return 0;
    }

    // 增量拉取
    int bus_inform_process::IncrProcess(row_t* row) {
        g_logger.notice("%s", __FUNCTION__);
        printf("Now in %s\n", __FUNCTION__);
        const char* db_name = row->get_db_name();
        const char* table_name = row->get_table();

        if (strcmp(db_name, "d_ec_user") == 0 && strcmp(table_name, "t_user_group") == 0) {
            IncrProcessFriendedAndStaff(row);// 对t_user_group表进行增量拉取
        } else {
            printf("对其他表进行增量拉取，暂时还未实现\n");
        }
    }

    // 将mysql当前解析到的binlog位置存储到redis
    int bus_inform_process::SaveNextreqPos(const char* szBinlogFileName, uint32_t uBinlogPos) {
        if (m_pRedis == nullptr)
            return -1;// redis连接断开

        if (szBinlogFileName[0] == 0 || uBinlogPos == 0)
            return 0;// mysql还没有生成binlog file

        g_logger.debug("save nextreq  logpos in redis, filename:%s, logpos:%u", szBinlogFileName, uBinlogPos);

        // 重连后请求的位置，存入redis
        m_pRedis->RedisCmdNoReply("hmset informhubsvr_binlog_pos_hash filename %s pos %d", szBinlogFileName, uBinlogPos);

        return 0;
    }

    // 读出redis中存放的上一次解析到的binlog位置，用于重新继续解析--本次解析失败
    int bus_inform_process::ReadNextreqPos(char* szBinlogFileName, uint32_t uFileNameLen, uint32_t* puBinlogPos) {
        if (m_pRedis == nullptr)
            return -1;// redis断开连接

        // 查询redis
        int ret = m_pRedis->RedisCmdWithReply("hmget informhubsvr_binlog_pos_hash filename pos");
        if (ret < 0) {
            g_logger.error("hmget informhubsvr_binlog_pos_hash filename pos失败");
            printf("hmget informhubsvr_binlog_pos_hash filename pos失败\n");
            return -1;
        }
        redisReply* pReply = m_pRedis->GetReply();// 获取redis查询结果
        if (REDIS_REPLY_NIL == pReply->element[0]->type || REDIS_REPLY_NIL == pReply->element[1]->type) {
            g_logger.error("fail to get logpos from redis, not exists key");
            printf("fail to get logpos from redis, not exists key");
            return -1;
        }

        // 将redis查询结果赋值给szBinlogFileName和puBinlogPos
        strncpy(szBinlogFileName, pReply->element[0]->str, uFileNameLen);// szBinlogFileName
        *puBinlogPos = atol(pReply->element[1]->str);// puBinlogPos
        g_logger.debug("read nextreq  logpos from redis. filename:%s, logpos:%u", szBinlogFileName, *puBinlogPos);

        return 0;
    }


    // 从mysql中全量拉取好友关系并存入redis
    int bus_inform_process::FullPullFriended() {
        printf("全量拉取好友关系\n");
        g_logger.notice("%s", __FUNCTION__);
        g_logger.notice("全量拉取好友关系");

        int nRet = 0;
        uint32_t uLimitOffset = 0;
        uint32_t uLimitCount = 5000;// 单次全量拉取记录条数上限
        int nUserId = 0;
        int nGroupId = 0;
        int nType = 0;
        MYSQL_RES* pRes = nullptr;// mysql查询结果
        MYSQL_ROW row = nullptr;// 查询结果--行
        int nCurRowCount = 0;
        int nAppendRedisPipeCount = 0;

        KeepMysqlAlive();// 保证mysql保持连接

        // 单次全量拉取记录条数上限为uLimitCount，可能需要多次同步才能将mysql中的数据全部同步完
        while (1) {
            snprintf(m_pBuf, 1024,
                     "select f_id, f_group_id, f_type from d_ec_user.t_user_group where f_type = 0 limit %u, %u",
                     uLimitOffset, uLimitCount);
            // 进行mysql查询
            nRet = mysql_real_query(&m_mysql, m_pBuf, strlen(m_pBuf));
            if (nRet != 0) {// 查询失败
                printf("failed to mysql_real_query, errstr: %s", mysql_error(&m_mysql));
                g_logger.error("failed to mysql_real_query, errstr: %s", mysql_error(&m_mysql));
                return -1;
            }


            // mysql查询结果
            pRes = mysql_store_result(&m_mysql);
            if (pRes == nullptr) {
                if (mysql_errno(&m_mysql) != 0) {
                    g_logger.error("failed to mysql_store_result, errstr: %s", mysql_error(&m_mysql));
                    printf("failed to mysql_store_result, errstr: %s", mysql_error(&m_mysql));
                    return -1;
                } else {
                    // 原因存疑
                    g_logger.notice("没有数据了,errstr: %s", mysql_errno(&m_mysql));
                    printf("没有数据了,errstr: %s", mysql_errno(&m_mysql));
                    break;// 跳出while
                }
            }

            nCurRowCount = mysql_num_rows(pRes);// 查询结果行数
            g_logger.notice("查询出的行数: %d", nCurRowCount);
            uLimitOffset += nCurRowCount;// 更新uLimitOffset
            if (nCurRowCount == 0) {
                g_logger.notice("nCurRowCount = 0");
                mysql_free_result(pRes);// 释放结果集pRes
                break;// 跳出while--没有数据了
            }

            //  每一次mysql_fetch_row()都获得当前行数据库，并赋值给数组row，然后自动滑向下一行；
            //  在取出最后一行后，函数将返回false，循环结束
            while ((row = mysql_fetch_row(pRes)) != nullptr) {
                nUserId = atoi(row[0]);
                nGroupId = atoi(row[1]);
                nType = atoi(row[2]);
                g_logger.notice("查询得到nGroupId: %d, nUserId: %d, nType: %d\n", nGroupId, nUserId, nType);
                // 存入redis
                // 使用Pipeline执行redis命令--提高命令执行速度，降低开销
                m_pRedis->RedisPipeAppendCmd("sadd friended_set:%d %d", nGroupId, nUserId);
                // redis连续插入超过100次？
                if (++nAppendRedisPipeCount >= 100) {
                    m_pRedis->RedisPipeGetResult();// 重连接redis
                    nAppendRedisPipeCount = 0;
                }
            }

            if (nAppendRedisPipeCount > 0) {
                m_pRedis->RedisPipeGetResult();
                nAppendRedisPipeCount = 0;
            }

            mysql_free_result(pRes);// 释放查询结果集
            pRes = nullptr;
        }
        g_logger.notice("全量拉取好友关系完成");
        printf("全量拉取好友关系完成\n");
        return 0;
    }

    // 全量拉取OnlineCs
    int bus_inform_process::FullPullOnlineCs() {
        printf("%s\n", __FUNCTION__);
    }

    int bus_inform_process::FullPullStaff() {
        printf("%s\n", __FUNCTION__);
    }
    int bus_inform_process::FullPullOnlineCsLeader() {
        printf("%s\n", __FUNCTION__);
    }

    // 增量拉取
    int bus_inform_process::IncrProcessFriendedAndStaff(row_t *row) {
        if (row->size() != 3) {
            g_logger.error("row->sizze() != 3, return %d", row->size());
            return -1;
        }

        char* p = nullptr;// 暂存
        int nUserId = 0;
        int nGroupId = 0;
        int nType = 0;
        int nOldUserId = 0;
        int nOldGroupId = 0;
        int nOldType = 0;

        // 获取发生变化的数据--添加/删除/更新
        if (row->get_value(0, &p))
            nUserId = atoi(p);
        else {
            g_logger.error("row->get_value(0) error, p: %p", p);
            return -1;
        }

        if (row->get_value(1, &p))
            nGroupId = atoi(p);
        else {
            g_logger.error("row->get_value(1), p: %p", p);
            return -1;
        }

        if (row->get_value(2, &p))
            nType = atoi(p);
        else {
            g_logger.error("row->get_value(2), p: %p", p);
            return -1;
        }

        // 根据nType分别进行修改
        g_logger.notice("new value, userid: %d, groupid: %d, type: %d", nUserId, nGroupId, nType);
        if (nType == 0) {// 修改好友数据
            g_logger.notice("nType: %d", nType);
            printf("修改好友关系数据\n");
            // mysql为添加操作
            if (row->get_action() == 0) {
                g_logger.notice("sadd friended_set:%d %d", nGroupId, nUserId);
                // 存入redis
                m_pRedis->RedisCmdNoReply("sadd friended_set:%d %d", nGroupId, nUserId);
            } else if (row->get_action() == 1) {// mysql更新操作
                // 先获取旧值--删除之再存入新的
                if (row->get_old_value(0, &p))
                    nOldUserId = atoi(p);
                else
                    return -1;
                if (row->get_old_value(1, &p))
                    nOldGroupId = atoi(p);
                else
                    return -1;
                if (row->get_old_value(2, &p))
                    nOldType = atoi(p);
                else
                    return -1;

                g_logger.notice("old value, userid: %d, groupid: %d, type: %d", nOldUserId, nOldGroupId, nOldType);
                // redis存入更新后的值
                if (nType == nOldType) {// type类型不能变
                    g_logger.notice("srem friended_set:%d %d", nOldGroupId, nOldUserId);
                    g_logger.notice("sadd friended_set:%d %d", nGroupId, nUserId);
                    m_pRedis->RedisCmdNoReply("srem friended_set:%d %d", nOldGroupId, nOldUserId);// 先删除旧值
                    m_pRedis->RedisCmdNoReply("sadd friended_set:%d %d", nGroupId, nUserId);
                }
            } else if (row->get_action() == 2) {// mysql删除操作
                g_logger.notice("srem friended_set:%d %d", nGroupId, nUserId);
                m_pRedis->RedisCmdNoReply("srem friended_set:%d %d", nGroupId, nUserId);
            }
        } else if (nType == 2) {// 修改同事关系
            g_logger.notice("nType: %d", nType);
            printf("修改同事关系数据\n");
            // mysql为添加操作
            if (row->get_action() == 0) {
                g_logger.notice("sadd group_staff_set:%d %d", nGroupId, nUserId);
                g_logger.notice("set user_groupid:%d %d", nUserId, nGroupId);
                m_pRedis->RedisCmdNoReply("sadd group_staff_set:%d %d", nGroupId, nUserId);
                m_pRedis->RedisCmdNoReply("set user_groupid:%d %d", nUserId, nGroupId);
            } else if (row->get_action() == 1) {// mysql更新操作
                // 先获取旧值--删除之再存入新的
                if (row->get_old_value(0, &p))
                    nOldUserId = atoi(p);
                else
                    return -1;
                if (row->get_old_value(1, &p))
                    nOldType = atoi(p);
                else
                    return -1;
                if (row->get_old_value(2, &p))
                    nOldGroupId = atoi(p);
                else
                    return -1;

                g_logger.notice("old value, userid: %d, groupid: %d, type: %d", nOldUserId, nOldGroupId, nOldType);
                // 存入更新后的值
                if (nType == nOldType) {// type类型不能变
                    g_logger.notice("srem group_staff_set:%d %d", nGroupId, nUserId);
                    g_logger.notice("del user_groupid:%d %d", nUserId, nGroupId);
                    g_logger.notice("sadd group_staff_set:%d %d", nGroupId, nUserId);
                    g_logger.notice("set user_groupid:%d %d", nUserId, nGroupId);
                    m_pRedis->RedisCmdNoReply("srem group_staff_set:%d %d", nGroupId, nUserId);
                    m_pRedis->RedisCmdNoReply("del user_groupid:%d %d", nUserId, nGroupId);
                    m_pRedis->RedisCmdNoReply("sadd group_staff_set:%d %d", nGroupId, nUserId);
                    m_pRedis->RedisCmdNoReply("set user_groupid:%d %d", nUserId, nGroupId);
                }
            } else if (row->get_action() == 2) {// mysql删除操作
                g_logger.notice("srem group_staff_set:%d %d", nGroupId, nUserId);
                g_logger.notice("del user_groupid:%d %d", nUserId, nGroupId);
                m_pRedis->RedisCmdNoReply("srem group_staff_set:%d %d", nGroupId, nUserId);
                m_pRedis->RedisCmdNoReply("del user_groupid:%d %d", nUserId, nGroupId);
            }
        }

        return 0;
    }


    int bus_inform_process::IncrProcessOnlineCs(row_t *row) {
        printf("%s\n", __FUNCTION__);
    }

    int bus_inform_process::IncrProcessCsLeader(row_t *row) {
        printf("%s\n", __FUNCTION__);
    }

    int bus_inform_process::GetCorpid(uint32_t *puCorpid, uint32_t *puCorpidCount, uint32_t uLimitOffset, uint32_t uLimitCount)
    {
        int nRet = 0;
        int nCurRowCount = 0;
        MYSQL_RES *pRes = NULL;
        MYSQL_ROW row = NULL;

        KeepMysqlAlive();
        snprintf(m_pBuf, 1024, "select f_corp_id from d_ec_corp.t_corp_info limit %u, %u", uLimitOffset, uLimitCount);
        nRet = mysql_real_query(&m_mysql, m_pBuf, strlen(m_pBuf));
        if (nRet != 0)
        {
            g_logger.error("mysql_real_query error.errstr:%s", mysql_error(&m_mysql));
            return -1;
        }

        pRes = mysql_store_result(&m_mysql);
        if (pRes == NULL)
        {
            if (mysql_errno(&m_mysql) != 0)
            {
                g_logger.error("Failed to mysql_store_rsult Error: %s", mysql_error(&m_mysql));
                return -1;
            }
            else
            {
                return 0;
            }
        }

        *puCorpidCount = mysql_num_rows(pRes);
        if (*puCorpidCount == 0)
        {
            mysql_free_result(pRes);
            return 0;
        }

        while ((row = mysql_fetch_row(pRes)) != NULL)
        {
            puCorpid[nCurRowCount] = atoi(row[0]);
            nCurRowCount++;
        }

        mysql_free_result(pRes);
        pRes = NULL;

        return *puCorpidCount;
    }

    int bus_inform_process::GetCSManagerAndCreater(uint32_t uCorpId)
    {
        int nRet = 0;
        MYSQL_RES *pRes = NULL;
        MYSQL_ROW row = NULL;
        int nCsId = 0;
        int nRoleId = 0;
        int nAppendRedisPipeCount = 0;

        KeepMysqlAlive();

        snprintf(m_pBuf,
                 1024,
                 "select a.f_id, a.f_group_id, b.f_role_id from d_ec_user.t_user_group a INNER JOIN d_ec_role.t_account_role b on a.f_id = b.f_user_id where a.f_group_id = %u and a.f_type = 2 and b.f_role_id in (7, 9)",
                 uCorpId);

        nRet = mysql_real_query(&m_mysql, m_pBuf, strlen(m_pBuf));
        if (nRet != 0)
        {
            g_logger.error("mysql_real_query error.errstr:%s", mysql_error(&m_mysql));
            return -1;
        }

        pRes = mysql_store_result(&m_mysql);
        if (pRes == NULL)
        {
            if (mysql_errno(&m_mysql) != 0)
            {
                g_logger.error("Failed to mysql_store_rsult Error: %s", mysql_error(&m_mysql));
                return -1;
            }
            else
            {
                return 0;
            }
        }

        while ((row = mysql_fetch_row(pRes)) != NULL)
        {
            nCsId = atoi(row[0]);
            nRoleId = atoi(row[2]);
            //m_pRedis->RedisCmdNoReply("set cs_corpid:%d %d", nCsId, uCorpId);
            //m_pRedis->RedisCmdNoReply("set cs_role:%d %d", nCsId, nRoleId);
            //m_pRedis->RedisCmdNoReply("sadd corp_cs_set:%d %d", uCorpId, nCsId);

            m_pRedis->RedisPipeAppendCmd("set cs_corpid:%d %d", nCsId, uCorpId);
            m_pRedis->RedisPipeAppendCmd("set cs_role:%d %d", nCsId, nRoleId);
            m_pRedis->RedisPipeAppendCmd("sadd corp_csleader_set:%d %d", uCorpId, nCsId);
            if (++nAppendRedisPipeCount >= 100)
            {
                m_pRedis->RedisPipeGetResult();
                nAppendRedisPipeCount = 0;
            }
        }

        if (nAppendRedisPipeCount > 0)
        {
            m_pRedis->RedisPipeGetResult();
            nAppendRedisPipeCount = 0;
        }
        mysql_free_result(pRes);
        pRes = NULL;
        return 0;
    }

    int bus_inform_process::GetCorpidByUserid(int nUserId, int *pnCorpId)
    {
        int nRet = 0;
        MYSQL_RES *pRes = NULL;
        MYSQL_ROW row = NULL;

        KeepMysqlAlive();

        snprintf(m_pBuf, 1024, "select f_group_id from d_ec_user.t_user_group where f_id=%d and f_type=2", nUserId);

        nRet = mysql_real_query(&m_mysql, m_pBuf, strlen(m_pBuf));
        if (nRet != 0)
        {
            g_logger.error("mysql_real_query error.errstr:%s", mysql_error(&m_mysql));
            return -1;
        }

        pRes = mysql_store_result(&m_mysql);
        if (pRes == NULL)
        {
            if (mysql_errno(&m_mysql) != 0)
            {
                g_logger.error("Failed to mysql_store_rsult Error: %s", mysql_error(&m_mysql));
                return -1;
            }
            else
            {
                return 0;
            }
        }

        if ((row = mysql_fetch_row(pRes)) != NULL)
        {
            *pnCorpId = atoi(row[0]);
        }

        mysql_free_result(pRes);
        pRes = NULL;
        return 0;
    }

    int bus_inform_process::ConnectMysql()
    {
        if(mysql_init(&m_mysql) == NULL)
        {
            g_logger.error("Failed to initate MySQL connection");
            return -1;
        }
        char value = 1;
        mysql_options(&m_mysql, MYSQL_OPT_RECONNECT, (char *)&value);
        if(!mysql_real_connect(&m_mysql, m_szMysqlIp, m_szMysqlUserName, m_szMysqlPasswd, NULL, m_nMysqlPort, NULL, 0))
        {
            g_logger.error("Failed to connect to MySQL: Error: %s, ip:%s", mysql_error(&m_mysql), m_szMysqlIp);
            return -1;
        }
        g_logger.debug("connected mysql succeed, fd:%d, ip:%s",m_mysql.net.fd, m_szMysqlIp);

        m_bIsMysqlConnected = true;
        return 0;
    }

    int bus_inform_process::DisConnectMysql()
    {
        mysql_close(&m_mysql);
        m_bIsMysqlConnected = false;
        return 0;
    }

    int bus_inform_process::KeepMysqlAlive()
    {
        if (m_bIsMysqlConnected == true)
        {
            time_t uNow = time(0);
            if (uNow - m_uLastKeepAliveTime > 10)
            {
                mysql_ping(&m_mysql);
                m_uLastKeepAliveTime = uNow;
            }
        }
        else
        {
            ConnectMysql();
        }

        return 0;
    }
}