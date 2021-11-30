#include <stdarg.h>
#include <string.h>
#include "myredis.h"

namespace bus {
    extern bus_log_t g_logger;

    MyRedis::MyRedis() {
        m_Redis = nullptr;
        m_Reply = nullptr;
        m_InnerReply = nullptr;
        m_nAppendCmdCount = 0;
    }

    MyRedis::~MyRedis() {
        if (m_Redis != nullptr) {
            delete m_Redis;
            m_Redis = nullptr;
        }
        if (m_Reply != nullptr) {
            delete m_Reply;
            m_Reply = nullptr;
        }
        if (m_InnerReply != nullptr) {
            delete m_InnerReply;
            m_InnerReply = nullptr;
        }
    }

    // 参数初始化 连接redis
    int MyRedis::Init(const char *ip, int port, bool bRedisNeedPassword, const char *szRedisPassword) {
        int ret = -1;
        if (0 == strcmp(ip, "") || 0 == port) {
            g_logger.error("invalid redis params, ip: %s, port: %d", ip, port);
            return -1;
        }

        memset(m_szIp, 0, sizeof(m_szIp));
        memset(m_szRedisPassword, 0, sizeof(m_szRedisPassword));
        strncpy(m_szIp, ip, sizeof(m_szIp));
        m_hdPort = port;

        m_bRedisNeedPassword = bRedisNeedPassword;
        strncpy(m_szRedisPassword, szRedisPassword, sizeof(m_szRedisPassword));
        g_logger.notice("redis ip: %s, port: %d", m_szIp, m_szRedisPassword);

        // 进行redis连接
        m_Redis = redisConnect(m_szIp, m_hdPort);
        if (nullptr == m_Redis || m_Redis->err) {
            if (m_Redis != nullptr) {
                delete m_Redis;
                m_Redis = nullptr;
//                redisFree(m_Redis);
            }
            g_logger.error("connect to redis fail, ip: %s, port: %d", m_szIp, m_szRedisPassword);
            return -1;
        }

        // 进行密码/身份验证
        ret = Auth();

        return ret;
    }

    int MyRedis::Auth() {
        if (m_bRedisNeedPassword) {
            m_InnerReply = (redisReply*)redisCommand(m_Redis, "AUTH %s", m_szRedisPassword);
            if (m_InnerReply->type == REDIS_REPLY_ERROR) {
                g_logger.error("redis auth fail, ip: %s, port: %d, password: %s", m_szIp, m_hdPort, m_szRedisPassword);
                return -1;
            }
            g_logger.notice("redis auth succeed, ip: %s, port: %d", m_szIp, m_hdPort);
            if (m_InnerReply != nullptr) {// 释放reply对象
                freeReplyObject(m_InnerReply);
                m_InnerReply = nullptr;
            }
        }
        return 0;
    }

    // 不需要返回值 不释放上次的m_Reply
    int MyRedis::RedisCmdNoReply(const char *pszFormat, ...) {
        va_list argList;
        int ret = -1;
        bool bOK = false;
        bool bReadOnlyError = false;
        bool bNoScriptError = false;
        bool bTryAgain = false;
        int i = 0;

        do {
            // 重新建立redis连接
            if (bTryAgain) {
                if (m_Redis) {
                    redisFree(m_Redis);
                    m_Redis = nullptr;
                }
                if (m_InnerReply) {
                    freeReplyObject(m_InnerReply);
                    m_InnerReply = nullptr;
                }
            }// if (bTryAgain)

            if (nullptr == m_Redis) {
                // 进行redis连接
                m_Redis = redisConnect(m_szIp, m_hdPort);
                if (nullptr == m_Redis || m_Redis->err) {
                    g_logger.error("connect to redis fail, try times: %d", i);
                    bTryAgain = true;
                    i++;
                    usleep(500);
                    continue;
                }
                // 密码验证
                ret = Auth();
                if (ret != 0) {// 密码验证失败
                    bTryAgain = true;
                    i++;
                    continue;
                }
            }// if (nullptr == m_Redis)

            va_start(argList, pszFormat);
            // 执行redis命令
            m_InnerReply = (redisReply*)redisvCommand(m_Redis, pszFormat, argList);
            va_end(argList);

            if (nullptr != m_InnerReply) {
                bOK = false;
                switch(m_InnerReply->type) {
                    case REDIS_REPLY_STATUS:
                        if (strcasecmp(m_InnerReply->str, "OK") == 0) {
                            bOK = true;
                        }
                        break;
                    case REDIS_REPLY_ERROR:
                        break;
                    default:
                        bOK = true;
                        break;
                }// switch

                if (bOK == false) {
                    if (REDIS_REPLY_ERROR == m_InnerReply->type && strstr(m_InnerReply->str, "READONLY You can't write against a read only slave")) {
                        g_logger.error("redis read only error");
                        bReadOnlyError = true;
                    } else if (REDIS_REPLY_ERROR == m_InnerReply->type && strstr(m_InnerReply->str, "NOSCRIPT NO matching script. Please use EVAL")) {
                        g_logger.error("%s script not exists error", __FUNCTION__);
                        bNoScriptError = true;
                    }
                    g_logger.error("redisCommand exec fail, %d %s %lld", m_InnerReply->type, m_InnerReply->str, m_InnerReply->integer);
                    if (bReadOnlyError) {
                        if (m_Redis) {
                            redisFree(m_Redis);
                            m_Redis = nullptr;
                        }
                        g_logger.error("redis read only error");
                        ret = -2;
                        bTryAgain = true;
                        i++;
                        continue;
                    } else if (bNoScriptError) {
                        g_logger.error("redis bNoScriptError error");
                        ret = -3;
                    } else {
                        g_logger.error("other error");
                        ret = -4;
                    }
                }// if (bOK == false)
                ret = 0;
                bTryAgain = false;

            } else {
                if (m_Redis != nullptr) {
                    redisFree(m_Redis);
                    m_Redis = nullptr;
                }
                g_logger.error("redisCommand fail, m_InnerReply is nullptr, try times: %s", i);
                bTryAgain = true;
                i++;
                continue;
            }
        }while (bTryAgain && i < 3);// do

        if (m_InnerReply != nullptr) {
            freeReplyObject(m_InnerReply);
            m_InnerReply = nullptr;
        }

        return ret;
    }// MyRedis::RedisCmdNoReply

    // get命令 需要返回值 释放上次的m_Reply
    int MyRedis::RedisCmdWithReply(const char *pszFormat, ...) {
        va_list argList;
        int ret = -1;
        bool bOK = false;
        bool bReadOnlyError = false;
        bool bNoScriptError = false;
        bool bTryAgain = false;
        int i = 0;

        if (m_Reply) {
            freeReplyObject(m_Reply);
            m_Reply = nullptr;
        }

        do {
            if (bTryAgain) {
                if (m_Redis) {
                    redisFree(m_Redis);
                    m_Redis = nullptr;
                }
                if (m_Reply) {
                    freeReplyObject(m_Reply);
                    m_Reply = nullptr;
                }
            }// if (bTryAgain)
            // 进行重新连接
            if (nullptr == m_Redis) {
                m_Redis = redisConnect(m_szIp, m_hdPort);
                if (nullptr == m_Redis || m_Redis->err) {
                    g_logger.error("connect to redis fail, ip: %s, port: %d, try times: %d", m_szIp, m_hdPort, i);
                    bTryAgain = true;
                    i++;
                    usleep(500);
                    continue;
                }
                ret = Auth();
                if (ret != 0) {
                    g_logger.error("auth fail");
                    bTryAgain = true;
                    i++;
                    continue;
                }
            }// if (nullptr = m_Redis)

            // redis连接成功--执行redis命令
            va_start(argList, pszFormat);
            m_Reply = (redisReply*)redisvCommand(m_Redis, pszFormat, argList);
            va_end(argList);
            // 执行结果判断
            if (nullptr != m_Reply) {
                bOK = false;
                switch(m_Reply->type) {
                    case REDIS_REPLY_STATUS:
                        if (strcasecmp(m_Reply->str, "OK") == 0)
                            bOK = true;
                        break;
                    case REDIS_REPLY_ERROR:
                        break;
                    case REDIS_REPLY_NIL:// get nil result
                        break;
                    default:
                        bOK = true;
                        break;
                }

                // 分析reply失败的原因
                if (bOK == false) {
                    if (REDIS_REPLY_NIL == m_Reply->type)
                    {
                        g_logger.error("key is not exist, get failed, reply type :%d", m_Reply->type);
                        return -1;
                    }
                    if(REDIS_REPLY_ERROR == m_Reply->type && strstr(m_Reply->str,"READONLY You can't write against a read only slave"))
                    {
                        g_logger.error("redis read only error!");
                        bReadOnlyError = true;
                    }
                    else if(REDIS_REPLY_ERROR == m_Reply->type && strstr(m_Reply->str,"NOSCRIPT No matching script. Please use EVAL"))
                    {
                        g_logger.error("script not exists error!");
                        bNoScriptError = true;
                    }
                    g_logger.error("redisCommand exec fail,%d %s %lld:",m_Reply->type,m_Reply->str,m_Reply->integer);
                    if(bReadOnlyError)
                    {
                        if(m_Redis)
                        {
                            redisFree(m_Redis);
                            m_Redis = NULL;
                        }

                        ret = -2;
                        bTryAgain = true;
                        i++;
                        continue;
                    }
                    else if(bNoScriptError)
                    {
                        ret = -3;
                        break;
                    }
                    else
                    {
                        ret = -4;
                        break;
                    }
                }// if (bOK == false)
                ret = 0;
                bTryAgain = false;
            }// if (nullptr != m_Reply)
            else {
                g_logger.error("redisCommand fail");
                if (m_Redis) {
                    redisFree(m_Redis);
                    m_Redis = nullptr;
                }
                bTryAgain = true;
                i++;
                continue;
            }
        }while (bTryAgain == true && i < 3);// do

        return ret;
    }

    // 使用管道执行多条redis命令
    int MyRedis::RedisPipeAppendCmd(const char* szFormat, ...) {
        int nRet = 0;
        va_list argList;
        va_start(argList, szFormat);
        nRet = redisvAppendCommand(m_Redis, szFormat, argList);
        va_end(argList);
        if (nRet != REDIS_OK) {
            g_logger.error("redisvAppendCommand error, maybe outoff memory.");
            return nRet;
        }
        m_nAppendCmdCount++;// redisvAppendCommand执行次数+1

        return nRet;
    }

    int MyRedis::RedisPipeGetResult() {
        redisReply* reply;// redis reply队列--*reply作为redisGetReply()的返回值,用不到
        int nRet = 0;

        for (; m_nAppendCmdCount > 0; m_nAppendCmdCount--) {
            nRet = redisGetReply(m_Redis, (void**)&reply);
            // 尝试三次
            for (int i = 0; nRet != 0 && i < 3; i++) {
                Reconnect();// ???
                usleep(500);
                nRet = redisGetReply(m_Redis, (void**)&reply);
                if (nRet == 0)// 没必要
                    break;
            }

            if (nRet != 0) {
                g_logger.error("RedisPipeGetResult fail, m_nAppendCmdCount = %d", m_nAppendCmdCount);
                return nRet;
            }
        }
        return 0;
    }

    int MyRedis::Reconnect() {
        if (m_Redis) {
            redisFree(m_Redis);
            m_Redis = nullptr;
        }
        m_Redis = redisConnect(m_szIp, m_hdPort);
        if (nullptr == m_Redis || m_Redis->err) {
            g_logger.error("redisConnect fail, ip: %s, port: %d", m_szIp, m_hdPort);
            return -1;
        }
        // 验证密码
        int nRet = Auth();

        return nRet;
    }

    int MyRedis::RedisCmd(const char* szFormat, ...) {
        va_start(argList, szFormat);
        m_Reply = (redisReply*)redisvCommand(m_Redis, szFormat, argList);
        va_end(argList);

        // 尝试三次
        for (int i = 0; m_Reply == nullptr && i < 3; i++) {
            Reconnect();
            usleep(500);
            m_Reply = (redisReply*)redisvCommand(m_Redis, szFormat, argList);
            if (m_Reply != nullptr)
                break;
        }

        if (nullptr == m_Reply) {
            g_logger.error("redisvCommand fail");
            return -1;
        }

        if (m_Reply->type == REDIS_REPLY_ERROR) {
            g_logger.error("redisvCommand error, errstr: %s", m_Reply->str);
            return -1;
        } else if (m_Reply->type == REDIS_REPLY_STATUS) {
            if (strcasecmp(m_Reply->str, "OK") == 0)
                return 0;
        } else if (m_Reply->type == REDIS_REPLY_NIL) {
            g_logger.error("redisvCommand get nil result");
            return 0;
        }

        return 0;
    }
}// namespace bus