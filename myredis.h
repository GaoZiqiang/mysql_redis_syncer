#ifndef MYREDIS_H
#define MYREDIS_H

#include <hiredis/hiredis.h>
#include <string>
#include "bus/bus_log.h"

namespace bus {
    class MyRedis {
    public:
        MyRedis();
        ~MyRedis();
        // 连接redis
        int Init(const char* ip, int port, bool bRedisNeedPassword, const char* szRedisPassword);
        // 执行redis命令 根据返回值m_Reply判断是否操作成功
        // set命令 不需要返回值 不释放上次的m_Reply
        int RedisCmdNoReply(const char* pszFormat, ...);
        // get命令 需要返回值 释放上次的m_Reply
        int RedisCmdWithReply(const char* pszFormat, ...);

        int RedisCmd(const char* szFormat, ...);
        // 使用管道执行连续多条命令,加快指令执行速度,节省开销
        int RedisPipeAppendCmd(const char* szFormat, ...);
        int RedisPipeGetResult();
        int Reconnect();

        int Auth();

        inline int GetReplyInt(int& reply) const {return (m_Reply ? (reply = m_Reply->integer, 0) : -1);}
        inline char* GetReplyStr() const {return (m_Reply ? m_Reply->str : nullptr);}
        inline redisReply* GetReply() const {return m_Reply;}

    private:
        redisContext*  m_Redis;// redis实例化对象
        redisReply*    m_Reply;
        redisReply*    m_InnerReply;
        unsigned short m_hdPort;
        char           m_szIp[128];
        char           m_szRedisPassword[128];
        bool           m_bRedisNeedPassword;
        va_list        argList;
        int            m_nAppendCmdCount;
    };
}
#endif