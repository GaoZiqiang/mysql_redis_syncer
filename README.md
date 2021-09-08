## MysqlRedisSyncer
解析mysql的binlog文件，并且把解析出来的数据写入redis，实现mysql和redis数据库的数据全量同步和增量同步两种同步。
## 项目部署要点
  1.支持mysql5.6及以下版本；

  2.mysql的binlog format为RAW；

  3.程序中对数据的读取要与mysql数据表的格式相符合。

