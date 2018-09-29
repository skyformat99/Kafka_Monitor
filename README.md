# Kafka_Monitor
使用zabbix发现规则，自动发现Topic列表，并监视各Topic相关指标

# 使用前注意
- python版本2.7
- 需要安装有kafka manager

# 操作步骤

- zabbix web端
1、创建探索规则
2、创建项目原型
3、创建触发器类型

- zabbix agent端

1、vim zabbix_agentd.conf
```sql
增加
#kafka
UserParameter=topic.discovery,python2.7 /etc/zabbix/script/discovery_kafka_topic.py
UserParameter=topic.check[*],python2.7 /etc/zabbix/script/topic_monitor.py "$1" "$2"
```
2、重启zabbix-agent


