# MHA-PY

基于MHA,Orchestrator的实现和思想, 用Python实现简单易用的MySQL HA工具

## 切换逻辑

* 老master设置readonly，然后kill掉存量所有连接
* 如果老master活着（表示online-switch），那么等slave追上master的binlog；如果master死了，那么不等binlog追平；无论如何， 最终都停掉io_thread
* 等待slave执行完存量relay log
* 选slave中binlog position最新的作为候选master;
* 候选master取消read_only并reset slave all，其他slave执行change master指向它
* 如果老master活着（表示online-switch）,那么老master也change master指向候选master
* 记录新的拓扑关系到锁文件

HA执行后会产生failover.once文件,程序将无法再次启动,必须介入修改conf.py中的主从关系到最新状态,删除failover.once并重启程序进入下一轮HA监控

## 脚本说明

conf.py: 配置主从实例的连接信息

```
# How long to wait before HA
HA_THRESHOLD_SECS=10

# Lock file
FAILOVER_ONCE='failover.once'

# PID file
MONITOR_PID='monitor.pid'

# Cluster config before HA 
MASTER=('10.0.0.235',3306,'root','baidu@123')
SLAVES=[
    ('10.0.0.236',3306,'root','baidu@123'),
    ('10.0.0.240',3306,'root','baidu@123')
]
```

hook.py: 编写自定义的钩子, 用来切dns,proxy等

```
# user-defined hooks

def before_ha(cluster):
    print('before_ha',cluster)

def after_ha(cluster,topology,exception=None):
    print('after_ha',cluster,topology,exception)
```

monitor.py: 常驻HA监控程序, master宕机后自动提升slave, 原master留给用户处理

```
python monitor.py
```

online-switch.py: 命令行工具, 指定slave触发主从切换, 原master成为从库

```
python online-switch.py
[INFO] ROLE=MASTER INSTANCE=10.0.0.235:3306 SQL=SELECT 1 RESULT=1
MASTER: ('10.0.0.235', 3306, 'root', 'baidu@123')
SLAVE ID:0, INFO:('10.0.0.236', 3306, 'root', 'baidu@123')
SLAVE ID:1, INFO:('10.0.0.240', 3306, 'root', 'baidu@123')
Input Slave ID to be promoted:1
...
```

