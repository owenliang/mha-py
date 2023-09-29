# MHA-PY

基于MHA,Orchestrator的实现和思想, 用Python实现简单易用的MySQL HA工具

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

