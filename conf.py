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