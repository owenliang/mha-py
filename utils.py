from conf import FAILOVER_ONCE,MONITOR_PID
import os
import time 
import json 
import psutil

def failover_once():
    if os.path.exists(FAILOVER_ONCE):
        while True:
            print('Failover has happened before, See {}'.format(FAILOVER_ONCE))
            time.sleep(1)

def failover_done(topology):
    with open(FAILOVER_ONCE,'w') as fp:
        json.dump(topology,fp)

def monitor_begin_run():
    with open(MONITOR_PID,'w') as fp:
        fp.write(str(os.getpid()))

def monitor_is_running():
    if os.path.exists(MONITOR_PID):
        with open(MONITOR_PID,'r') as fp:
            pid=int(fp.read())
            return psutil.pid_exists(pid)
    return False 