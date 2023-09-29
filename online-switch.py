from mha import MasterSlaves
from conf import MASTER,SLAVES
from utils import failover_once,failover_done,monitor_is_running
import sys 

failover_once()

if monitor_is_running():
    print('monitor is running, can not do online-switch')
    sys.exit(-1)

cluster=MasterSlaves(master=MASTER,slaves=SLAVES)

if not cluster.is_master_alive():
    print('master is down, can not do online-switch')
    sys.exit(-1)

print('MASTER:',MASTER)
for i in range(len(SLAVES)):
    print('SLAVE ID:{}, INFO:{}'.format(i,SLAVES[i]))

slave_id=int(input('Input Slave ID to be promoted:'))

try:
    topology=cluster.switch(SLAVES[slave_id])
    print('online-switch done')
except Exception as e:
    print('online-switch exception,{}'.format(e))
    topology={}
finally:
    failover_done(topology)