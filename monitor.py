from mha import MasterSlaves
from conf import MASTER,SLAVES,HA_THRESHOLD_SECS
from utils import failover_once,failover_done,monitor_begin_run,monitor_is_running
import time,sys 

'''
sysbench --db-driver=mysql --mysql-host=10.0.0.235 --mysql-port=3306 --mysql-user=root --mysql-password='baidu@123' --mysql-db=sbtest --table_size=25000 --tables=250 --events=0 --time=600  oltp_read_write prepare
sysbench --db-driver=mysql --mysql-host=10.0.0.235 --mysql-port=3306 --mysql-user=root --mysql-password='baidu@123' --mysql-db=sbtest --table_size=25000 --tables=250 --events=0 --time=600   --threads=2 --percentile=95 --report-interval=1 oltp_read_write run
sysbench --db-driver=mysql --mysql-host=10.0.0.235 --mysql-port=3306 --mysql-user=root --mysql-password='baidu@123' --mysql-db=sbtest --table_size=25000 --tables=250 --events=0 --time=600   --threads=2 --percentile=95  oltp_read_write cleanup
stop slave;CHANGE MASTER TO MASTER_HOST="10.0.0.235", MASTER_PORT=3306,MASTER_USER='root',MASTER_PASSWORD='baidu@123',MASTER_AUTO_POSITION=1;start slave;show slave status\G;
'''

failover_once()

if monitor_is_running():
    print('monitor is already running')
    sys.exit(1)

monitor_begin_run()

cluster=MasterSlaves(master=MASTER,slaves=SLAVES)

fail_times=0
while True:
    master_alive=cluster.is_master_alive()
    if not master_alive:
        fail_times+=1
    else:
        fail_times=0
        print('master still alive...')

    if fail_times>=HA_THRESHOLD_SECS:
        try:
            topology=cluster.switch()
            print('auto-failover done')
        except Exception as e:
            print('auto-failover exception,{}'.format(e))
            topology={}
        finally:
            failover_done(topology)
        sys.exit(0)
    
    time.sleep(1)