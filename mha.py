import pymysql
from pymysql.cursors import DictCursor
import time 
import os 
import sys 

# Failover配置
MAX_FAIL_TIMES=2

# MySQL主从关系
MASTER=('10.0.0.235',3306,'root','baidu@123')
SLAVES=[
    ('10.0.0.236',3306,'root','baidu@123'),
]

# DB客户端(无需关心连接释放)
class DBWrapper:
    def __init__(self,host,port,user,password):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.db=None
        self.cursor=None
    
    def _prepare(self):
        self.db=pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,autocommit=True,connect_timeout=1,read_timeout=1,write_timeout=1)
        self.cursor=self.db.cursor(DictCursor)

    def _destroy(self):
        try:
            if self.cursor:
                self.cursor.close()
        except:
            pass
        try:
            if self.db:
                self.db.close()
        except:
            pass
        self.db=self.cursor=None

    def exec(self,sql):
        try:
            self._prepare()
            return self.cursor.execute(sql)
        finally:
            self._destroy()
    
    def query(self,sql):
        try:
            self._prepare()
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        finally:
            self._destroy()

# 禁止连续Failover
if os.path.exists('failover.flag'):
    print('已经发生Failover，请人工介入!')
    sys.exit(1)

master_wrapper=DBWrapper(MASTER[0],MASTER[1],MASTER[2],MASTER[3])
slave_wrappers=[]
for slave in SLAVES:
    slave_wrappers.append(DBWrapper(slave[0],slave[1],slave[2],slave[3]))

# Failover循环检查
fail_times=0
while True:
    # 探测Master存活
    try:
        slave_status=master_wrapper.query('SELECT 1')
    except Exception as e:
        fail_times+=1
        print('【Master探测】Master探测连续失败{}次,错误:{}'.format(fail_times, e))
    else:
        fail_times=0
        print('【Master探测】Master运行正常,持续观测中...')
    
    # 未到达failover阈值 
    if fail_times<MAX_FAIL_TIMES:
        time.sleep(1)
        continue
    
    # 触发failover流程
    print('【Master故障】Master Failover开始执行!')

    try:
        # 1，所有SLAVE节点stop slave io_thread
        for i in range(len(SLAVES)):
            slave_wrappers[i].exec('stop slave io_thread')
             
        # 2，选取SLAVE中(Master_Log_File,Read_Master_Log_Pos)最新的节点作为新Master
        latest_Master_Log_File=''
        latest_Read_Master_Log_Pos=''
        latest_slave_idx=0
        for i in range(len(SLAVES)):
            slave_status=slave_wrappers[i].query('show slave status')[0]
            Master_Log_File=slave_status['Master_Log_File']
            Read_Master_Log_Pos=slave_status['Read_Master_Log_Pos']
            if Master_Log_File>latest_Master_Log_File or (Master_Log_File==latest_Master_Log_File and Read_Master_Log_Pos>latest_Read_Master_Log_Pos):
                latest_slave_idx=i
                latest_Master_Log_File=Master_Log_File
                latest_Read_Master_Log_Pos=Read_Master_Log_Pos
        print('【选举Master成功】当前Master->{}:{},候选Master->{}:{},Master_Log_File:{},Read_Master_Log_Pos:{}'.format(
            MASTER[0],MASTER[1],
            SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1],latest_Master_Log_File,latest_Read_Master_Log_Pos))

        # 3，等待新Master的Slave_SQL_Running_State: Slave has read all relay log; waiting for more updates出现，此后其binlog内含最全数据，其他SLAVE可切过来
        while True:
            latest_slave_status=slave_wrappers[latest_slave_idx].query('show slave status')[0]
            Slave_SQL_Running=latest_slave_status['Slave_SQL_Running']
            Master_Log_File=latest_slave_status['Master_Log_File']
            Read_Master_Log_Pos=latest_slave_status['Read_Master_Log_Pos']
            Relay_Master_Log_File=latest_slave_status['Relay_Master_Log_File']
            Exec_Master_Log_Pos=latest_slave_status['Exec_Master_Log_Pos']
            Slave_SQL_Running_State=latest_slave_status['Slave_SQL_Running_State']
            if 'waiting for more updates' in Slave_SQL_Running_State:
                break
            print('【后续Master正在追Relay】候选Master->{}:{},Master_Log_File:{},Read_Master_Log_Pos:{},Relay_Master_Log_File:{},Exec_Master_Log_Pos:{}'.format(
                SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1],Master_Log_File,Read_Master_Log_Pos,Relay_Master_Log_File,Exec_Master_Log_Pos))
            time.sleep(1)
        print('【候选Master Relay已追平】候选Master->{}:{},Master_Log_File:{},Read_Master_Log_Pos:{},Relay_Master_Log_File:{},Exec_Master_Log_Pos:{}'.format(
                    SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1],Master_Log_File,Read_Master_Log_Pos,Relay_Master_Log_File,Exec_Master_Log_Pos))
        
        # 4，所有SLAVE节点stop slave停止sql_thread
        for i in range(len(SLAVES)):
            slave_wrappers[i].exec('stop slave')

        # 5，所有SLAVE节点change master to新Master，并start slave
        for i in range(len(SLAVES)):
            if i==latest_slave_idx:
               continue
            error=slave_wrappers[i].exec("CHANGE MASTER TO MASTER_HOST='{}', MASTER_PORT={},MASTER_USER='{}',MASTER_PASSWORD='{}',MASTER_AUTO_POSITION=1".format(
                SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1],SLAVES[latest_slave_idx][2],SLAVES[latest_slave_idx][3]
            ))
            print('【Slave指向新Master】Slave->{}:{},新Master->{}:{}'.format(SLAVES[i][0],SLAVES[i][1],SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1]))

        # 6，新Master执行reset slave all关停slave角色

        # 7，新Master执行set read_only=0开放写入
        # 8，执行Hook，通知变更完成、或者通知切换异常
    except Exception as e:
        print('【Failover异常】{}'.format(e))
    else:
        print('【Failover成功】新Master->{}:{}'.format(SLAVES[latest_slave_idx][0],SLAVES[latest_slave_idx][1]))
    finally:
        with open('failover.flag','w',encoding='utf-8') as fp:  # 禁止连续切换
            pass
    break