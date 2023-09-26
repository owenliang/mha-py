import pymysql
import time 

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
        self.cursor=self.db.cursor()

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

# Failover循环检查
wrapper=DBWrapper(MASTER[0],MASTER[1],MASTER[2],MASTER[3])
fail_times=0
while True:
    # 探测Master存活
    try:
        slave_status=wrapper.query('SELECT 1')
    except Exception as e:
        fail_times+=1
        print('Master探测连续失败{}次,错误:{}'.format(fail_times, e))
    else:
        fail_times=0
    
    # 未到达failover阈值 
    if fail_times<10:
        time.sleep(1)
        continue
    
    # 触发failover流程
    print('Master failover开始执行!')

    # 1，所有SLAVE节点stop slave io_thread
    # 2，所有SLAVE节点等待Slave_SQL_Running_State: Slave has read all relay log; waiting for more updates出现
    # 3，选取SLAVE中(Master_Log_File,Read_Master_Log_Pos)最新的节点作为新Master
    # 4，所有SLAVE节点stop slave停止sql_thread
    # 5，新Master执行reset slave all关停slave角色
    # 6，新Master执行set read_only=0开放写入
    # 7，所有SLAVE节点change master to新Master，并start slave
    # 8，执行Hook，通知变更完成、或者通知切换异常
    break