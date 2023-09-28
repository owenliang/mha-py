import pymysql
from pymysql.cursors import DictCursor
import time 

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

class MasterSlaves:
    def __init__(self,master,slaves):
        self.master=master
        self.slaves=slaves

        self.master_wrapper=DBWrapper(master[0],master[1],master[2],master[3])
        self.slave_wrappers=[DBWrapper(slave[0],slave[1],slave[2],slave[3]) for slave in slaves]
    
    def is_master_alive(self):
        try:
            self.master_wrapper.query('SELECT 1')
        except:
            return False 
        return True

    def desc_inst(inst):
        return ''

    def error(self,msg):
        print('[ERROR] {}'.format(msg))

    def info(self,msg):
        print('[INFO] {}'.format(msg))

    def run_sql(self,inst,sql,is_query=False):
        role='MASTER'
        if inst!=self.master_wrapper:
            role='SLAVE'
        try:
            if is_query:
                result=inst.query(sql)
            else:
                result=inst.exec(sql)
        except Exception as e:
            self.error('ROLE={} INSTANCE={}:{} SQL={} ERROR={}'.format(role,inst.host,inst.port,sql,e))
            return e
        else:
            self.info('ROLE={} INSTANCE={}:{} SQL={} RESULT={}'.format(role,inst.host,inst.port,sql,result))
            return result

    def _set_readonly(self,inst,kill=False):
        if isinstance(self.run_sql(inst,'set global read_only=1'),Exception):
            return False 
        if kill:
            result=self.run_sql(inst,"select  concat('KILL ',id,';') as cmd  from information_schema.processlist where command not like '%Binlog%'",is_query=True)
            if isinstance(result,Exception):
                return False
            for row in result:
                result=self.run_sql(inst,row['cmd'])
                if isinstance(result,Exception) and 'Unknown thread id' not in str(result):
                    return False
        return True
    
    def _off_readonly(self,inst):
        if isinstance(self.run_sql(inst,'set global read_only=0'),Exception):
            return False 
        return True 
        
    def _start_slave(self,inst):
        if isinstance(self.run_sql(inst,'start slave'),Exception):
            return False 
        return True 

    def _stop_io(self,inst):
        if isinstance(self.run_sql(inst,'stop slave io_thread'),Exception):
            return False 
        return True 

    def _master_status(self):
        status=self.run_sql(self.master_wrapper,'show master status',is_query=True)
        if isinstance(status,Exception) or len(status)==0:
            return False 
        return status[0]

    def _slave_status(self,inst):
        status=self.run_sql(inst,'show slave status',is_query=True)
        if isinstance(status,Exception) or len(status)==0:
            return False 
        return status[0]

    def _slave_wait_binlog(self,inst,master_status,timeout=30):
        st=time.time()
        while True:
            status=self._slave_status(inst)
            if status:
                if (status['Master_Host'],status['Master_Port'])!=(self.master[0],self.master[1]):
                    return False 
                if not status['Master_Log_File']:
                    return False
                if (status['Master_Log_File'],status['Read_Master_Log_Pos'])==(master_status['File'],master_status['Position']):
                    return True
            time.sleep(1)
            if time.time()-st>=timeout:
                return False

    def _slave_wait_relay(self,inst,timeout=30):
        st=time.time()
        while True:
            status=self._slave_status(inst)
            if status:
                if 'waiting for more updates' in status['Slave_SQL_Running_State']:
                    return True
            time.sleep(1)
            if time.time()-st>=timeout:
                return False

    def _slave_stop_slave(self,inst):
        if isinstance(self.run_sql(inst,'stop slave'),Exception):
            return False
        return True
    
    def _reset_slave(self,inst):
        if isinstance(self.run_sql(inst,'reset slave all'),Exception):
            return False
        return True

    def _change_master(self,inst,new_master):
        SQL="CHANGE MASTER TO MASTER_HOST='{}', MASTER_PORT={},MASTER_USER='{}',MASTER_PASSWORD='{}',MASTER_AUTO_POSITION=1".format(new_master.host,new_master.port,new_master.user,new_master.password)
        if isinstance(self.run_sql(inst,SQL),Exception):
           return False
        if isinstance(self.run_sql(inst,'start slave'),Exception):
           return False
        if not self._set_readonly(inst):
            return False 
        return True

    def _find_latest_slave(self,slaves):
        latest_Master_Log_File=''
        latest_Read_Master_Log_Pos=0
        latest_slave=None
        for s in slaves:
            status=self._slave_status(s)
            if not status:
                return False 
            Master_Log_File=status['Master_Log_File']
            Read_Master_Log_Pos=status['Read_Master_Log_Pos']
            if Master_Log_File>latest_Master_Log_File or (Master_Log_File==latest_Master_Log_File and Read_Master_Log_Pos>latest_Read_Master_Log_Pos):
                latest_slave=s
                latest_Master_Log_File=Master_Log_File
                latest_Read_Master_Log_Pos=Read_Master_Log_Pos
        return latest_slave

    def switch(self,force_master=None):   
        # try to set master read only
        master_alive=self.is_master_alive()
        if master_alive:
            if not self._set_readonly(self.master_wrapper,True):
                raise Exception('set master readonly fail')
            master_status=self._master_status()    # master binlog position

        # Let slaves catch up master's binlog If master is alive, GIVEUP ANY FAILED SLAVE!
        slaves=[]
        force_master_ok=False
        for inst in self.slave_wrappers:
            status=self._slave_status(inst)
            if not status:
                continue 
            if (status['Master_Host'],status['Master_Port'])!=(self.master[0],self.master[1]):
                continue 
            if not status['Master_Log_File']:
                continue
            if master_alive:
                if not self._start_slave(inst):
                    continue
                if not self._slave_wait_binlog(inst,master_status): # wait master binlog position
                    continue
            if not self._stop_io(inst):
                continue
            slaves.append(inst)
            if force_master and (inst.host,inst.port)==(force_master[0],force_master[1]):
                force_master_ok=True
            
        if len(slaves)==0:
            raise Exception('no valid slaves to elect')
        
        if force_master and not force_master_ok:
            raise Exception('force_master is not reachable')
    
        # Let slaves execute all its relay log
        for inst in slaves:
            if not self._slave_wait_relay(inst):
                raise Exception('slave relay wait timeout')
            if not self._slave_stop_slave(inst):
                raise Exception('stop slave fail')

        # Select the latest slave as the new master
        new_master=self._find_latest_slave(slaves)
        if force_master and (new_master.host,new_master.port)!=(force_master[0],force_master[1]):
            raise Exception('force_master is not the latest slave, you should only consider force_master when do online-switch but not auto-failover')

        # Change the other slaves to the new master
        if not self._off_readonly(new_master):
            raise Exception('new master off readonly fail')
        if not self._reset_slave(new_master):
            raise Exception('new master reset slave fail')
        for inst in slaves:
            if inst!=new_master:
                if not self._change_master(inst,new_master):
                    raise Exception('change master fail')
        
        topology={
            'old_master':self.master,
            'new_master':(new_master.host,new_master.port,new_master.user,new_master.password),
            'good_slaves':[],
            'unknown_slaves':[],
        }
        for s in self.slave_wrappers:
            s_info=(s.host,s.port,s.user,s.password)
            if s in slaves:
                if s!=new_master:
                    topology['good_slaves'].append(s_info)
            else:
                topology['unknown_slaves'].append(s_info)
        return topology

if __name__=='__main__':
    import json,os,sys  
    if os.path.exists('topology'):
        print('topology exists, remove it before run')
        sys.exit(1)
    #stop slave;CHANGE MASTER TO MASTER_HOST="10.0.0.235", MASTER_PORT=3306,MASTER_USER='root',MASTER_PASSWORD='baidu@123',MASTER_AUTO_POSITION=1;start slave;show slave status\G;
    cluster=MasterSlaves(master=('10.0.0.235',3306,'root','baidu@123'),slaves=[('10.0.0.236',3306,'root','baidu@123'),('10.0.0.240',3306,'root','baidu@123')])
    topology=cluster.switch() # MySQL集群的整体协调太复杂,HA逻辑没法回滚
    with open('topology','w') as fp:
        json.dump(topology,fp)