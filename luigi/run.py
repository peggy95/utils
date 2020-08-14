# -*- coding:utf-8 -*-
import sys
import pdb
import os
import datetime
import time
import logging
import random
import operator
import subprocess
import luigi
import luigi.contrib.hive
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi import six
import utils

logging.basicConfig(level = logging.INFO, 
                    format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt = '%a, %d %b %Y %H:%M:%S',
                    stream = sys.stdout,
                    filemode = 'a')

def executeCmd(cmd, exceptionInfo='', exitFlag=False):
    ret = subprocess.call(cmd)
    if ret != 0:
        logging.error(exceptionInfo)
        if exitFlag == True:
            os._exit(-1)

class HivePartitionTarget(luigi.Target):

    def __init__(self, table, partition, database):
        self.table = table
        self.partition = partition
        self.database = database

    def exists(self):
        str_partition = ','.join(["{0}='{1}'".format(k, v) for (k, v) in sorted(six.iteritems(self.partition), key=operator.itemgetter(0))])
        cmd = """use %s; show partitions %s partition (%s)""" % (self.database, self.table, str_partition)

        try:
            run_result = luigi.contrib.hive.run_hive_cmd(cmd)
            if run_result:
                return True
            else:
                logging.info(self.table + " is not ready!!!")
                return False
        except:
            logging.info("No such table " + self.table)
            return False

class ExternalHiveDepends(luigi.ExternalTask):
    # check hive partition exists
    table = luigi.Parameter()
    processDay = luigi.Parameter()
    database = luigi.Parameter()
    
    def exists_once(self, cmd, waitTime):
        try:
            run_result = luigi.contrib.hive.run_hive_cmd(cmd)
            if run_result:
                return True
            else:
                logging.info(self.table + " is not ready!!! " + str(waitTime) + " seconds left")
                return False
        except:
            logging.info("No such table " + self.table + ", " + str(waitTime) + " seconds left")
            return False

    def output(self):
        return HivePartitionTarget(self.table,  {'dt': self.processDay}, self.database)

    def run(self):
        waitTime = 3600*7
        check_interval = 1*5
        self.partition = {'dt': self.processDay}
    
        str_partition = ','.join(["{0}='{1}'".format(k, v) for (k, v) in sorted(six.iteritems(self.partition), key=operator.itemgetter(0))])
        cmd = """use %s; show partitions %s partition (%s)""" % (self.database, self.table, str_partition)
        logging.info(cmd)

        while False == self.exists_once(cmd, waitTime):
            time.sleep(check_interval)
            waitTime -= check_interval

            if waitTime < 0:
                logging.info("wait too long")
                os._exit(-1)


class ExternalDepends(luigi.ExternalTask):
    # check hdfs file exists
    check_path = luigi.Parameter()
    
    def hdfs_exist(self, path):
        cmd = ['hadoop','fs','-test','-e', path]
        ret = subprocess.call(cmd)
        if ret != 0:
            return False
        else:
            return True

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.check_path)

    def run(self):
        waitTime = 3600*24
        check_interval = 1*5
        while False == self.hdfs_exist(self.check_path):
            time.sleep(check_interval)
            waitTime -= check_interval
            logging.info(str(waitTime) + " seconds left")
            if waitTime < 0:
                logging.info("wait too long")
                os._exit(-1)


class UserPayMonth(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    sql_name = luigi.Parameter()
    depends_database = luigi.Parameter()
    depends_table = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table =luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()
    
    def requires(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        return [ExternalHiveDepends(table=self.depends_table, 
                                    processDay=first_day_str, 
                                    database=self.depends_database),
                ExternalHiveDepends(table=self.depends_table, 
                                    processDay=last_day_str, 
                                    database=self.depends_database)]
    
    def output(self):
        return HivePartitionTarget(self.output_table, {'dt': self.processDay}, self.output_database)
    
    def run(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        logging.info('---------------------------[begin UserPayMonth]---------------------')
        logging.info('Date is from %s(excluded) to %s(included) with %s days' % (first_day_str,last_day_str,self.history))
        
        cmd = ['hive', '-f', self.sql_name, 
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_bg='+first_day_str, 
               '--hiveconf', 'date_nd='+last_day_str,
               '--hiveconf', 'target_path='+self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end UserPayMonth]---------------------')
        logging.info('---------------------------[delete previous UserPayMonth]---------------------')
        utils.deleteHdfsData(self.output_path, last_day_str, int(self.max_keep_days), prefix='dt=')
        utils.deleteHivePartition(self.output_database,self.output_table, last_day_str, int(self.max_keep_days))
        
class UserClickMonth(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    sql_name = luigi.Parameter()
    depends_database_1 = luigi.Parameter()
    depends_database_2 = luigi.Parameter()
    depends_table_1 =luigi.Parameter()
    depends_table_2 = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table = luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()
    
    def requires(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        return [ExternalHiveDepends(table=self.depends_table_1, 
                                    processDay=first_day_str, 
                                    database=self.depends_database_1),
                ExternalHiveDepends(table=self.depends_table_1, 
                                    processDay=last_day_str, 
                                    database=self.depends_database_1),
                ExternalHiveDepends(table=self.depends_table_2, 
                                    processDay=first_day_str, 
                                    database=self.depends_database_2),
                ExternalHiveDepends(table=self.depends_table_2, 
                                    processDay=last_day_str, 
                                    database=self.depends_database_2)]
    
    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(os.path.join(self.output_path,'dt=' + self.processDay))
    
    def run(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        logging.info('---------------------------[begin UserClickMonth]---------------------')
        logging.info('Date is from %s(excluded) to %s(included) with %s days' % (first_day_str,last_day_str,self.history))
        
        cmd = ['hive', '-f', self.sql_name, 
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_bg='+first_day_str, 
               '--hiveconf', 'date_nd='+last_day_str,
               '--hiveconf', 'target_path='+self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end UserClickMonth]---------------------')
        #logging.info('---------------------------[delete previous UserClickMonth]---------------------')
        #utils.deleteHdfsData(self.output_path, last_day_str, int(self.max_keep_days), prefix='dt=')
        #utils.deleteHivePartition(self.output_database,self.output_table, last_day_str, int(self.max_keep_days))


class UserSearchDay(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    sql_name = luigi.Parameter()
    depends_database_1 = luigi.Parameter()
    depends_database_2 = luigi.Parameter()
    depends_table_1 = luigi.Parameter()
    depends_table_2 = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table = luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        return [ExternalHiveDepends(table=self.depends_table_1,
                                    processDay=last_day_str,
                                    database=self.depends_database_1),
                ExternalHiveDepends(table=self.depends_table_2,
                                    processDay=last_day_str,
                                    database=self.depends_database_2)]

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(os.path.join(self.output_path,'dt=' + self.processDay))

    def run(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        logging.info('---------------------------[begin UserSearchDay]---------------------')
        logging.info('Date is from %s(excluded) to %s(included) with %s days' % (first_day_str,last_day_str,self.history))

        cmd = ['hive', '-f', self.sql_name,
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_bg='+first_day_str,
               '--hiveconf', 'date_nd='+last_day_str,
               '--hiveconf', 'target_path='+self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logging.info(' '.join(cmd))
        logging.info('---------------------------[end UserSearchDay]---------------------')
        #logging.info('---------------------------[delete previous UserSearchDay]---------------------')
        #utils.deleteHdfsData(self.output_path, last_day_str, int(self.max_keep_days), prefix='dt=')
        #utils.deleteHivePartition(self.output_database, self.output_table, last_day_str, int(self.max_keep_days))

class UserSearchMonth(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    sql_name = luigi.Parameter()
    depends_database_1 = luigi.Parameter()
    depends_database_2 = luigi.Parameter()
    depends_table_1 = luigi.Parameter()
    depends_table_2 = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table = luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        return [ExternalHiveDepends(table=self.depends_table_1, 
                                    processDay=first_day_str, 
                                    database=self.depends_database_1),
                ExternalHiveDepends(table=self.depends_table_1, 
                                    processDay=last_day_str, 
                                    database=self.depends_database_1),
                ExternalHiveDepends(table=self.depends_table_2, 
                                    processDay=first_day_str, 
                                    database=self.depends_database_2),
                ExternalHiveDepends(table=self.depends_table_2, 
                                    processDay=last_day_str, 
                                    database=self.depends_database_2)]
    
    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(os.path.join(self.output_path,'dt=' + self.processDay))

    def run(self):
        last_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        first_day = last_day - datetime.timedelta(int(self.history))
        last_day_str = '{date:%Y-%m-%d}'.format(date=last_day)
        first_day_str = '{date:%Y-%m-%d}'.format(date=first_day)
        logging.info('---------------------------[begin UserSearchMonth]---------------------')
        logging.info('Date is from %s(excluded) to %s(included) with %s days' % (first_day_str,last_day_str,self.history))
        
        cmd = ['hive', '-f', self.sql_name, 
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_bg='+first_day_str, 
               '--hiveconf', 'date_nd='+last_day_str,
               '--hiveconf', 'target_path='+self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logger.info(' '.join(cmd))
        logging.info('---------------------------[end UserSearchMonth]---------------------')
        #logging.info('---------------------------[delete previous UserSearchMonth]---------------------')
        #utils.deleteHdfsData(self.output_path, last_day_str, int(self.max_keep_days), prefix='dt=')
        #utils.deleteHivePartition(self.output_database, self.output_table, last_day_str, int(self.max_keep_days))

class UserBehavioursMonth(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    sql_name = luigi.Parameter()
    depends_database_1 = luigi.Parameter()
    depends_database_2 = luigi.Parameter()
    search_table = luigi.Parameter()
    click_table = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table = luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return [UserSearchMonth(processDay = process_day_str),
                UserClickMonth(processDay = process_day_str)]
    
    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(os.path.join(self.output_path,'dt=' + self.processDay))

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        logging.info('---------------------------[begin UserBehavioursMonth]---------------------')
        logging.info('Date is ended at %s with 30 days' % (process_day_str))
        
        cmd = ['hive', '-f', self.sql_name,
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_nd='+ process_day_str,
               '--hiveconf', 'search_table='+ self.search_table,
               '--hiveconf', 'click_table='+ self.click_table,
               '--hiveconf', 'target_path='+ self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end UserBehavioursMonth]---------------------')
        logging.info('---------------------------[delete previous UserBehavioursMonth]---------------------')
        #utils.deleteHdfsData(self.output_path, process_day_str, int(self.max_keep_days), prefix='dt=')
        #utils.deleteHivePartition(self.output_database,self.output_table, process_day_str, int(self.max_keep_days))


class UserBehavioursEval(luigi.Task):
    # processDay is the last date for training data
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    # search date is the date you want to evaluation on
    sql_name = luigi.Parameter()
    depends_database_1 = luigi.Parameter()
    depends_database_2 = luigi.Parameter()
    search_table = luigi.Parameter()
    click_table = luigi.Parameter()
    output_database = luigi.Parameter()
    output_table = luigi.Parameter()
    output_path = luigi.Parameter()
    history = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        evalDay_str = '{date:%Y-%m-%d}'.format(date=(process_day + datetime.timedelta(days=33)))
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return [UserSearchDay(processDay = evalDay_str),
                UserClickMonth(processDay = process_day_str)]

    def output(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        evalDay_str = '{date:%Y-%m-%d}'.format(date=(process_day + datetime.timedelta(days=33)))
        return luigi.contrib.hdfs.HdfsTarget(os.path.join(self.output_path, 'dt=' + self.processDay, 'eval_dt=' + evalDay_str))

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        evalDay_str = '{date:%Y-%m-%d}'.format(date=(process_day + datetime.timedelta(days=33)))
        logging.info('---------------------------[begin UserBehavioursDay]---------------------')
        logging.info('Join click data ended at %s with search-one-day data on %s. ' % (self.processDay, evalDay_str))

        cmd = ['hive', '-f', self.sql_name,
               '--hiveconf', 'output_table='+self.output_table,
               '--hiveconf', 'date_click='+ self.processDay,
               '--hiveconf', 'date_search='+ evalDay_str,
               '--hiveconf', 'search_table='+ self.search_table,
               '--hiveconf', 'click_table='+ self.click_table,
               '--hiveconf', 'target_path='+ self.output_path]
        output_info = 'run %s error'%self.sql_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end UserBehavioursDay]---------------------')
        logging.info('---------------------------[delete previous UserBehavioursDay]---------------------')
        #utils.deleteHdfsData(self.output_path, process_day_str, int(self.max_keep_days), prefix='dt=')
        #utils.deleteHivePartition(self.output_database,self.output_table, process_day_str, int(self.max_keep_days))

class CountSkuFreq(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    python_name = luigi.Parameter()
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return UserBehavioursMonth(processDay = process_day_str)

    def output(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        output_path = os.path.join(self.output_path, process_day_str, '_SUCCESS')
        return luigi.contrib.hdfs.HdfsTarget(output_path)

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        logging.info('---------------------------[begin CountSkuFreq]---------------------')
        logging.info('Count Sku Freq with date: %s' % (process_day_str))
        
        cmd = ['python', self.python_name, 
               '-t', 'run',
               '-d', process_day_str,
               '-ip', self.input_path,
               '-op', self.output_path]
        output_info = 'run %s error'%self.python_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end CountSkuFre]---------------------')
        logging.info('---------------------------[delete previous CountSkuFre]---------------------')
        utils.deleteHdfsData(self.output_path, process_day_str, int(self.max_keep_days))

class GetVocabFile(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    python_name = luigi.Parameter()
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    max_keep_days = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return CountSkuFreq(processDay = process_day_str)

    def output(self):
        return luigi.LocalTarget(self.output_path + '_' + self.processDay)

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        logging.info('---------------------------[begin GetVocabFile]---------------------')
        logging.info('Get vocab_file with date: %s' % (process_day_str))

        cmd = ['python', self.python_name,
               '-d', process_day_str,
               '-ip', self.input_path]
        output_info = 'run %s error'%self.python_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end GetVocabFile]---------------------')
        logging.info('---------------------------[delete previous GetVocabFile]---------------------')
        utils.deleteHdfsData(self.output_path, process_day_str, int(self.max_keep_days))
        
class Shuffle(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    python_name = luigi.Parameter()
    input_path = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return UserBehavioursMonth(processDay = process_day_str)

    def output(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        output_path = os.path.join(self.input_path, 'dt=' + process_day_str, 'shuffled', '_SUCCESS')
        return luigi.contrib.hdfs.HdfsTarget(output_path)

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        logging.info('---------------------------[begin Shuffle]---------------------')
        logging.info('Shuffle data with date: %s' % (process_day_str))
        cmd = ['python', self.python_name,
               '-t', 'run',
               '-d', process_day_str,
               '-ip', self.input_path]
        output_info = 'run %s error'%self.python_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end Shuffle]---------------------')

class ProcessValiTestData(luigi.Task):
    processDay = luigi.Parameter(default='{date:%Y-%m-%d}'.format(date=(datetime.date.today() - datetime.timedelta(days=1))))
    python_name = luigi.Parameter()
    input_path = luigi.Parameter()
    vali_path = luigi.Parameter() 
    test_path =luigi.Parameter()
    eval_path = luigi.Parameter()

    def requires(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        return [UserBehavioursEval(processDay = self.processDay),Shuffle(processDay = process_day_str), GetVocabFile(processDay = process_day_str)]

    def output(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        evalDay_str = '{date:%Y-%m-%d}'.format(date=(process_day + datetime.timedelta(days=33)))
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        vali_put = os.path.join(self.vali_path, 'dt=' + process_day_str, '_SUCCESS')
        test_put = os.path.join(self.test_path, 'dt=' + process_day_str, '_SUCCESS')
        eval_put = os.path.join(self.eval_path, 'dt=' + process_day_str, 'eval_dt=' + evalDay_str, '_SUCCESS')
        return [luigi.contrib.hdfs.HdfsTarget(vali_put),
                luigi.contrib.hdfs.HdfsTarget(test_put),
                luigi.contrib.hdfs.HdfsTarget(eval_put)]

    def run(self):
        process_day = datetime.datetime.strptime(self.processDay, '%Y-%m-%d')
        process_day_str = '{date:%Y-%m-%d}'.format(date=process_day)
        logging.info('---------------------------[begin ProcessValiTestData]---------------------')
        logging.info('ProcessValiTestData data with date: %s' % (process_day_str))
        cmd = ['python', self.python_name,
               '-date', process_day_str,
               '-tp', self.test_path,
               '-vp', self.vali_path,
               '-ip', self.input_path]
        output_info = 'run %s error'%self.python_name
        executeCmd(cmd, output_info, True)
        logging.info('---------------------------[end ProcessValiTestData]---------------------')

    
if __name__ == '__main__':
    luigi.run()
 
