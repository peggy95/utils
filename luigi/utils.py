#!/usr/bin/env python
# coding:utf-8

import logging
import commands
import sys
import os
import math
import subprocess
from datetime import datetime
import shutil

sys.path.append("./")


def is_empty(s):
    return s == None or s == '' or s == '\N'


def exe_cmd_output(cmd, exceptionInfo='', exit=True):
    """
    执行输入的cmd
    exit==True时
    当cmd执行返回值不等于0，则退出当前进程
    exit==False时
    无论cmd执行结果，皆继续运行
    """
    logging.info(cmd)
    ret, retr = commands.getstatusoutput(cmd)
    if ret != 0:
        if not exit:
            logging.warning('%s. cmd: [%s]' % (exceptionInfo, cmd))
        else:
            logging.error(retr)
            logging.error(cmd)
            sys.exit(1)
    return ret, retr


def exe_cmd_system(cmd, exceptionInfo='', exit=True):
    """
    执行输入的cmd
    exit==True时
    当cmd执行返回值不等于0，则退出当前进程
    exit==False时
    无论cmd执行结果，皆继续运行
    """
    logging.info(cmd)
    ret = os.system(cmd)
    if ret != 0:
        if not exit:
            logging.warning('%s. cmd: [%s]' % (exceptionInfo, cmd))
        else:
            logging.error(cmd)
            sys.exit(1)
    return ret

    cmd = '%s fs -test -e %s' % (hadoop, hdfs_path)
    ret = exe_cmd_system(cmd, False)
    if ret == 0:
        return True
    else:
        return False

def exe_cmd_output_with_retry(cmd, exit = True, retry_times = 5, interval_minutes = 1):
    logging.info(cmd)
    for i in range(1, retry_times + 1):
        ret, retr_str = commands.getstatusoutput(cmd)
        if ret == 0:
            break
        else:
            if i == retry_times:
                msg = 'execute cmd [%s] error, retr_str is [%s], retry %s times, exit!' % (cmd, retr_str, i)
                logging.error(msg)
                if exit:
                    sys.exit(msg)
            else:
                msg = 'execute cmd [%s] error, retr_str is [%s], retry %s times, will retry after %s minute' % (cmd, retr_str, i, interval_minutes)
                logging.warn(msg)
                time.sleep(interval_minutes * 60)
    return ret, retr_str

def check_hive_partition_by_dt(table_name, partition_name):
    """
    检查hive表partition是否存在
    """
    hive_cmd = "show partitions %s partition(dt='%s')" % (table_name, partition_name)
    cmd = 'hive -S -e "%s" 2> /dev/null' % hive_cmd
    _, retr_str = exe_cmd_output(cmd)
    if retr_str !='':
        logging.info('Hive table %s partition dt=%s exist. '%(table_name, partition_name))
        return True
    else:
        logging.info('Hive table %s partition dt=%s not exist. '%(table_name, partition_name))
        return False


def deleteLocalData(path, specified_date, keep_days):
    """
    按照specified_date一共保留keep_days天数据，包括specified_date
    """
    paths = os.listdir(path)
    valide_path = []
    for p in paths: 
        if False == os.path.isdir(os.path.join(path, p)) or p > specified_date:
            continue
        flag = False 
        try:
            datetime.strptime(p, "%Y%m%d")
            flag=True
        except:
            flag=False
        if False==flag:
            continue

        valide_path.append(p)
    valide_path.sort(reverse = True)
    delete_path = valide_path[keep_days:]
    logging.info('now delete %r of path %s' % (delete_path, path))
    for p in delete_path:
       shutil.rmtree(os.path.join(path, p))

def delete_partition_by_dt(table_name,partition):
    cmd = 'hive -S -e \'alter table %s drop if exists partition (dt="%s");\' >/dev/null' % (table_name, partition)
    exe_cmd_system(cmd, exit=False)

def delete_hdfs_partition_by_dt(hdfs_path, partition):
    delete_path = os.path.join(hdfs_path, 'dt=' + partition)
    cmd = 'hadoop fs -rm -r %s' % delete_path
    exe_cmd_system(cmd, exit=False)

def deleteLocalData(path, specified_date, keep_days):
    """
    按照specified_date一共保留keep_days天数据，包括specified_date
    """
    paths = os.listdir(path)
    valide_path = []
    for p in paths: 
        if False == os.path.isdir(os.path.join(path, p)) or p > specified_date:
            continue
        flag = False 
        try:
            datetime.strptime(p, "%Y%m%d")
            flag=True
        except:
            flag=False
        if False==flag:
            continue

        valide_path.append(p)
    valide_path.sort(reverse = True)
    delete_path = valide_path[keep_days:]
    logging.info('now delete %r of path %s' % (delete_path, path))
    for p in delete_path:
        shutil.rmtree(os.path.join(path, p))
    
def deleteHdfsData(path, specified_date, keep_days, prefix=None):
    """
    按照specified_date一共保留keep_days天数据，包括specified_date
    """
    cmd = ['hadoop', 'fs', '-ls', path] 
    out_pipe = subprocess.Popen(args=cmd, stdout=subprocess.PIPE)
    hdfs_files = []
    for file_name in out_pipe.stdout:
        cols = file_name.split(' ')
        flag = False
        file_path = None
        for col in cols: 
            if  -1 == col.find(path):
                continue
            else:
                flag = True
                file_path = col
                break
        if False==flag or file_path.endswith('_SUCCESS'):
            continue
        base_name = os.path.basename(file_path.rstrip('\n'))
        if prefix !=None and base_name.find(prefix):
            base_name = base_name[base_name.find(prefix)+len(prefix):]
        try:
            datetime.strptime(base_name, "%Y-%m-%d")
        except:
            continue
        if base_name > specified_date:
            continue
        hdfs_files.append(base_name)
    hdfs_files.sort(reverse=True)
    delete_path = hdfs_files[keep_days:]
    logging.info('now delete %r of path %s' % (delete_path, path))
    for p in delete_path:
        if prefix ==None:
            rm_cmd = ['hadoop', 'fs', '-rm', '-R', '-skipTrash', os.path.join(path, p)]
        else:
            rm_cmd = ['hadoop', 'fs', '-rm', '-R', '-skipTrash', os.path.join(path, prefix + p)]
        output_info = 'exe %r error' % rm_cmd
        executeCmd(rm_cmd, output_info, True)

def deleteHivePartition(hive_database_name, hive_table_name, specified_date, keep_days):
    """
    按照specified_date一共保留keep_days天数据，包括specified_date
    """
    hive_table_name = hive_database_name + '.' + hive_table_name
    cmd = ["hive", "-e", "show partitions %s;" % (hive_table_name)]
    logging.info("%r" % cmd)
    out_pipe = subprocess.Popen(args=cmd, stdout=subprocess.PIPE)
    hive_partitions = []
    for pt in out_pipe.stdout:
        date = pt.rstrip('\n').split('=')[1]
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except:
            continue
        if date > specified_date:
            continue

        hive_partitions.append(date)
    hive_partitions.sort(reverse=True)
    delete_date = hive_partitions[keep_days:]
    logging.info('now delete %r of table %s' % (delete_date, hive_table_name))
    for d_t in delete_date:
        rmHiveCmd = 'hive -e \'alter table %s drop if exists partition (dt="%s");\'' % (hive_table_name, d_t)
        exe_cmd_system(rmHiveCmd,exit=False)
    return







