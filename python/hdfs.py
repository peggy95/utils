#-*- coding:utf-8 -*-
import os
import sys
import logging
import subprocess
import commands

def hdfs_file_exist(hadoop, hdfs_path):
    cmd = '%s fs -test -e %s' % (hadoop, hdfs_path)
    ret = exe_cmd_system(cmd, False)
    if ret == 0:
        return True
    else:
        return False

def get_hdfs_file_size(hadoop, hdfs_path):
    """
        获取hdfs路径的大小, 调用前请确保hdfs_path存在
    """
    cmd = 'set -o pipefail && %s fs -du -s %s | gawk \'{print $1}\'' % (hadoop, hdfs_path)

    retry_count = 3

    for i in range(retry_count):
        try:
            ret, file_size = exe_cmd_output(cmd, True)
            int_file_size = int(file_size)
            return int_file_size
        except Exception as e:
            print >> sys.stderr, 'error occured'
            if i == retry_count - 1:
                raise

def get_hdfs_fils(hdfs_path):
    cmd = ['hadoop', 'fs', '-ls', hdfs_path]
    out_pipe = subprocess.Popen(args=cmd, stdout=subprocess.PIPE)
    hdfs_files = []
    for file_name in out_pipe.stdout:
        cols = file_name.split(' ')
        flag = False
        file_path = None
        for col in cols: 
            if  -1 == col.find(hdfs_path):
                continue
            else:
                flag = True
                file_path = col
                break
        if False==flag or file_path.endswith('_SUCCESS'):
            continue
        hdfs_files.append(file_path.rstrip('\n'))
    
    return hdfs_files

def deleteHdfsData(path, specified_date, keep_days):
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
        try:
            datetime.strptime(base_name, "%Y%m%d")
        except:
            continue
        if base_name > specified_date:
            continue
        hdfs_files.append(base_name)
    hdfs_files.sort(reverse=True)
    delete_path = hdfs_files[keep_days:]
    logging.info('now delete %r of path %s' % (delete_path, path))
    for p in delete_path:
        rm_cmd = ['hadoop', 'fs', '-rm', '-R', '-skipTrash', os.path.join(path, p)]
        output_info = 'exe %r error' % rm_cmd
        executeCmd(rm_cmd, output_info, True)

