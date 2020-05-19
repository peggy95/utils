#-*- coding:utf-8 -*-
import os,sys
import logging
import subprocess
from datetime import datetime
import shutil

def exe_cmd_output(cmd, exit=True):
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
            logging.warning('execute cmd error. cmd: [%s]' % (cmd))
        else:
            logging.error(retr)
            logging.error(cmd)
            sys.exit(1)
    return ret, retr


def rm_dir_files(folder):
    # os.listdir: 列出文件夹下文件名，不包含.和..
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            # os.path.isfile 判断是否为文件
            if os.path.isfile(file_path):
                logging.info('rm %s' % file_path)
                # os.unlink 删除文件，文件夹则error
                os.unlink(file_path)
        except Exception as e:
            print(e)
            os._exit(-1)

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

