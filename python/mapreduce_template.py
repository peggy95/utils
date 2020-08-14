# -*- coding:utf-8 -*-
import numpy as np
import logging
import sys
import os
import argparse
import ConfigParser
import datetime
import threading
import itertools
import random
import time
import commands
from math import sin,cos,pi,log
import datetime
sys.path.append("./")
reload(sys)
sys.setdefaultencoding('utf-8')

processDay=''

def checkFileExist(hadoop, pathPrefix,checkDate):
    checkDayFilePath = os.path.join(pathPrefix,checkDate)
    checkCmd = '%s fs -test -e %s' % (hadoop, checkDayFilePath)
    ret = os.system(checkCmd)
    if ret == 0:
        return True
    return False

def waitForHdfsFile (hadoop,pathPrefix,checkDate,waitMin=300,checkIntervalSecond=60):

    checkStart = datetime.datetime.now()
    waitMin = datetime.timedelta(minutes=waitMin)

    checkNow = datetime.datetime.now()
    while checkNow - checkStart <waitMin:
        if checkFileExist(hadoop,pathPrefix,checkDate):
          logging.info('checkTime [%s], [EXIST] check hdfs [%s]' , checkNow.strftime("%H:%M:%S"),os.path.join(pathPrefix,checkDate))
          return True
        else:
           logging.info('checkTime [%s], [Not Exist] check hdfs [%s]', checkNow.strftime("%H:%M:%S"),os.path.join(pathPrefix,checkDate))
        time.sleep(checkIntervalSecond)
        checkNow = datetime.datetime.now()
    return False

def deleteHadoopFile(hadoop, path, debug = False):
    testCmd = '%s fs -test -e %s' %(hadoop, path)
    if debug:
        print("-"*20 + " cmd " + "-"*20)
        print(testCmd)
        print("-"*45)
    ret = os.system(testCmd)
    if ret != 0:
        return
    cmd_del_pre_outfile = '%s fs -rm -r -skipTrash %s' %(hadoop, path)
    exe_cmd(cmd_del_pre_outfile, debug)

def existHadoopFile(path, hadoop='hadoop'):
    testCmd = '%s fs -test -e %s' %(hadoop, path)
    ret = os.system(testCmd)
    if 0 == ret:
       return True
    else:
       return False

def exe_cmd(cmd, debug = False):
    if debug:
        print("-"*20 + " cmd " + "-"*20)
        print(cmd)
        print("-"*45)
    logging.info("--- process cmd ---")
    logging.info(cmd)
    ret = os.system(cmd)
    if ret != 0:
        logging.error("--- %s is failed ---" %(cmd))
        os._exit(1)

def run(**kwargs):
    hadoop = kwargs.get('hadoop', 'hadoop')
    streaming = kwargs.get('streaming', '/home/ads_search/tool/hadoop-streaming-2.2.0.jar')

    processDay_ = datetime.datetime.strptime(processDay,'%Y-%m-%d').strftime('%Y-%m-%d')
    
    if not existHadoopFile(os.path.join(args.input_path, 'dt=' + processDay_)):
        logging.info('[ Not Exist] hdfs [%s]',os.path.join(args.input_path,'dt=' + processDay_))
        os._exit(1)

    hadoopInputPath = os.path.join(input_path, 'dt=' + processDay_, '00*') 
    hadoopOutputPath = os.path.join(output_path, processDay_)

    print('Source path : ',hadoopInputPath)
    print('Output path: ',hadoopOutputPath)
# you should put your tar/any other files in hdfs: /user/jd_ad/ads_search/tools/python2.7.tar.gz#python27 and rename it.
# this will be used to run your code.
    cmd_run = '%(hadoop)s jar %(streaming)s\
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -D stream.num.map.output.key.fields=1 \
            -D num.key.fields.for.partition=1 \
            -D mapred.job.name=%(names)s\
            -D mapred.reduce.tasks=%(reduce_num)d\
            -D mapred.job.map.capacity=%(map_capacity)d\
            -D mapreduce.reduce.memory.mb=5120\
            -D mapred.job.reduce.capacity=%(reduce_capacity)d\
            -D mapred.textoutputformat.ignoreseparator=true\
            -files %(this_file)s \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner\
            -input %(inputFile)s\
            -output %(outputFile)s\
            -mapper "python27/bin/python2.7 %(this_file)s -t map "  \
            -reducer "python27/bin/python2.7 %(this_file)s -t reduce"\
            -cacheArchive /user/jd_ad/ads_search/tools/python2.7.tar.gz#python27 \
            '% {
                'hadoop': hadoop,
                'streaming': streaming,
                'names': __file__ + "@wangpei960",
                'reduce_num': 200,
                'map_capacity': 300,
                'reduce_capacity': 300,
                'inputFile': hadoopInputPath,
                'outputFile': hadoopOutputPath,
                'this_file': __file__
                }

    deleteHadoopFile(hadoop, hadoopOutputPath)
    exe_cmd(cmd_run)


def mapper():
    # 新版本的api为mapreduce_map_input_file，老版本的api为map_input_file，在集群上尝试了老版本的api，代码会报错
    for line in sys.stdin: 
        line = line.strip()
        if line == "" or line == '\n':
            continue
        cols = line.split('\t')
        if len(cols) != 13:
            continue
        try:
            skus_1 =list(map(lambda x: str(int(x)%int(3e7-1)+1) ,cols[1].split(',')))
            skus_2 =list(map(lambda x: str(int(x)%int(3e7-1)+1) ,cols[5].split(',')))
            skus_3 =list(map(lambda x: str(int(x)%int(3e7-1)+1) ,cols[9].split(',')))
            print('\n'.join(skus_1+skus_2+skus_3))
        except ValueError:
            continue

def reducer():
    current_key = ""
    for line in sys.stdin:
        line = line.strip()
        key = line
        if current_key == "":
            current_key = key
            cnt = 1 
        else:
            if key == current_key:
                cnt += 1
                continue
            else:
                print(current_key + ',' + str(cnt))
                current_key = key
                cnt = 1
    print(current_key + ',' + str(cnt))

if __name__ == '__main__':

    #processDay = datetime.date.today().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description=__file__)
    # parser.add_argument('-t', dest='cmd', choices=['run', 'map', 'reduce'],
    #                     default='run', help='run type: default \'run\'')
    parser.add_argument('-t', dest='cmd', choices=['run', 'map', 'reduce'],
                        default='run', help='run type: default \'run\'')
    parser.add_argument('-d', dest='processDay', default='', help='processDay')
    parser.add_argument('-ip', dest='input_path', default='hdfs://ns1018/user/jd_ad/wangpei960/user_behaviour/user_behaviour_month')
    parser.add_argument('-op', dest='output_path', default='hdfs://ns1018/user/jd_ad/wangpei960/user_behaviour/sku_freq_count')
    args = parser.parse_args()
    cmd = args.cmd
    processDay = args.processDay
    input_path = args.input_path
    output_path = args.output_path

    if 'run' == cmd:
        myFilePath = os.path.realpath(__file__)
        ## level参数设置输出级别
        logging.basicConfig(level = logging.INFO,
                            format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt = '%a, %d %b %Y %H:%M:%S',
                            stream = sys.stdout,
                            filemode = 'a')
        logging.info('--- ' + __file__ + ' ---')
        logging.info(args.__dict__)
        run(**args.__dict__)

    elif 'map' == cmd:
        mapper()
    elif 'reduce' == cmd:
        reducer()
    sys.exit(0)



