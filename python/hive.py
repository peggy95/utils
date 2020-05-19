#-*- coding:utf-8 -*-

import os
import sys
import random
import logging
import subprocess
from datetime import datetime

def deleteHivePartition(hive_base, hive_table_name, specified_date, keep_days):
    """
    按照specified_date一共保留keep_days天数据，包括specified_date
    """
    cmd = ["hive", "-e", "show partitions %s.%s;" % (hive_base, hive_table_name)]
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
        rmHiveCmd = 'hive -e \'alter table %s.%s drop if exists partition (dt="%s");\'' % (hive_base, hive_table_name, d_t)
        os.system(rmHiveCmd)
    return

