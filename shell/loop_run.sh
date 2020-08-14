#!/bin/bash

#export HADOOP_CONF_DIR=/software/conf/ads_search/hadoop_conf

period=5
parallel=3

for (( i=1; i <= $period; ++i ));do
    j=$((i-1))
    fileTimeStrap=$(date -d "2020-06-20 + $j days"  "+%Y-%m-%d")
    echo "$fileTimeStrap"
    nohup /home/ads_search/guanjing/joint_learning/data_pipeline/pb_pipe/pb/env/pyana/bin/python user_hist_to_pb.py -dat $fileTimeStrap > logs/log_2pb.$fileTimeStrap 2>&1 & 
    if [ $((i%parallel)) -eq 0 ]; then
        echo "waiting for previous tasks..."
        wait;
    fi
done

