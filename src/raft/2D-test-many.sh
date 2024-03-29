#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

runs=$1

rm -rf ./log/2D/test_log_*

fail_count=0
for i in $(seq 1 $runs); do
    echo "***" DOING THE $i TEST TRIAL
    log_name=./log/2D/test_log_$i.txt
    ## 记录日志
    go test -run 2D > $log_name
    ## 从日志中筛选失败消息
    fail_result=`cat $log_name | grep "FAIL"`
    if [[ $fail_result =~ "FAIL" ]]
    then
        echo '*******' FAILED TESTS IN TRIAL $i '*******'
        fail_count=`expr $fail_count + 1`
    else
      ## 删掉正常执行的日志
        rm -f $log_name
        echo "***" PASSED THE $i TESTING TRIAL
    fi
done
if [[ $fail_count -eq 0 ]]
then
  echo '***' PASSED ALL $runs TESTING TRIALS
else
  echo '***' FAILED SOME TESTING TRIALS : $fail_count
fi
