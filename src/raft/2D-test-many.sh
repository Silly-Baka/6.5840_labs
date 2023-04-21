#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

runs=$1

rm -rf ./log/2D/test_log_*

pass_count=0
fail_count=0
for i in $(seq 1 $runs);
do
    echo "***" DOING THE $i TEST TRIAL
    log_name=./log/2D/test_log_$i.txt
    ## 记录日志
    go test -run 2D > $log_name
    ## 从日志中筛选失败消息
    fail_result=`cat $log_name | grep "FAIL"`

    ## 输出最后两行的时间记录
    tail -2 $log_name

    if [[ $fail_result =~ "FAIL" ]]
    then
        fail_count=`expr $fail_count + 1`
        echo FAILED in $i , pass $pass_count,fail $fail_count
    else
      ## 删掉正常执行的日志
        rm -f $log_name
        pass_count=`expr $pass_count + 1`
        echo PASSED in $i , pass $pass_count,fail $fail_count
    fi
done
if [[ $fail_count -eq 0 ]]
then
  echo '***' PASSED ALL $runs TESTING TRIALS
else
  echo '***' FAILED SOME TESTING TRIALS : $fail_count
fi
