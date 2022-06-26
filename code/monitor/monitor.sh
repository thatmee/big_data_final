#!/bin/sh
# 启动 httpd 服务
systemctl start httpd.service

# 使配置项生效
[ ! -r /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf ] && [ -r /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf.bak ] && mv /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf.bak /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf

[ ! -r /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh ] && [ -r /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh.bak ] && mv /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh.bak /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh

# 启动 pyspark
. ./.bash_profile
python3 /root/big_data.py

# 等待 pyspark 任务结束
flag=1
result=1
while [ "$flag" -eq 1 ]; do
    sleep 2s
    result=`pidof python3 /root/big_data.py`
    if  [ -z "$result" ]; then
        echo "process is finished"
        flag=0
    fi
done

# 恢复配置项
mv /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf /root/spark-2.1.1-bin-hadoop2.7/conf/spark-defaults.conf.bak
mv /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh /root/spark-2.1.1-bin-hadoop2.7/conf/spark-env.sh.bak

# 停止 httpd 服务
systemctl stop httpd.service