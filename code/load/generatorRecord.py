# 作用：模拟客户端，向flume_host:flume_port建立连接并发送数据file_path
# 启动参数：两个
#   - kafka_hosts
#   - file_path
import time
import random
import sys
import getopt
import json
from kafka import KafkaProducer


def generator(file_path):
    random.seed(19960106)
    with open(file_path,"r") as file:
        line = file.readline()
        while line:
            yield json.loads(line.strip("\n"))
            # yield line.strip("\n")
            line = file.readline()
            time.sleep(random.randint(0,30)/10)

def getHostPath():
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv,"h:f:")
    except:
        print("Error")
        return None, None
    host, file_path= None, None
    for opt, arg in opts:
        if opt in ['-h']:
            host = arg
        elif opt in ['-f']:
            file_path = arg
    print(host, file_path)
    return host, file_path

if __name__=="__main__":
    host, file_path = getHostPath()
    if (not host) or (not file_path):
        print("Error2")
        exit(0)
    print(host)
    f = generator(file_path)

    producer = KafkaProducer(bootstrap_servers=host, 
                    key_serializer=lambda k: k.encode(), 
                    value_serializer=lambda v: v.encode())
    while True:
        try:
            plain_data = next(f)
            print(plain_data)
            future = producer.send(
                'movie_rating_records', 
                key=plain_data['headers']['key'],
                value=plain_data['body'])
            future.get(timeout=10) # 监控是否发送成功           
        except StopIteration:
            break
    print("It's over!")


