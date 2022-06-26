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


def randomGen(file_path):
    random.seed(19960106)
    with open(file_path,"r") as file:
        line = file.readline()
        while line:
            yield json.loads(line.strip("\n"))
            # yield line.strip("\n")
            line = file.readline()
            time.sleep(random.randint(0,30)/10)

def genTrain(train_path):
    with open(train_path,"r") as file:
        line = file.readline()
        while line:
            yield json.loads(line.strip("\n"))
            # yield line.strip("\n")
            line = file.readline()

def getHostPath():
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv,"h:f:")
    except:
        print("Error")
        return None, None
    host, test_path, train_path = None, None, None
    for opt, arg in opts:
        if opt in ['-h']:
            host = arg
        elif opt in ['-f']:
            test_path = arg
        elif opt in ['-t']:
            train_path = arg
    print(host, test_path, train_path)
    return host, test_path, train_path

if __name__=="__main__":
    host, test_path, train_path = getHostPath()
    if (not host) or (not test_path) or (not train_path):
        print("Error2")
        exit(0)
    print(host)
    producer = KafkaProducer(bootstrap_servers=host, 
                    key_serializer=lambda k: k.encode(), 
                    value_serializer=lambda v: v.encode())

    # 先发送所有训练数据
    t = genTrain(train_path)
    print("start sending training data.")
    while True:
        try:
            plain_data = next(t)
            # print(plain_data)
            future = producer.send(
                'movie_rating_records', 
                key=plain_data['headers']['key'],
                value=plain_data['body'])
            future.get(timeout=10) # 监控是否发送成功           
        except StopIteration:
            break
    print("trainning data send over!")

    f = randomGen(test_path)
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