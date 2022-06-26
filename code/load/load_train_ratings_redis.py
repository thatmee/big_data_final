# 作用：向redis中放入movieId2movieName
# 启动参数：三个
#   - redis_host
#   - redis_port
#   - file_path
from pydoc import plain
from pandas.core import generic
import numpy as np
import pandas as pd
import sys
import redis
import random
import json

def redis_connect(host="bd", port=6379):    
    pool = redis.ConnectionPool(host=host, port=port, decode_responses=True, password='1Cuk1Be4O^4aXx3LL33=')
    return pool

def getArgs():
    argv = sys.argv[1:]
    return argv[0], argv[1], argv[2]

def genTrain(train_path):
    with open(train_path,"r") as file:
        line = file.readline()
        while line:
            yield json.loads(line.strip("\n"))
            # yield line.strip("\n")
            line = file.readline()

if __name__=='__main__':
    # host, port, file_path = getArgs()
    file_path = "data/train.json"
    # redis_pool = redis_connect(host, port)
    t = genTrain(file_path)
    df_train = pd.DataFrame(columns=['key','userId','movieId','rating','timestamp'])
    print("start sending training data.")

    for i in range(10):
        plain_data = next(t)
        df_tmp = pd.DataFrame([plain_data["headers"]["key"], plain_data["body"]["userId"], plain_data["body"]["movieId"], plain_data["body"]["rating"], plain_data["body"]["timestamp"]])
        df_train = df_train.append(df_tmp)
    print("trainning data send over!")
    print(df_train)


    # list_name = []

    # for i, nowl in enumerate(load_file(file_path)):
    #     print(nowl["headers"]["key"],nowl["body"]["userId"])
    #     if i == 10:
    #         break



        # r = redis.Redis(connection_pool=redis_pool)
        # r.delete(f"movieId2movieTitle_{nowl[0]}")
        # r.set(f"movieId2movieTitle_{nowl[0]}",str(nowl[1]))
        # genres = []
        # r.delete(f"movie2genres_movieId_{nowl[0]}")
        # for i in range(4,len(nowl)):
        #     # print(nowl[i],end=" || ")
        #     if int(nowl[i])==1:
        #         r.rpush(f"movie2genres_movieId_{nowl[0]}",i-4)
        # print()
        # r.close()




