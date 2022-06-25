# 作用：模拟服务端，连接redis，并监听端口等待客户端的连接
#   1. 请求redis中"popular_movie_all"
#   2. 请求redis中的特征
#   2. 请求redis中"movieId2movieTitle_xxx"
# 启动参数：三个
#   - redis_host
#   - redis_port
#   - listen_port:监听端口
import json
import socket
import redis
import sys
import pickle
import numpy as np
from math import e

def redis_connect(host="bd", port=6379):    
    pool = redis.ConnectionPool(host=host,port=port,decode_responses=True)
    return pool

def getArgs():
    argv = sys.argv[1:]
    return argv

def init_socket(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("",port))
    server_socket.listen(128)
    return server_socket

def send_data(cs, datas):
    byteStream = pickle.dumps(datas)
    length = len(byteStream)
    byteStream = bytes(f"{length:<16}", 'utf-8')+byteStream
    cs.sendall(byteStream)

def recv_data(cs):
    msg = cs.recv(1024)
    length = int(msg[:16])
    full_msg = b''
    full_msg += msg[16:]
    nowsize = len(full_msg)
    while nowsize < length:
        more = cs.recv(length - nowsize)
        full_msg = full_msg + more
        nowsize += len(more)
    return pickle.loads(full_msg)


def get_popular_list(r):
    popular_list = r.lrange("popular_movies_all",0,-1)
    return [int(x) for x in popular_list]

def get_liking_genre_list(r,userId):
    # 得到最喜欢
    genre_Id,best_rating = 0, 0.0
    for i in range(19):
        value = r.get(f"streaming2feature_userId_to_genresId_{userId}_{i}")
        if not value:
            continue
        value = float(value)
        if value>best_rating:
            genre_Id, best_rating = i, value
    # 
    liking_genre_list = r.lrange(f"popular_movies_genreId_{genre_Id}",0,-1)
    # 再求最喜欢的list
    return [int(x) for x in liking_genre_list]

def get_model_arams(redis):
    coefficients = redis.lrange("params_coefficients",0,-1)
    coefficients = [float(x) for x in coefficients]
    coefficients = np.array(coefficients)
    intercept = float(redis.get("params_intercept"))
    return coefficients,intercept


def get_features_userId(r, userId):
    user_features = dict()
    value = r.get(f"batch2feature_userId_rating1_{userId}")
    user_features["batch2feature_userId_rating1"] = float(value) if value else 0
    value = r.get(f"batch2feature_userId_rating0_{userId}")
    user_features["batch2feature_userId_rating0"] = float(value) if value else 0
    value = r.get(f"streaming2feature_userId_rating1_{userId}")
    user_features["streaming2feature_userId_rating1"] = float(value) if value else 0
    value = r.get(f"streaming2feature_userId_rating0_{userId}")
    user_features["streaming2feature_userId_rating0"] = float(value) if value else 0
    return user_features

def get_features_movieId(r, userId, movieId):
    movie_features = dict()
    value = r.get(f"batch2feature_movieId_rating1_{movieId}")
    movie_features["batch2feature_movieId_rating1"] = float(value)  if value else 0
    value = r.get(f"batch2feature_movieId_rating0_{movieId}")
    movie_features["batch2feature_movieId_rating0"] = float(value)  if value else 0
    value = r.get(f"streaming2feature_movieId_rating1_{movieId}")
    movie_features["streaming2feature_movieId_rating1"] = float(value)  if value else 0
    value = r.get(f"streaming2feature_movieId_rating0_{movieId}")
    movie_features["streaming2feature_movieId_rating0"] = float(value)  if value else 0
    return movie_features

def get_userId2movieId(r,userId,movieId):
    userId_movieId_features = dict()
    liking_genre_list = r.lrange(f"movie2genres_movieId_{movieId}",0,-1)
    sum = 0.0
    for liking_genrdId in liking_genre_list:
        value = r.get(f"batch2feature_userId_to_genresId_{userId}_{liking_genrdId}")
        sum += float(value) if value else 0
    userId_movieId_features[f'batch2feature_userId_to_movieId'] = sum
    sum = 0 
    for liking_genrdId in liking_genre_list:
        value = r.get(f"batch2feature_userId_to_genresId_{userId}_{liking_genrdId}")
        sum += float(value)  if value else 0
    userId_movieId_features[f'streaming2feature_userId_to_movieId'] = sum
    return userId_movieId_features

def sort_recall_list_by_model(r,userId,recalls,coefficients,intercept):
    user_features = get_features_userId(r, userId)
    rating = []
    coefficients = np.array(coefficients)
    for movieId in recalls: 
        movie_features = get_features_movieId(r, userId, movieId)
        user_movie_features = get_userId2movieId(r,userId,movieId)
        # print(f"movie_features={movieId}:{movie_features},{user_movie_features}")
        features = [0 for i in range(10)]
        # 批式：5维
        features[0],features[1] = user_features['batch2feature_userId_rating1'],user_features['batch2feature_userId_rating0']
        features[2],features[3] = movie_features['batch2feature_movieId_rating1'],movie_features['batch2feature_movieId_rating0']
        features[4] = user_movie_features[f'batch2feature_userId_to_movieId']
        # 实时：5维
        features[5],features[6] = user_features['streaming2feature_userId_rating1'],user_features['streaming2feature_userId_rating1']
        features[7],features[8] = movie_features['streaming2feature_movieId_rating1'],movie_features['streaming2feature_movieId_rating0']
        features[9] = user_movie_features[f'streaming2feature_userId_to_movieId']
        features = np.array(features)
        # print(features)
        # 模型计算
        pred = np.dot(features, coefficients) + intercept
        rating.append((movieId,e** (pred) / (1. + e** (pred))))
    rating.sort(key=lambda x:x[1],reverse=True)
    print(rating)
    return [x[0]for x in rating[:10]]

def get_recommend_list(redis,userId):
    # 1. 召回
    recall_list = []
    #   （1）载入popular_list
    recall_list += get_popular_list(r)
    #   （2）载入最喜欢的list
    recall_list += get_liking_genre_list(r,userId)
    recall_list = list(set(recall_list)) # 去重
    # print(recall_list)
    # 2. 排序
    coefficients, intercept = get_model_arams(r)
    recommend_list = sort_recall_list_by_model(r,userId,recall_list,coefficients,intercept) 
    # 发送推荐列表
    print(f"[INFO]userId:{userId}, list:{recommend_list}")
    print([r.get(f"movieId2movieTitle_{x}") for x in recommend_list])
    return [r.get(f"movieId2movieTitle_{x}") for x in recommend_list]
    
if __name__=="__main__":
    argv = getArgs()
    pool = redis_connect(argv[0],argv[1])
    r = redis.Redis(connection_pool=pool)
    # ----Test-------
    # userId = 51
    # print(get_recommend_list(r,userId))
    # ---------------
    server_socket = init_socket(int(argv[2]))
    client_socket, client_address = server_socket.accept()
    while True:
        r = redis.Redis(connection_pool=pool)
        userId = recv_data(client_socket)
        response = get_recommend_list(r,userId)
        send_data(client_socket,response)



