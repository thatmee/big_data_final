# 作用：向 redis 中放入训练数据
# 启动参数：三个
#   - redis_host
#   - redis_port
#   - file_path
import pandas as pd
import sys
import redis
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
            line = file.readline()

if __name__=='__main__':
    host, port, file_path = getArgs()
    redis_pool = redis_connect(host, port)
    t = genTrain(file_path)
    df_train = pd.DataFrame(columns=['key','userId','movieId','rating','timestamp']) # str,int,int,float,int
    print("start sending training data.")

    for i in range(3):
        plain_data = next(t)  # dict
        insert_list = {'key':plain_data["headers"]["key"],'userId':plain_data["body"]["userId"], 'movieId':plain_data["body"]["movieId"],
                    'rating': plain_data["body"]["rating"], 'timestamp':plain_data["body"]["timestamp"]}
        df_train = df_train.append(insert_list, ignore_index=True)

    # 统计(1)(2)
    dict_rating_pos = {}
    dict_rating_neg = {}
    for i, row in df_train.iterrows():
        if row['rating'] == 1.0:
            if str(row['userId']) not in dict_rating_pos.keys():
                dict_rating_pos[str(row["userId"])] = 1
            else:
                dict_rating_pos[str(row["userId"])] += 1
        else:
            if str(row['userId']) not in dict_rating_neg.keys():
                dict_rating_neg[str(row["userId"])] = 1
            else:
                dict_rating_neg[str(row["userId"])] += 1
    for k, v in dict_rating_pos.items():
        r = redis.Redis(connection_pool=redis_pool)
        r.delete(f"batch2feature_userId_rating1_{k}")
        r.set(f"batch2feature_userId_rating1_{k}", str(v))
        r.close()
    for k, v in dict_rating_neg.items():
        r = redis.Redis(connection_pool=redis_pool)
        r.delete(f"batch2feature_userId_rating0_{k}")
        r.set(f"batch2feature_userId_rating0_{k}", str(v))
        r.close()

    # 统计(4)(5)
    dict_movie_pos = {}
    dict_movie_neg = {}
    for i, row in df_train.iterrows():
        if row['rating'] == 1.0:
            if str(row['movieId']) not in dict_movie_pos.keys():
                dict_movie_pos[str(row["movieId"])] = 1
            else:
                dict_movie_pos[str(row["movieId"])] += 1
        else:
            if str(row['movieId']) not in dict_movie_neg.keys():
                dict_movie_neg[str(row["movieId"])] = 1
            else:
                dict_movie_neg[str(row["movieId"])] += 1
    for k, v in dict_movie_pos.items():
        r = redis.Redis(connection_pool=redis_pool)
        r.delete(f"batch2feature_movieId_rating1_{k}")
        r.set(f"batch2feature_movieId_rating1_{k}", str(v))
        r.close()
    for k, v in dict_movie_neg.items():
        r = redis.Redis(connection_pool=redis_pool)
        r.delete(f"batch2feature_movieId_rating0_{k}")
        r.set(f"batch2feature_movieId_rating0_{k}", str(v))
        r.close()

    # 统计(3)
    # 维护一个dict
    # {user1:{g1:1, g2:2}, user2:{g1:2, g2:4}, user3:{}}
    dict_ratio = {}
    r = redis.Redis(connection_pool=redis_pool)
    for i, row in df_train.iterrows():
        if str(row['userId']) not in dict_ratio.keys():
            dict_ratio[str(row['userId'])] = {}
            movieId = row["movieId"]
            genres = r.lrange(f'movie2genres_movieId_{movieId}', 0, -1)
            for g_id in genres:  # g_id 是 str
                if g_id not in dict_ratio[str(row['userId'])].keys():
                    dict_ratio[str(row['userId'])][g_id] = 1
                else:
                    dict_ratio[str(row['userId'])][g_id] += 1
        else:
            movieId = row["movieId"]
            genres = r.lrange(f'movie2genres_movieId_{movieId}', 0, -1)
            for g_id in genres:
                if g_id not in dict_ratio[str(row['userId'])].keys():
                    dict_ratio[str(row['userId'])][g_id] = 1
                else:
                    dict_ratio[str(row['userId'])][g_id] += 1
    r.close()
    for k, v in dict_ratio.items():
        total = 0  # user1的总点击次数
        for k1, v1 in v.items():
            total += v1
        for k1, v1 in v.items():
            r = redis.Redis(connection_pool=redis_pool)
            r.delete(f"batch2feature_userId_to_genresId_{k}_{k1}")
            r.set(f"batch2feature_userId_to_genresId_{k}_{k1}", str(v1/total))
            print(f"batch2feature_userId_to_genresId_{k}_{k1}", str(v1/total))
            r.close()