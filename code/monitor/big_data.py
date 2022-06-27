# -------import---------
import imp

from pyspark.sql import SparkSession
import json
import redis
from pyecharts.charts import Bar
from pyecharts import options as opts
import time


# -------params---------
# spark
spark_host = "local[1]"
app_name = "test_spark_sql"
spark_session = SparkSession.builder.master(
    spark_host).appName(app_name).getOrCreate()

# hbase
hbase_host = "nyf-2019211193-0001"
table_name = "movie_records"
conf = {
    "hbase.zookeeper.quorum": hbase_host,
    "hbase.mapreduce.inputtable": table_name
}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_structure = ["movieId", "rating", "timestamp", "userId"]

# id2name
id_name = {
    "0": "Documentary",
    "1": "Mystery",
    "2": "Fantasy",
    "3": "Action",
    "4": "Children",
    "5": "Comedy",
    "6": "Film-Noir",
    "7": "Adventure",
    "8": "Drama",
    "9": "Thriller",
    "10": "Musical",
    "11": "War",
    "12": "IMAX",
    "13": "Animation",
    "14": "Romance",
    "15": "Western",
    "16": "Sci-Fi",
    "17": "Crime",
    "18": "Horror"
}
# name2cnt
name_cnt = {
    "Documentary": 0,
    "Mystery": 0,
    "Fantasy": 0,
    "Action": 0,
    "Children": 0,
    "Comedy": 0,
    "Film-Noir": 0,
    "Adventure": 0,
    "Drama": 0,
    "Thriller": 0,
    "Musical": 0,
    "War": 0,
    "IMAX": 0,
    "Animation": 0,
    "Romance": 0,
    "Western": 0,
    "Sci-Fi": 0,
    "Crime": 0,
    "Horror": 0
}

# redis
pool = redis.ConnectionPool(host="nyf-2019211193-0001", port=6379,
                            decode_responses=True, password='redis_passwd')


# -------funtion---------
# 将RDD转化为DataFrame
def row_transform(row_cells_info, hbase_structure):
    row_cell_info_list = [json.loads(i) for i in row_cells_info]
    row_dict = {}

    hbase_index = 0
    for cell_index in range(len(row_cell_info_list)):
        column_name = row_cell_info_list[cell_index]['qualifier']
        column_value = row_cell_info_list[cell_index]['value']
        if hbase_structure[hbase_index] == column_name:
            row_dict[column_name] = column_value
            hbase_index += 1
        else:
            row_dict[hbase_structure[hbase_index]] = "Null"
            for j in range(hbase_index + 1, len(hbase_structure)):
                if hbase_structure[j] == column_name:
                    row_dict[column_name] = column_value
                    hbase_index = j + 1
                    break
                else:
                    row_dict[hbase_structure[j]] = "Null"
    for j in range(hbase_index, len(hbase_structure)):
        row_dict[hbase_structure[j]] = "Null"

    return row_dict


def rdd_to_df(hbase_rdd, hbase_structure):
    # 同一个RowKey对应的列之间是用\n分割，进行split，split后每列是个dict
    data_rdd_split = hbase_rdd.map(lambda x: (x[0], x[1].split('\n')))
    # 提取列名和取值
    data_rdd_columns = data_rdd_split.map(
        lambda x: (x[0], row_transform(x[1], hbase_structure)))
    data = data_rdd_columns.map(lambda x: [x[0]] + [x[1][i] for i in x[1]])
    data_df = spark_session.createDataFrame(
        data, ["row_key"] + hbase_structure)

    return data_df


# -------main---------
while True:
    # rdd
    hbase_rdd = spark_session.sparkContext.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf
    )

    # dataframe
    hbase_df = rdd_to_df(hbase_rdd, hbase_structure)
    # print("\n\n=============================")
    # print(type(hbase_df))
    # print(hbase_df.head())
    # print("=============================\n\n")

    # connect to redis
    r = redis.Redis(connection_pool=pool)

    # 遍历dataframe
    for row in hbase_df.collect():
        movie_id = int(row['movieId'])
        # print("movie_id : " + str(movie_id))
        # 通过movie_id在redis里查找对应的genre_id
        genres = r.lrange(f'movie2genres_movieId_{movie_id}', 0, -1)
        for genre_id in genres:
            genre_name = id_name[genre_id]
            name_cnt[genre_name] += 1
    r.close()

    # html-bar展示统计结果
    bar = (
        Bar(init_opts=opts.InitOpts(
            animation_opts=opts.AnimationOpts(animation=False)))
        .add_xaxis(list(name_cnt.keys()))
        .add_yaxis('count of records', list(name_cnt.values()))
        .set_global_opts(
            title_opts=opts.TitleOpts(title="电影记录按类别统计直方图"),
            # datazoom_opts=opts.DataZoomOpts(start_value=100),
            xaxis_opts=opts.AxisOpts(axislabel_opts={"rotate": -45}))
    )
    bar.render('/var/www/html/movie_genre_bar.html')  # 渲染

    # 实时刷新
    with open('/var/www/html/movie_genre_bar.html', 'a+') as f:
        refresh = '<meta http-equiv="Refresh" content="3";/> <!--页面每1秒刷新一次-->'
        f.write(refresh)

    print("\n\n=============================")
    print('html ready')
    print("=============================\n\n")

    # 每30s更新一次
    time.sleep(30)
