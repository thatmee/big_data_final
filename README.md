# big_data_final
## 基础部分配置
### 可能出现的问题
1. 各种依赖包问题，按照如下顺序安装：
   ```bash
   yum install python3-devel
   pip3 install happybase
   python3 -m pip install --upgrade --force pip
   pip3 install setuptools==33.1.1
   pip3 install pandas
   pip3 install redis
   pip3 install kafka
   ```
2. 要记得修改 6379 和 9090 端口的准入规则
3. 注意随时使用 jps 等命令检查各个组件运行情况
### 启动命令
1. 启动 HDFS 
   
   `/home/modules/hadoop-2.7.7/sbin/start-all.sh`
2. 启动zookeeper（所有节点都要运行）
   
   `/usr/local/zookeeper/bin/zkServer.sh start`
3. 启动HBase
   
   `/usr/local/hbase/bin/start-hbase.sh`
4. 配置 HBase Thrift 连接，以便 python 中的 happybase 库能够连接 Hbase：
   
   `/usr/local/hbase/bin/hbase-daemon.sh start thrift`
5. 上传所需的代码和数据文件，分别放到 `/root/data` 和 `/root/code`
6. 在主节点服务器上启动 load_train_ratings_hbase.py
   
   `python3 /root/code/load_train_ratings_hbase.py 117.78.0.185 9090 "movie_records" "/root/data/json_train_ratings.json"`
7. 启动 redis
   
   `redis-server redis-6.0.6/redis.conf`
8. 在主节点服务器上启动 load_movie_redis.py 
   
   `python3 /root/code/load_movie_redis.py 117.78.0.185 6379 "/root/data/movies.csv"`
9.  在所有节点上启动 Kafka：
    
    `/home/modules/kafka_2.11-0.10.2.2/bin/kafka-server-start.sh /home/modules/kafka_2.11-0.10.2.2/config/server.properties`
10. 创建 Kafka Topic（只需创建一次）
    
    `/home/modules/kafka_2.11-0.10.2.2/bin/kafka-topics.sh --zookeeper nyf-2019211193-0001:2181 --create --topic movie_rating_records --partitions 1 --replication-factor 1`
11. 在主节点服务器上启动 generatorRecord.py
    
    `python3 /root/code/generatorRecord.py -h nyf-2019211193-0001:9092  -f "/root/data/json_test_ratings.json"` 
12. spark 提交 hbase2spark 任务
    
    `/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class hbase2spark --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 recommend.jar > /dev/null 2>&1 &`
13. spark 提交 kafkaStreaming 任务
    
    `/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class kafkaStreaming --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 recommend.jar > /dev/null 2>&1 &`
14. spark提交recommend任务
    
    `/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class recommend --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 recommend.jar > /dev/null 2>&1 &`
15. 在主节点服务器上启动 recommend_server.py
    
    `python3 /root/code/recommend_server.py "117.78.0.185" 6379 23456`
16. 在主节点服务器上启动 recommend_client.py
    
    `python3 /root/code/recommend_client.py nyf-2019211193-0001 23456`
17. 在客户端输入任意用户 id，即可查看推荐结果

## 后台监控系统
### 实现思路
1. 我们能从 HBase 中获取到 movieId、userId、rating 等信息，能从 redis 中获取到 movieId 对应的类别代号，从 0-18 的代号分别代表"Documentary"、"Mystery"、"Fantasy"、"Action"、"Children"、"Comedy"、"Film-Noir"、"Adventure"、"Drama"、"Thriller"、"Musical"、"War"、"IMAX"、"Animation"、"Romance"、"Western"、"Sci-Fi"、"Crime"、"Horror" 这 19 种电影类型。
2. 由上述观察，要想统计不同类别的电影的记录数，可以维护一个以电影类别代号作关键字的字典，每隔 30s 获取 HBase 中的记录，对其中的每一个电影 id，在 redis 中查询其类别，并在字典中更新信息，最后将字典的数据通过 echarts 等方法绘制到 html 中。
3. 生成 html 后，利用 httpd 部署静态网页：[centos下利用httpd搭建http服务器方法 - yvhqbat - 博客园](https://www.cnblogs.com/walkinginthesun/p/9543001.html)，从而实现了简易后台监控系统。
### 启动方法
1. 正常启动基础版推荐系统
2. 运行 `./monitor.sh`
3. 在浏览器中输入 `http://hostname:7777/movie_genre_bar.html` 查看统计结果

## 提高部分
### 实现思路
1. 仍然使用 kafka 作为数据流，在流处理和批处理部分都使用 SparkStreaming，通过设置 kafka 消费者的 offset 来获取全局历史数据和实时数据，从而将批处理和流处理整合在一个代码逻辑中。
2. 由于批处理直接从 kafka 中获取数据，而不再需要 HBase 了，我们直接取消了 HBase，在原始 lambda 架构中，HBase 除了用于批处理的数据源，还用于加载初始的训练数据、做后台监控系统的数据源。
   - 我们对训练数据的解决方法是，重新实现了一个 python 文件，直接将训练数据提取特征并存储到 redis 中。
   - 对于后台监控，可能的解决方法是，直接从 kafka 流获取数据，通过 scala、echarts 进行统计和展示。（由于时间原因，后台监控没有在提高部分中实现）

综上，对文件的功能和修改情况整理如下：
```
big_data_final
├─ README.md
├─ code
│    ├─ monitor
│    │    ├─ monitor.sh                   + 启动后台监控，并完成相应的配置维护功能
│    │    └─ big_data.py                  √ 后台监控和可视化
│    ├─ load
│    │    ├─ generatorRecord.py           √ kafka 生产者，从 "data/json_test_rat
│    │    │                                 ings.json" 文件获取数据放到 kafka top
│    │    │                                 ic，模拟用户点击
│    │    ├─ load_movie_redis.py          √ 向 redis 中放入 movieId2movieName
│    │    ├─ load_train_ratings_hbase.py  ！（将训练数据加载到redis） 将原始训练数据
│    │    │                                 "data/json_train_ratings.json" 文件
│    │    │                                 加载到 hbase
│    │    └─ load_train_ratings_redis.py  + 提取原始训练数据特征，将特征加载到 redis
│    ├─ server-client
│    │    ├─ recommend_client.py          √ 推荐系统的客户端
│    │    └─ recommend_server.py          √ 推荐系统的服务器端，和客户端之间用
│    │                                      socket 通信，根据客户端发来的用户 id，
│    │                                      返回推荐商品
│    └─ spark-sparkstreaming-recommend
│           ├─ base_code_auth             √ 基础实验代码
│           ├─ pom.xml
│           ├─ v0_just1file               + 将实时和全局放在一个文件中处理
│           |    ├─ hbase2spark.scala     -（从 kafka 流获取历史数据，使用回放功
│           |    │                          能，放到 redis）# 用 spark 进行批处
│           |    │                          理，从 hbase 获取数据并计算“历史”特征
│           |    ├─ kafkaStreaming.scala  ！（获取的数据只需要存储到 redis 了）# ka
│           |    │                           fka 消费流，从 kafka topic 获取数据，
│           |    │                           存储到 redis和hbase，计算流式特征
│           |    └─ recommend.scala       √ 推荐系统，从 redis 获取数据，完成训练
│           └─ v1_split2files             + 将实时和全局分开两个文件处理
└─ data
     ├─ json_test_ratings.json
     ├─ json_train_ratings.json
     ├─ movies.csv
     └─ train.json
```
参考资料：

- [实时数仓之 Kappa 架构与 Lambda 架构](https://new.qq.com/omn/20220204/20220204A08WXH00.html)

- [大数据之Kappa架构_we.think的博客-CSDN博客_kappa架构](https://blog.csdn.net/ioteye/article/details/114776345)
### 启动命令
1. 启动 HDFS 
   
   `/home/modules/hadoop-2.7.7/sbin/start-all.sh`
2. 启动zookeeper（所有节点都要运行）
   
   `/usr/local/zookeeper/bin/zkServer.sh start`
3. 启动 redis
   
   `redis-server redis-6.0.6/redis.conf`
4. 在主节点服务器上启动 load_movie_redis.py 
   
   `python3 /root/code/load_movie_redis.py 117.78.0.185 6379 "/root/data/movies.csv"`

5. 在主节点服务器上启动 load_train_ratings_redis.py
   
   `python3 /root/code/load_train_ratings_redis.py 117.78.0.185 6379 "/root/data/train.json"`

6. 在所有节点上启动 Kafka：
   
   `/home/modules/kafka_2.11-0.10.2.2/bin/kafka-server-start.sh /home/modules/kafka_2.11-0.10.2.2/config/server.properties`
7. 若没有 kafka topic，则需创建 kafka topic，此处不需要
8. 在主节点服务器上启动 generatorRecord.py
   
   `python3 /root/code/generatorRecord.py -h nyf-2019211193-0001:9092  -f "/root/data/json_test_ratings.json"` 
9. spark 提交 kafkaStreaming 任务
    
    `/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class kafkaStreaming --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 advance2.jar > /dev/null 2>&1 &`
10. spark提交recommend任务
    
    `/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class recommend --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 advance2.jar > /dev/null 2>&1 &`
11. 在主节点服务器上启动 recommend_server.py
    
    `python3 /root/code/recommend_server.py "117.78.0.185" 6379 23456`
12. 在主节点服务器上启动 recommend_client.py
    
    `python3 /root/code/recommend_client.py nyf-2019211193-0001 23456`