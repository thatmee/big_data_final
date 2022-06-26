import com.alibaba.fastjson
import com.alibaba.fastjson.JSON.parseObject
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row,DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.io.{BufferedWriter, FileWriter}
import java.lang.Thread.sleep
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.{Collections, Date, Properties}
import scala.collection.mutable
import scala.concurrent.duration.Duration

object recommend {
  def add_record(map:mutable.Map[String,Int],jedis: Jedis,json:JSONObject) = {
    //    println(json)
    val userId = json.getOrDefault("userId",null).toString.toInt
    val movieId = json.getOrDefault("movieId",null).toString.toInt
    val rating = json.getOrDefault("rating",null).toString.toFloat.toInt
    if (rating == 1.0) {
      //统计 a)用户历史正反馈次数
      var key = s"counteruserId1_${userId.toString}"
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt+1)
      //统计 c)电影历史正反馈次数
      key = s"countermovieId1_${movieId.toString}"
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt+1)
      //统计 e)用户历史点击该分类比例
      key = s"counteruserId2movie_${userId}_${movieId}"
      val genres = jedis.lrange(s"movie2genres_movieId_${movieId}",0,-1)
      val it = genres.iterator()
      while (it.hasNext) {
        val genresId = it.next().toInt
        val key2 = s"counteruserId2genre_${userId}_${genresId}"
        map += key2 -> (map.getOrElseUpdate(key2, 0)+1)
      }
    }
    else {
      //统计 b)用户历史负反馈次数
      var key = "counteruserId0_" + userId.toString
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt+1)
      //统计 d)电影历史负反馈次数
      key = "countermovieId0_" + movieId.toString
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt+1)
    }
    map
  }
  def delete_record(map:mutable.Map[String,Int],jedis: Jedis,json:JSONObject) = {
    val userId = json.getOrDefault("userId",null).toString.toInt
    val movieId = json.getOrDefault("movieId",null).toString.toInt
    val rating = json.getOrDefault("rating",null).toString.toFloat.toInt
    if (rating == 1.0) {
      //统计 a)用户历史正反馈次数
      var key = s"counteruserId1_${userId.toString}"
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt-1)
      //统计 c)电影历史正反馈次数
      key = s"countermovieId1_${movieId.toString}"
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt-1)
      //统计 e)用户历史点击该分类比例
      key = s"counteruserId2movie_${userId}_${movieId}"
      val genres = jedis.lrange(s"movie2genres_movieId_${movieId}",0,-1)
      val it = genres.iterator()
      while (it.hasNext) {
        val genresId = it.next().toInt
        val key2 = s"counteruserId2genre_${userId}_${genresId}"
        map += key2 -> (map.getOrElseUpdate(key2, 0)-1)
      }
    }
    else {
      //统计 b)用户历史负反馈次数
      var key = "counteruserId0_" + userId.toString
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt-1)
      //统计 d)电影历史负反馈次数
      key = "countermovieId0_" + movieId.toString
      map += key-> (map.getOrElseUpdate(key, 0).toString.toInt-1)
    }
    map
  }
  //  def getRedisList(jedis: Jedis,key:String) = {
  //    val value = jedis.lrange(key,0,-1)
  //    jedis.close()
  //    value
  //  }
  def collect_train_data() = {
    val jedis = new Jedis(redis_host,redis_port,redis_timeout)
    jedis.auth("1Cuk1Be4O^4aXx3LL33=")
    val records_length = jedis.llen("streaming_records")
    var map: mutable.Map[String, Int] = mutable.Map()
    for(i <- 0 to records_length.toInt/2) {
      val json = JSON.parseObject(jedis.lindex("streaming_records",i))
      map = add_record(map,jedis,json)
    }
    var data:Seq[(Double,Vector)] = Seq()
    for(i <- records_length.toInt / 2 + 1 until records_length.toInt) {
      var json = JSON.parseObject(jedis.lindex("streaming_records",i-records_length.toInt/2))
      map = delete_record(map,jedis,json)
      json = JSON.parseObject(jedis.lindex("streaming_records",i))
      map = add_record(map,jedis,json)
      val userId = json.getOrDefault("userId",null).toString.toInt
      val movieId = json.getOrDefault("movieId",null).toString.toInt
      val rating = json.getOrDefault("rating",null).toString.toDouble
      var features:Seq[Double] = Seq()
      for (key<-Array(
        s"batch2feature_userId_rating1_${userId}",
        s"batch2feature_userId_rating0_${userId}",
        s"batch2feature_movieId_rating1_${movieId}",
        s"batch2feature_movieId_rating0_${movieId}")) {
        features = features :+ {
          if (jedis.exists(key)) {
            jedis.get(key).toDouble
          } else
            0
        }
      }
      var sum:Double = 0.0
      val key = s"batch2feature_userId_to_genresId_${userId}"
      for(i <- 0 to 19) {
        sum = sum + {
          if (jedis.exists(s"${key}_${i}"))
            jedis.get(s"${key}_${i}").toDouble
          else
            0
        }
      }
      features = features :+ sum
      for (key<-Array(
        s"counteruserId1_${userId}",
        s"counteruserId0_${userId}",
        s"countermovieId1_${movieId}",
        s"countermovieId0_${movieId}")) {
        features = features :+ map.getOrElseUpdate(key,0).toDouble
      }
      val counteruserIdsum =  map.getOrElseUpdate(s"counteruserId1_${userId}",0) + map.getOrElseUpdate(s"counteruserId0_${userId}",0)
      //简单完成一个加和即可
      sum = 0.0
      for(i <- 0 to 19) {
        sum = sum + {
          val value = map.getOrElseUpdate(s"counteruserId2genre_${userId}_${i}",0).toDouble
          if (value==0)
            0.toDouble
          else
            value/counteruserIdsum.toDouble
        }
      }
      features = features :+ sum
      data = data :+ (rating, Vectors.dense(features.toArray))
    }
    jedis.close()
    data
  }

  def trainRecommendModel(spark:SparkSession) = {
    val train_data = collect_train_data()
    val train_dataframe = spark.createDataFrame(train_data).toDF("label","features")
    train_dataframe.show(20,truncate = false)
    import spark.implicits._
    //    val training = spark.read.format("libsvm").load("./sample_libsvm_data.txt")
    val lr = new LogisticRegression()
      .setMaxIter(20)

    // Fit the model
    val lrModel = lr.fit(train_dataframe)
    //    val trainingSummary = lrModel.binarySummary
    //    val objectiveHistory = trainingSummary.objectiveHistory
    //    println("objectiveHistory:")
    //    objectiveHistory.foreach(loss => println(loss))
    //    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    val params_coefficients = lrModel.coefficients.toDense.toArray
    println(params_coefficients.mkString("Array(", ", ", ")"))
    val jedis = new Jedis(redis_host,redis_port,redis_timeout)
    jedis.auth("1Cuk1Be4O^4aXx3LL33=")
    jedis.del("params_coefficients")
    for (i<- params_coefficients.indices)
      jedis.rpush("params_coefficients",params_coefficients(i).toString)
    jedis.set("params_intercept",lrModel.intercept.toString)

  }
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }
  val redis_host:String = "nyf-2019211193-0001"
  val redis_port:Int = 6379
  val redis_timeout:Int = 10000
  def main(args:Array[String]) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    while (true) {
      print(s"${NowDate()} [INFO] Begin to train lr model")
      val spark = SparkSession.builder().master("local[2]").appName("ALSExample").getOrCreate()
      trainRecommendModel(spark)
      spark.close()
      print(s"${NowDate()} [INFO] Success!")
      sleep(1000*60*5)
    }
  }
}
