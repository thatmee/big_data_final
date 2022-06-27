import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import java.util
import collection.JavaConverters._

object kafkaStreaming {
  def getSc() = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("kafkaConsumer")
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(30))
    streamingContext
  }
  def isEqual(x:(Int,Float), y:Float):List[(Int, Int)] = {
    if (x._2==y)
      List((x._1,1))
    else
      List()
  }
  def getStream(sc:StreamingContext) = {
    val kafkaParams = Map[String, Object] (
      "bootstrap.servers" -> "nyf-2019211193-0001:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkaStreaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "partition.assignment.strategy"->"org.apache.kafka.clients.consumer.RangeAssignor"
    )
    val topics = Array("movie_rating_records")
    val stream = KafkaUtils.createDirectStream[String,String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
  def streamingCore(sc: StreamingContext): Unit = {
    val stream = kafkaStreaming.getStream(sc).map(x=>{
      val json: JSONObject = JSON.parseObject(x.value())
      (x.key(),json.get("userId").toString.toInt,json.get("movieId").toString.toInt,json.get("rating").toString.toFloat,json.get("timestamp").toString)
    })

    //缓存最近的记录,写入redis && hbase
    stream.foreachRDD( rdd =>
      rdd.foreachPartition { part => {
        //redis connection
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        //hbase connection
        val config = HBaseConfiguration.create
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.zookeeper.quorum", "nyf-2019211193-0001")
        val connection = ConnectionFactory.createConnection(config)
        val table = connection.getTable(TableName.valueOf("movie_records"))
        var records: List[Put] = List()
        part.foreach { x => {
          //写入redis
          while (jedisIns.llen("streaming_records") >= 200) jedisIns.lpop("streaming_records")
          val record: Map[String, String] = Map("userId" -> x._2.toString,
                                                "movieId" -> x._3.toString,
                                                "rating" -> x._4.toString,
                                                "timestamp" ->x._5.toString)
          //              println(record)
          //              println(JSON.toJSONString(record.asJava,SerializerFeature.WriteMapNullValue))
          jedisIns.rpush("streaming_records", JSON.toJSONString(record.asJava, SerializerFeature.WriteMapNullValue))
          //写入hbase
          val put = new Put(Bytes.toBytes(x._1))
          put.addColumn(Bytes.toBytes("details"),Bytes.toBytes("userId"),Bytes.toBytes(record("userId")))
          put.addColumn(Bytes.toBytes("details"),Bytes.toBytes("movieId"),Bytes.toBytes(record("movieId")))
          put.addColumn(Bytes.toBytes("details"),Bytes.toBytes("rating"),Bytes.toBytes(record("rating")))
          put.addColumn(Bytes.toBytes("details"),Bytes.toBytes("timestamp"),Bytes.toBytes(record("timestamp")))
          records = records :+ put
        }
        import scala.collection.JavaConverters._
        table.put(records.asJava)
        table.close()
        jedisIns.close()
        }
      }
      }
    )
    //全局topK统计
    stream.map(x=>(x._3,1))
      .reduceByKey((x,y)=>(x+y))
      .foreachRDD(line=> {
        line.sortBy(x => x._2, ascending = false).take(10).foreach(
          x => {
            val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
            jedisIns.auth("redis_passwd")
            while (jedisIns.llen("popular_movies_all")>=30) jedisIns.lpop("popular_movies_all")
            jedisIns.rpush("popular_movies_all",x._1.toString)
            jedisIns.close()
          }
        )
      }
      )
    //热点topK统计
    stream.flatMap(x=> {
      var seqList: Seq[(Int, (Int, Int))] = Seq()
      val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
      jedisIns.auth("redis_passwd")
      val genresList = jedisIns.lrange(s"movie2genres_movieId_${x._3}",0,-1)
      val it = genresList.iterator()
      while (it.hasNext) {
        val genresId = it.next().toInt
        seqList = seqList :+ (genresId,(x._3,1))
      }
      jedisIns.close()
      seqList
    }).groupByKey()
      //写入redis
      .mapValues(records=>{
        val answers: Array[(Int, Int)] = new Array[(Int, Int)](11)
        var len = 0
        for(record <- records) {
          answers(len) = record
          var i = len
          while (i>0 && answers(i-1)._2<answers(i)._2) {
            val tmp = answers(i-1)
            answers(i-1) = answers(i)
            answers(i) = tmp
            i-=1
          }
          len+=1
          if (len==11){
            len = 10
          }
        }
        (len,answers)
      })
      .foreachRDD(
        rdd => rdd.foreach{
        x => {
          val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
          jedisIns.auth("redis_passwd")
          jedisIns.del(s"popular_movies_genreId_${x._1}")
          for (i <- 0 until x._2._1) {
            jedisIns.rpush(s"popular_movies_genreId_${x._1}", x._2._2(i)._1.toString)
          }
          jedisIns.close()
        }
      }
      )
//
    //特征抽取
    //统计 a)用户历史正反馈次数
    val counterUserIdPos = stream.flatMap(x => isEqual((x._2,x._4),1.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 b)用户历史负反馈次数
    val counterUserIdNeg = stream.flatMap(x => isEqual((x._2,x._4),0.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 c)电影历史正反馈次数
    val counterMovieIdPos = stream.flatMap(x => isEqual((x._3,x._4),1.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 d)电影历史负反馈次数
    val counterMovieIdNeg = stream.flatMap(x => isEqual((x._3,x._4),0.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 e)用户历史点击该分类比例
    val counterUserId2MovieId = stream.filter(x=>x._4==1.0)
      .map(x=>(x._2,x._3))
      .groupByKey()
      .flatMapValues(x=>{
        var sum = 0
        val one_hot: Array[Int] = new Array[Int](19)
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        for (record<-x) {
          sum=sum+1
          val genres_list = jedisIns.lrange("movie2genres_movieId_" + record.toString,0,-1)
          val it = genres_list.iterator()
          while (it.hasNext) {
            val genresId = it.next().toInt
            one_hot(genresId) = one_hot(genresId)+1
          }
        }
        var counter:List[(Int,Float)] = List()
        for (i<-one_hot.indices) {
          if (one_hot(i)>0) counter = counter :+ (i,one_hot(i).toFloat/sum)
        }
        jedisIns.close()
        counter
      })
    counterUserIdPos.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set("streaming2feature_userId_rating1_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterUserIdNeg.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set("streaming2feature_userId_rating0_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterMovieIdPos.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set("streaming2feature_movieId_rating1_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterMovieIdNeg.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set("streaming2feature_movieId_rating0_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterUserId2MovieId.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set(s"streaming2feature_userId_to_genresId_${x._1.toString}_${x._2._1}", x._2._2.toString)
        jedisIns.close()
      }}
    )
  }
  val redis_host:String = "nyf-2019211193-0001"
  val redis_port:Int = 6379
  val redis_timeout:Int = 10000
  def main(args:Array[String]) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sc:StreamingContext = kafkaStreaming.getSc()
    streamingCore(sc)
    sc.start()
    sc.awaitTermination()
  }
}
