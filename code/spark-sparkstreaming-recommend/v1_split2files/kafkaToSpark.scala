import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.Jedis

object kafkaToSpark {
  def getSc(duration:Int) = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("kafkaConsumer2")
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(duration))
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
      "group.id" -> "kafkaToSpark",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "partition.assignment.strategy"->"org.apache.kafka.clients.consumer.RangeAssignor",
      "client.id" -> "batch_client"
    )
    val topics = Array("movie_rating_records")
    val stream = KafkaUtils.createDirectStream[String,String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
  def streamingCore(sc: StreamingContext, task: String): Unit = {
    val stream = kafkaToSpark.getStream(sc).map(x=>{
      val json: JSONObject = JSON.parseObject(x.value())
      (x.key(),json.get("userId").toString.toInt,json.get("movieId").toString.toInt,json.get("rating").toString.toFloat,json.get("timestamp").toString)
    })

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
        jedisIns.set(task + "2feature_userId_rating1_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterUserIdNeg.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set(task + "2feature_userId_rating0_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterMovieIdPos.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set(task + "2feature_movieId_rating1_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterMovieIdNeg.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set(task + "2feature_movieId_rating0_" + x._1.toString, x._2.toString)
        jedisIns.close()
      }}
    )
    counterUserId2MovieId.foreachRDD(
      rdd => rdd.foreach { x => {
        val jedisIns = new Jedis(redis_host,redis_port,redis_timeout)
        jedisIns.auth("redis_passwd")
        jedisIns.set(task + s"2feature_userId_to_genresId_${x._1.toString}_${x._2._1}", x._2._2.toString)
        jedisIns.close()
      }}
    )
  }
  val redis_host:String = "nyf-2019211193-0001"
  val redis_port:Int = 6379
  val redis_timeout:Int = 10000

  def main(args:Array[String]) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val batchSc:StreamingContext = kafkaToSpark.getSc(30)
    streamingCore(batchSc, "batch")
    batchSc.start()
    batchSc.awaitTermination()
  }
}
