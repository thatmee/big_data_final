import breeze.linalg.Broadcaster
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.lang.Thread.sleep
import java.text.SimpleDateFormat
import java.util.Date

object hbase2spark {
  def getHBaseConfiguration(quorum:String, port:String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)
    conf
  }
  def isEqual(x:(Int,Float), y:Float):List[(Int, Int)] = {
    if (x._2==y)
      List((x._1,1))
    else
      List()
  }
  def batch2feature(sc:SparkContext) {
    val hbaseconf = getHBaseConfiguration("nyf-2019211193-0001","2181")
    hbaseconf.set(TableInputFormat.INPUT_TABLE,"movie_records")
    // HBase数据转成RDD
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseconf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    // RDD数据操作
    val data = hBaseRDD.map(x => {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      val rating = Bytes.toString(result.getValue("details".getBytes,"rating".getBytes)).toFloat
      val userId = Bytes.toString(result.getValue("details".getBytes,"userId".getBytes)).toInt
      val movieId = Bytes.toString(result.getValue("details".getBytes,"movieId".getBytes)).toInt
      val timestamp = Bytes.toString(result.getValue("details".getBytes,"timestamp".getBytes))
      (key,userId,movieId,rating,timestamp)
    })

    //统计 a)用户历史正反馈次数
    val counterUserIdPos = data.flatMap(x => isEqual((x._2,x._4),1.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 b)用户历史负反馈次数
    val counterUserIdNeg = data.flatMap(x => isEqual((x._2,x._4),0.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 c)电影历史正反馈次数
    val counterMovieIdPos = data.flatMap(x => isEqual((x._3,x._4),1.0.toFloat))
      .reduceByKey((x,y)=> x+y)
    //统计 d)电影历史负反馈次数
    val counterMovieIdNeg = data.flatMap(x => isEqual((x._3,x._4),0.0.toFloat))
      .reduceByKey((x,y)=> x+y)

    //统计 e)用户历史点击该分类比例
    val counterUserId2MovieId = data.filter(x=>x._4==1.0)
      .map(x=>(x._2,x._3))
      .groupByKey()
      .flatMapValues(x=>{
        var sum = 0
        val one_hot: Array[Int] = new Array[Int](19)
//        val jedisIns = new JedisIns("bd",6379,100000)
        val jedisIns:Jedis = new Jedis("nyf-2019211193-0001",6379,100000)
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
        jedisIns.close()
        var counter:List[(Int,Float)] = List()
        for (i<-one_hot.indices) {
          if (one_hot(i)>0) counter = counter :+ (i,one_hot(i).toFloat/sum)
        }
        counter
      })
    // 依次输出统计结果
    counterUserIdPos.foreach( x=> {
      val jedisIns = new Jedis("nyf-2019211193-0001",6379,100000)
      jedisIns.auth("redis_passwd")
      jedisIns.set("batch2feature_userId_rating1_"+x._1.toString, x._2.toString)
      jedisIns.close()
    })
    counterUserIdNeg.foreach( x=> {
      val jedisIns = new Jedis("nyf-2019211193-0001",6379,100000)
      jedisIns.auth("redis_passwd")
      jedisIns.set("batch2feature_userId_rating0_"+x._1.toString, x._2.toString)
      jedisIns.close()
    })
    counterMovieIdPos.foreach( x=> {
      val jedisIns = new Jedis("nyf-2019211193-0001",6379,100000)
      jedisIns.auth("redis_passwd")
      jedisIns.set("batch2feature_movieId_rating1_"+x._1.toString, x._2.toString)
      jedisIns.close()
    })
    counterMovieIdNeg.foreach( x=> {
      val jedisIns = new Jedis("nyf-2019211193-0001",6379,100000)
      jedisIns.auth("redis_passwd")
      jedisIns.set("batch2feature_movieId_rating0_"+x._1.toString, x._2.toString)
      jedisIns.close()
    })
    counterUserId2MovieId.foreach(x=> {
      val jedisIns = new Jedis("nyf-2019211193-0001",6379,100000)
      jedisIns.auth("redis_passwd")
      jedisIns.set(s"batch2feature_userId_to_genresId_${x._1.toString}_${x._2._1}", x._2._2.toString)
      jedisIns.close()
    })
  }
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }
  def main(args: Array[String]): Unit = {
    // Spark
//    val jedisIns = new JedisIns("bd",6379,100000)
//    jedisIns.testJedis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    while (true) {
      println(s"${NowDate()} [INFO] Begin to calculate batch features")
      val sparkConf = new SparkConf().setAppName("HBaseReadTest").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
//      val broadcast: Broadcast[JedisPoolUtil] = sc.broadcast(jedis_pool)
      batch2feature(sc)
      sc.stop()
      println(s"${NowDate()} [INFO] Success!")
      sleep(1000*60*5)
    }
  }
}