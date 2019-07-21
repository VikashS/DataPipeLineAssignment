package code.kafka.read.main

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SparkStreamingFromKafka extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf().setAppName("KafkaDirectStreaming").setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.auth.username", "cassandra")
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val kafkaTopicRaw = "mytopics"
  val kafkaBroker = "127.0.0.1:9092"
  val topicsSet = Set("mytopics")

  val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
  val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
  val tweets: DStream[String] = messages.map { case (key, message) => message }
  tweets.foreachRDD { (rdd: RDD[String], time: Time) =>
    if (rdd.count() == 0) {
      println("Kafka Down")
    } else {
      val tweets: DataFrame = spark.sqlContext.read.json(rdd)
      tweets.createOrReplaceTempView("mytable")
      val wordCountsDataFrame: DataFrame = spark.sql("SELECT id,created_at,favorite_count from mytable Where retweet_count >= 0 ")
      wordCountsDataFrame.write.mode(SaveMode.Append)
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "twitterdata", "keyspace" -> "vkspace"))
        .save()
      wordCountsDataFrame.show(false)
    }
  }
  //ssc.checkpoint("checkpointDir")
  ssc.start()
  ssc.awaitTermination()
}
