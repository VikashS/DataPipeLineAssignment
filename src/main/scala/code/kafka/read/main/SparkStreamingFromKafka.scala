package code.kafka.read.main

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingFromKafka extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setAppName("KafkaDirectStreaming").setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  val kafkaTopicRaw = "mytopics"
  val kafkaBroker = "127.0.0.1:9092"
  val topicsSet = Set("mytopics")
  //ssc.checkpoint("checkpointDir")
  val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
  val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
  val tweets: DStream[String] = messages.map { case (key, message) => message }
  AggregationAndInsert.createTwitterView(ssc.sparkContext, tweets)

  ssc.start()
  ssc.awaitTermination()

}
