package code.kafka.read.main

import scala.concurrent.duration._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector

object TwitterDataAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("TwitterDataAnalysis")
    sparkConf.setIfMissing("spark.master", "local[5]")
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaTopicRaw = "mytopics"
    val kafkaBroker = "127.0.01:9092"


    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
    val rawTwitterStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    val hashTags = rawTwitterStream.flatMap(_.split(",")).filter(_.contains("@domAAdom"))
    val topTenPopularTwiite = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val counyVal = topTenPopularTwiite.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular channel in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    //ssc.checkpoint("addd the hdfs path from cloudera vm  hdfs://ip:8020/test")
    rawTwitterStream.print
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
