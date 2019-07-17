package code.kafka.read.main

import code.kafka.read.main.TwitterSchema.RawTwitterData
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterDataAnalysis {

  val localLogger = Logger.getLogger("TwitterDataAnalysis")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TwitterDataAnalysis")
    sparkConf.setIfMissing("spark.master", "local[5]")
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaTopicRaw = "mytopics"
    val kafkaBroker = "127.0.01:9092"
    //cassandra details
    val cassandraKeyspace = "isd_weather_data"
    val cassandraTableRaw = "raw_weather_data"
    val cassandraTableDailyPrecip = "daily_aggregate_precip"

    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
    val rawTwitterStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val parsedTwitterStream: DStream[RawTwitterData] = ingestStream(rawTwitterStream)

    parsedTwitterStream.print
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def ingestStream(rawTwitterStream: InputDStream[(String, String)]): DStream[RawTwitterData] = {
    val parsedWeatherStream = rawTwitterStream.map(_._2.split(","))
      .map(RawTwitterData(_))
    parsedWeatherStream
  }

}
