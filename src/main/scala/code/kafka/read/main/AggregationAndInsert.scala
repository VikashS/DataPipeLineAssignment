package code.kafka.read.main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

object AggregationAndInsert {

  def createTwitterView(sparkContext: SparkContext, tweets: DStream[String]) = {
    createViewForTwitterCount(sparkContext, tweets)
  }

  def createViewForTwitterCount(sparkContext: SparkContext, tweets: DStream[String]) = {
    tweets.foreachRDD { (rdd: RDD[String], time: Time) =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val tweets: DataFrame = spark.sqlContext.read.json(rdd)
      tweets.createOrReplaceTempView("mytable")
      val wordCountsDataFrame: DataFrame = spark.sql("SELECT id,created_at,favorite_count from mytable Where retweet_count >= 0 ")
     // val res: DataFrame = wordCountsDataFrame.withColumnRenamed("id","userid").withColumnRenamed("created_at","createdat").withColumnRenamed("favorite_count","friendscount")
      wordCountsDataFrame.write.mode(SaveMode.Overwrite)
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "twitterdata", "keyspace" -> "vkspace"))
        .save()
      wordCountsDataFrame.show(false)

    }
  }

}
