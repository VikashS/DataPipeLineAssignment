package code.kafka.read.main
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object Regex extends App
{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "StringManupRdd")
  val orderRDD=sc.textFile("F://dataFrom//data-master//retail_db//orders")
  orderRDD.take(1).foreach(println)
  //val orderMap=orderRDD.map(vik=>vik.split(",")(1).substring(0,10).replace("-","").toInt)
  val orderMapPair=orderRDD.map(vik=>{
    val splitvalPairRDD = vik.split(",")
    (splitvalPairRDD(0).toInt,splitvalPairRDD(1).substring(0,10).replace("-","").toInt)
  })

  orderMapPair.take(10).foreach(println)


  val orderItemRDD=sc.textFile("F://dataFrom//data-master//retail_db//order_items")
  orderItemRDD.take(1).foreach(println)

  val orderItemRDDMap=orderItemRDD.map(vik2=>{
    val splitVal=vik2.split(",")
    (splitVal(1).toInt,vik2)
  })
  orderItemRDDMap.take(5).foreach(println)
}