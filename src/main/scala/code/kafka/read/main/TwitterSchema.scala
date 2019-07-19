package code.kafka.read.main

object TwitterSchema {

  sealed trait TwitterSchema extends Serializable

  case class TwitterData(
                          id: String,
                          name: String,
                          countryCode: String,
                          callSign: String,
                          lat: Double,
                          long: Double,
                          elevation: Double) extends TwitterSchema

  case class RawTwitterData(
                             wsid: String,
                             count: Int,
                             topic: String) extends TwitterSchema

  object RawTwitterData {
    def apply(array: Array[String]): RawTwitterData = {
      RawTwitterData(
        wsid = array(0),
        count = array(1).toInt,
        topic = array(2)
      )
    }
  }

  trait TwitterAggregate extends TwitterSchema with Serializable {
    def wsid: String
  }

}
