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
                             year: Int,
                             month: Int,
                             day: Int,
                             hour: Int,
                             temperature: Double,
                             dewpoint: Double,
                             pressure: Double,
                             windDirection: Int,
                             windSpeed: Double,
                             skyCondition: Int,
                             skyConditionText: String,
                             oneHourPrecip: Double,
                             sixHourPrecip: Double) extends TwitterSchema


  object RawTwitterData {
    def apply(array: Array[String]): RawTwitterData = {
      RawTwitterData(
        wsid = array(0),
        year = array(1).toInt,
        month = array(2).toInt,
        day = array(3).toInt,
        hour = array(4).toInt,
        temperature = array(5).toDouble,
        dewpoint = array(6).toDouble,
        pressure = array(7).toDouble,
        windDirection = array(8).toInt,
        windSpeed = array(9).toDouble,
        skyCondition = array(10).toInt,
        skyConditionText = array(11),
        oneHourPrecip = array(11).toDouble,
        sixHourPrecip = Option(array(12).toDouble).getOrElse(0))
    }
  }

  trait TwitterAggregate extends TwitterSchema with Serializable {
    def wsid: String
  }

  trait Precipitation extends TwitterAggregate

  case class DailyAnalysis(wsid: String,
                                year: Int,
                                month: Int,
                                day: Int,
                                precipitation: Double) extends Precipitation


}
