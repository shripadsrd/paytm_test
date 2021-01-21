package scala

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

case class WeatherData
(
  stationNumber: Long,
  wban: Long,
  date: String,
  temperature: Double,
  dewPoint: Double,
  slp: Double,
  stp: Double,
  visibility: Double,
  wdsp: Double,
  mxspd: Double,
  gust: Double,
  maxTemp: Double,
  minTemp: Double,
  precipitation: Double,
  snowDepth: Double,
  frshttIndicators: String
)

case class Country
(
  countryAbbrevation: String,
  fullName: String
)
case class Station
(
  stationNumber: String,
  countryAbbrevation: String
)

object DataEngineerChallenge extends App{

  private val MASTER = "local"
  private val APP_NAME = "paytm"
  private val WEATHER_DATA_PATH = "./src/main/resources/data/2019/*.csv"
  private val COUNTRY_DATA_PATH = "./src/main/resources/data/countrylist.csv"
  private val STATION_DATA_PATH = "./src/main/resources/data/stationlist.csv"
  private val COUNTRY_ABBR = "countryAbbrevation"
  private val STATION_NUMBER = "stationNumber"
  private val FULLNAME = "fullName"
  private val TEMP = "temperature"
  private val AVG_TEMP = "averageTemperature"
  private val AVG_WINDSPEED = "averageWindspeed"
  private val DATE = "date"
  private val RANK = "rank"
  private val WDSP = "wdsp"
  private val FRSHTT = "frshttIndicators"

  val spark = SparkSession.builder
    .appName(APP_NAME).master(MASTER).getOrCreate
  import spark.implicits._

  val weatherSchema = Encoders.product[WeatherData].schema
  val countrySchema = Encoders.product[Country].schema
  val stationSchema = Encoders.product[Station].schema

  val weatherData = spark.read.schema(weatherSchema).csv(WEATHER_DATA_PATH).as[WeatherData]
  val countriesData = spark.read.schema(countrySchema).csv(COUNTRY_DATA_PATH).as[Country]
  val stationsData = spark.read.schema(stationSchema).csv(STATION_DATA_PATH).as[Station]

  val weatherForStations = weatherData.join(broadcast(stationsData
    .join(countriesData, COUNTRY_ABBR)), STATION_NUMBER).cache

  val weatherSortedByTemp = weatherForStations.groupBy(FULLNAME).agg(avg(TEMP).as(AVG_TEMP))
    .sort(col(AVG_TEMP).desc)
  val topTemp = weatherSortedByTemp.select(FULLNAME, AVG_TEMP)

  val countryWithMaxTemp = topTemp.first().getAs[String](FULLNAME)
  val maxTemp = topTemp.first().getAs[Double](AVG_TEMP)

  val window = Window.partitionBy(STATION_NUMBER).orderBy(DATE)
  val topTornadoCountry = weatherForStations.filter(col(FRSHTT).substr(6, 6) === 1)
    .withColumn(RANK, rank over window).groupBy(FULLNAME, DATE, RANK).agg(sum(RANK).as(RANK))
    .sort(col(RANK).desc).first().getAs[String](FULLNAME)

  val weatherSortedByWind = weatherForStations.groupBy(FULLNAME).agg(avg(WDSP).as(AVG_WINDSPEED))
    .sort(col(AVG_WINDSPEED).desc)
  val topWind = weatherSortedByWind.select(FULLNAME, AVG_WINDSPEED)

  val countryWithSecondHighestWind = topTemp.first().getAs[String](FULLNAME)
  val top2Wind = topWind.takeAsList(2).get(1)

  println("The country with the highest mean temperature over the year is: " + countryWithMaxTemp
    + " with the average temperature of: " + maxTemp)
  println("The country with the most consecutive days of tornadoes/funnel cloud\nformations is: " + topTornadoCountry)
  println("The country with the second highest average mean wind speed over the year is: "
    + top2Wind.getString(0) + " with the average temperature of: " + top2Wind.getAs[Double](1))


}
