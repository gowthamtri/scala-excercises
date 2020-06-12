package observatory

import java.nio.file.Paths
import java.time.LocalDate

import scala.io.Source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  val sparkSession: SparkSession = SparkSession.builder().appName("Observatory").master("local").getOrCreate()

  def readFile(path: String): RDD[String] = {
    //sparkSession.sparkContext.parallelize(Source.fromResource(path).getLines().toSeq)
    val fileStream = Source.getClass.getResourceAsStream(path)
    sparkSession.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toSeq)
  }

  def toCelsius(fahrenheit: Double): Double = (fahrenheit - 32) * 5 / 9

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stations = readFile(stationsFile)
      .map(_.split(","))
      .filter(l => l.size >= 4 && !l(2).isEmpty && !l(3).isEmpty)
      .map(l => ((l(0), l(1)), Location(l(2).toDouble, l(3).toDouble))).cache()

    val temperatures = readFile(temperaturesFile)
      .map(_.split(","))
      .filter(_.size >= 5)
      .map(l => ((l(0), l(1)), (LocalDate.of(year, l(2).toInt, l(3).toInt), if (l(4) == "9999.9") 0 else l(4).toDouble))).cache()

    stations.join(temperatures).map({
      case (_, (loc: Location, (date: LocalDate, temp))) => (date, loc, toCelsius(temp))
    }).collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    sparkSession.sparkContext.parallelize(records.toList)
      .map({
        case (_, l, t) => (l, (t, 1))
      })
      .reduceByKey((k, v) => (k._1 + v._1, k._2 + v._2))
      .mapValues({
        case (temp, count) => (temp / count * 10).round / 10.0
      })
      .collect()
  }
}
