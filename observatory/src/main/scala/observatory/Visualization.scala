package observatory

import java.nio.file.Paths
import java.time.LocalDate

import scala.io.Source

import scala.math._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val sparkSession: SparkSession = SparkSession.builder().appName("Observatory").master("local").getOrCreate()

  def greatCircleDistance(location: Location, another: Location): Double =
    6371 * acos(
      sin(toRadians(location.lat)) * sin(toRadians(another.lat)) +
        cos(toRadians(location.lat)) * cos(toRadians(another.lat)) *
          cos(abs(toRadians(location.lon) - toRadians(another.lon))))

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val greatCircleDistances = temperatures.map(t => (greatCircleDistance(location, t._1), t._2))

    def dConv(distance: Double) = 1 / pow(distance, 3)

    greatCircleDistances.find(_._1 < 1.0) match {
      case Some(x) => x._2
      case None => {
        val acc = greatCircleDistances.foldLeft((0.0, 0.0))((acc, curr) =>
          (acc._1 + dConv(curr._1) * curr._2, acc._2 + dConv(curr._1)))
        acc._1 / acc._2
      }
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    def interpolate(p0: (Double, Int), p1: (Double, Int)) =
      (p0._2 + (value - p0._1) * (p1._2 - p0._2) / (p1._1 - p0._1)).round.toInt
    val res: List[(Double, Color)] = ((value, Color(-1, -1, -1)) :: points.toList).sortWith(_._1 < _._1)

    val index = res.indexWhere(t => t._2.red == -1 && t._2.blue == -1 && t._2.blue == -1)

    index match {
      case 0 => res(1)._2
      case last if last == res.size - 1 => res(last - 1)._2
      case i => {
        val upper = res(i + 1)
        val lower = res(i - 1)

        Color(
          interpolate((lower._1, lower._2.red),   (upper._1, upper._2.red)),
          interpolate((lower._1, lower._2.green), (upper._1, upper._2.green)),
          interpolate((lower._1, lower._2.blue),  (upper._1, upper._2.blue)))
      }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val locations = for (y <- 90 to -89 by -1;x <- -180 to 179) yield Location(y, x)

    val pixels = locations.par
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, 255))

    Image(360, 180, pixels.toArray)
  }

}

