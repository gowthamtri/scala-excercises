package observatory

import scala.math._

import observatory.Visualization._
import observatory.Interaction._
import observatory.Extraction._

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    (d00 * (1.0 - point.x) * (1.0 - point.y)
      + d10 * point.x * (1.0 - point.y)
      + d01 * (1.0 - point.x) * point.y
      + d11 * point.x * point.y).round
  }

  def predictTemperature(tile: Tile, grid: GridLocation => Double): Double = {
    val loc = tileLocation(tile)
    val x0 = ceil(loc.lat).toInt
    val x1 = floor(loc.lat).toInt
    val y0 = ceil(loc.lon).toInt
    val y1 = floor(loc.lon).toInt

    bilinearInterpolation(CellPoint(loc.lat, loc.lon),
      grid(GridLocation(x0, y0)),
      grid(GridLocation(x0, y1)),
      grid(GridLocation(x1, y0)),
      grid(GridLocation(x1, y1)))
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    val c = for {
      j <- 0 until 256
      i <- 0 until 256
    } yield interpolateColor(colors, predictTemperature(tile, grid))
    val pixels = c.par.map(c => Pixel(c.red, c.green, c.blue, 127))
    Image(256, 256, pixels.toArray)
  }

}
