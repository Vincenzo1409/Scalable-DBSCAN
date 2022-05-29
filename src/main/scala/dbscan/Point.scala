package dbscan

import scala.collection.parallel.mutable.ParArray

case class Point(val vector: ParArray[Double]){
  def x = vector(0)
  def y = vector(1)

  def distanceSquared(other: Point): Double = {  // take euclidean distance implemented before
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }
}
