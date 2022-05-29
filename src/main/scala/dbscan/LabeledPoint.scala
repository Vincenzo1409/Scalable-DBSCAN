package dbscan

import scala.collection.parallel.mutable.ParArray

object LabeledPoint {
  val Unknown = 0

  object Flag extends Enumeration {
    type Flag = Value
    val Border, Core, Noise, NotFlagged = Value
  }
}

class LabeledPoint(vector: ParArray[Double]) extends Point(vector) {

  def this(point: Point) = this(point.vector)

  var flag = LabeledPoint.Flag.NotFlagged
  var cluster = LabeledPoint.Unknown
  var visited = false

  override def toString(): String = {
    s"$vector,$cluster,$flag"
  }

}