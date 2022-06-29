package dbscan

import scala.annotation.tailrec
import org.apache.spark.internal.Logging


/**
 * Partitioner object which is used to create rectangle boxes
 */
object Partitioner {

  def partition(
                 toSplit: Set[(Rectangle, Int)],
                 maxPointsPerPartition: Long,
                 minimumRectangleSize: Double): List[(Rectangle, Int)] = {
    new Partitioner(maxPointsPerPartition, minimumRectangleSize)
      .findPartitions(toSplit)
  }

}

class Partitioner(
                   maxPointsPerPartition: Long,
                   minimumRectangleSize: Double) extends Logging {

  type RectangleWithCount = (Rectangle, Int)

  def findPartitions(toSplit: Set[RectangleWithCount]): List[RectangleWithCount] = {

    val boundingRectangle = findBoundingRectangle(toSplit) //main rectangle which contains partitions rectangles

    def pointsIn = pointsInRectangle(toSplit, _: Rectangle)

    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

    val partitions = partition(toPartition, partitioned, pointsIn)

    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })
  }

  @tailrec
  private def partition(
                         remaining: List[RectangleWithCount],
                         partitioned: List[RectangleWithCount],
                         pointsIn: (Rectangle) => Int): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>  // (rectangle, count) it's the head of the list (first element), rest it's the tail (it's a list too)
        if (count > maxPointsPerPartition) {
          if (canBeSplit(rectangle)) {
            def cost = (r: Rectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
            val (split1, split2) = split(rectangle, cost)
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)

          } else {
            partition(rest, (rectangle, count) :: partitioned, pointsIn)
          }

        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }

      case Nil => partitioned

    }

  }

  def split(
             rectangle: Rectangle,
             cost: (Rectangle) => Int): (Rectangle, Rectangle) = {

    val smallestSplit =
      findPossibleSplits(rectangle)
        .reduceLeft {
          (smallest, current) =>

            if (cost(current) < cost(smallest)) {
              current
            } else {
              smallest
            }

        }

    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
   * Returns the box which covers the space inside boundary that is not covered by the box
   */
  private def complement(box: Rectangle, boundary: Rectangle): Rectangle =
    if (box.x == boundary.x && box.y == boundary.y) {
      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) {
        if (box.y2 == boundary.y2) {
          Rectangle(box.x2, box.y, boundary.x2, boundary.y2)
        } else if (box.x2 == boundary.x2) {
          Rectangle(box.x, box.y2, boundary.x2, boundary.y2)
        } else {
          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("rectangle is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }

  /**
   * Returns all the possible ways in which the given box can be split
   */
  private def findPossibleSplits(box: Rectangle): Set[Rectangle] = {

    val xSplits = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize

    val ySplits = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize

    val splits =
      xSplits.map(x => Rectangle(box.x, box.y, x.toDouble, box.y2)) ++
        ySplits.map(y => Rectangle(box.x, box.y, box.x2, y.toDouble))

    splits.toSet
  }

  /**
   * Returns true if the given rectangle can be split into at least two rectangles of minimum size
   */
  private def canBeSplit(box: Rectangle): Boolean = {
    (box.x2 - box.x > minimumRectangleSize * 2 ||
      box.y2 - box.y > minimumRectangleSize * 2)
  }

  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: Rectangle): Int = {
    space.view
      .filter({ case (current, _) => rectangle.contains(current) })
      .foldLeft(0) {
        case (total, (_, count)) => total + count
      }
  }

  def findBoundingRectangle(rectanglesWithCount: Set[RectangleWithCount]): Rectangle = {

    val invertedRectangle =
      Rectangle(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)

    rectanglesWithCount.foldLeft(invertedRectangle) {
      case (bounding, (c, _)) =>
        Rectangle(
          bounding.x.min(c.x), bounding.y.min(c.y),
          bounding.x2.max(c.x2), bounding.y2.max(c.y2))
    }
  }
}