package dbscan

import dbscan.LabeledPoint._
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class DBSCANCore(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared: Double = eps * eps

  def samplePoint: Array[LabeledPoint] = Array(new LabeledPoint(ParArray(0D, 0D)))

  def fit(points: Iterable[Point]): Iterable[LabeledPoint] = {

    logInfo(s"About to start fitting")

    val labeledPoints = points.map { new LabeledPoint(_) }.toArray

    val totalClusters =
      labeledPoints
        .foldLeft(LabeledPoint.Unknown)(
          (cluster, point) => {
            if (!point.visited) {
              point.visited = true

              val neighbors = findNeighbors(point, labeledPoints)

              if (neighbors.size < minPoints) {
                point.flag = Flag.Noise
                cluster
              } else {
                expandCluster(point, neighbors, labeledPoints, cluster + 1)
                cluster + 1
              }
            } else {
              cluster
            }
          })

    logInfo(s"found: $totalClusters clusters")

    labeledPoints

  }

  private def findNeighbors(
                             point: Point,
                             all: Array[LabeledPoint]): Iterable[LabeledPoint] =
    all.view.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared
    })

  def expandCluster(
                     point: LabeledPoint,
                     neighbors: Iterable[LabeledPoint],
                     all: Array[LabeledPoint],
                     cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    val allNeighbors = mutable.Queue(neighbors)

    while (allNeighbors.nonEmpty) {

      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = findNeighbors(neighbor, all)

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }

          if (neighbor.cluster == LabeledPoint.Unknown) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }

      })

    }

  }

}