package dbscan

import dbscan.LabeledPoint._
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters.ArrayIsParallelizable
import scala.collection.parallel.mutable.ParArray
/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */

// TODO: fix out of bound and out of memory errors
class DBSCANCore(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared: Double = eps * eps

  def samplePoint: ParArray[LabeledPoint] = ParArray(new LabeledPoint(ParArray(0D, 0D)))

  def fit(points: Iterable[Point]): ParArray[LabeledPoint] = {

    logInfo(s"About to start fitting")

    val labeledPoints = points.map { new LabeledPoint(_) }.toArray.par

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
                             all: ParArray[LabeledPoint]): ParArray[LabeledPoint] =
    all.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared
    })

  def expandCluster(
                     point: LabeledPoint,
                     neighbors: ParArray[LabeledPoint],
                     all: ParArray[LabeledPoint],
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