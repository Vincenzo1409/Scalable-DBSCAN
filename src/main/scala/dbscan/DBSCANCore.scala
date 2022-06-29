package dbscan

import dbscan.LabeledPoint._
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray


class DBSCANCore(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared: Double = eps * eps

  def samplePoint: Array[LabeledPoint] = Array(new LabeledPoint(ParArray(0D, 0D)))

  def fit(points: Iterable[Point]): Iterable[LabeledPoint] = {

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

    labeledPoints.toList

  }

  // find neighbours using euclidean distance
  private def findNeighbors(
                             point: Point,
                             all: ParArray[LabeledPoint]): ParArray[LabeledPoint] = {
    all.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared
    })
  }

  //expand partition to then get intersections
  def expandCluster(
                     point: LabeledPoint,
                     neighbors: ParArray[LabeledPoint],
                     all: ParArray[LabeledPoint],
                     cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    val allNeighbors = mutable.Queue(neighbors)
    val allNeighPar = allNeighbors.map(x => x.toArray.par)

    while (allNeighPar.nonEmpty) {

      allNeighPar.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = findNeighbors(neighbor, all)
          val newNeigh = neighborNeighbors.toArray.par

          if (newNeigh.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(newNeigh)
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