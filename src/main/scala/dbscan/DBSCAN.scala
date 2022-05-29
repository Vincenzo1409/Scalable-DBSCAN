package dbscan
import dbscan.LabeledPoint._
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable.ParArray

object DBSCAN {

  /**
   * Train a DBSCAN Model using the given set of parameters
   *
   * @param data training points stored as `RDD[Vector]`
   * only the first two points of the vector are taken into consideration
   * @param eps the maximum distance between two points for them to be considered as part
   * of the same region
   * @param minPoints the minimum number of points required to form a dense region
   * @param maxPointsPerPartition the largest number of points in a single partition
   */
  def train(
             data: RDD[ParArray[Double]],
             eps: Double,
             minPoints: Int,
             maxPointsPerPartition: Int): DBSCAN = {

    new DBSCAN(eps, minPoints, maxPointsPerPartition, null, null).train(data)

  }

}

class DBSCAN private (
                       val eps: Double,
                       val minPoints: Int,
                       val maxPointsPerPartition: Int,
                       @transient val partitions: List[(Int, Rectangle)],
                       @transient private val labeledPartitionedPoints: RDD[(Int, LabeledPoint)])

  extends Serializable with Logging {

  type Margins = (Rectangle, Rectangle, Rectangle)
  type ClusterId = (Int, Int)

  def minimumRectangleSize: Double = 2 * eps

  def labeledPoints: RDD[LabeledPoint] = {
    labeledPartitionedPoints.values
  }

  def train(data: RDD[ParArray[Double]]): DBSCAN = {
    // generate the smallest rectangles that split the space
    // and count how many points are contained in each one of them
    val minimumRectanglesWithCount =
      data
        .map(toMinimumBoundingRectangle)
        .map((_, 1))
        .aggregateByKey(0)(_ + _, _ + _)
        .collect()
        .toSet

    val localPartitions = Partitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)

    val localMargins =
      localPartitions
        .map({ case (p, _) => (p.shrink(eps), p, p.shrink(-eps)) })
        .zipWithIndex

    val partitionsNum = localPartitions.size

    val margins = data.context.broadcast(localMargins)

  /**
      for {
      x <- xs
      y <- ys
      if cond
    } yield (x, y)

    xs.flatMap(ys.withFilter(y => cond).map(y => (x, y)))
  **/

    val x = data.map(p => Point(p)).flatMap(point => margins.value.filter(y => y._1._3.contains(point)).map(r => (r._2,point))) //it's equivalent to the for-if-yeld below

// assign each point to its proper partition
    val duplicated = for {
      point <- data.map(Point)
      ((inner, main, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)

    //println("PRINT DI DUPLICATED")
    //duplicated.map(a => println("ID: " + a._1 + " -- POINT: " /**+ a._2.vector.mkString("Array(", ", ", ")")*/))
    //duplicated.foreach(a => println("ID: " + a._1 + " -- POINT: " + a._2.vector.mkString("Array(", ", ", ")")))
    //println("FINE PRINT DI DUPLICATED")
    //println("data len: " + data.collect.length)
    //println("duplicated len: " + duplicated.collect.length)


    // perform local dbscan
    val clustered =
      duplicated
        .groupByKey(partitionsNum)
        .flatMapValues(points =>
          new DBSCANCore(eps, minPoints).fit(points))
        .cache()

    //println("PRINT DI CLUSTERED")
    //clustered.foreach(a => println("id: " + a._1 + " -- point: " + a._2.vector.mkString(" - ") + " -- visited: " + a._2.visited + " -- cluster: " + a._2.cluster + " -- flag: " + a._2.flag))
    //println("FINE PRINT DI CLUSTERED")
    //var wait = readLine()

    // find all candidate points for merging clusters and group them
    val mergePoints =
      clustered
        .flatMap({
          case (partition, point) =>
            margins.value
              .filter({
                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
              })
              .map({
                case (_, newPartition) => (newPartition, (partition, point))
              })
        })
        .groupByKey()

    // find all clusters with aliases from merging candidates
    val adjacencies =
      mergePoints
        .flatMapValues(findAdjacencies)
        .values
        .collect()

    //var tst = readLine()
    //mergePoints foreach println
    //mergePoints.foreach(a => println("osservazione: " + a._1 + " --- point: " + a._2.foreach(b => println("point cl: " + b._1 +
      //                                                    " lbl point: " + b._2.vector.mkString(" - ") + " -- flag: " +
        //                                                  b._2.flag + " -- cluster: " + b._2.cluster + " -- visited: " + b._2.visited))))
    //adjacencies.foreach(a => println("a._1._1: " + a._1._1 + " -- a._1._2: " + a._1._2 + " -- a._2._1: " + a._2._1 + " -- a._2._2: " + a._2._2))
    //adjacencies foreach println
    //println()
    val mP = mergePoints
    //println("len: " + mP.collect().length)
    //println("adjacencies len: " + adjacencies.length)

    //var tst2 = readLine()
    // generated adjacency graph
    val adjacencyGraph = adjacencies.foldLeft(Graph[ClusterId]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

    logDebug("About to find all cluster ids")
    // find all cluster ids
    val localClusterIds =
      clustered
        .filter({ case (_, point) => point.flag != Flag.Noise })
        .mapValues(_.cluster)
        .distinct()
        .collect()
        .toList

    // assign a global Id to all clusters, where connected clusters get the same id
    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]())) {
      case ((id, map), clusterId) => {

        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            logDebug(s"Connected clusters $connectedClusters")
            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(x) =>
            (id, map)
        }

      }
    }

    logDebug("Global Clusters")
    clusterIdToGlobalId.foreach(e => logDebug(e.toString))
    logInfo(s"Total Clusters: ${localClusterIds.size}, Unique: $total")

    val clusterIds = data.context.broadcast(clusterIdToGlobalId)

    logDebug("About to relabel inner points")
    // relabel non-duplicated points
    val labeledInner =
      clustered
        .filter(isInnerPoint(_, margins.value))
        .map {
          case (partition, point) => {

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            (partition, point)
          }
        }

    logDebug("About to relabel outer points")
    // de-duplicate and label merge points
    val labeledOuter =
      mergePoints.flatMapValues(partition => {
        partition.foldLeft(Map[Point, LabeledPoint]())({
          case (all, (partition, point)) =>

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) => {
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
              }
            }

        }).values
      })

    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    new DBSCAN(eps,minPoints,maxPointsPerPartition,finalPartitions,labeledInner.union(labeledOuter))
  }

  /**
   * Find the appropriate label to the given `vector`
   *
   * This method is not yet implemented
   */
  def predict(vector: Vector): LabeledPoint = {
    throw new NotImplementedError
  }

  private def isInnerPoint(
                            entry: (Int, LabeledPoint),
                            margins: List[(Margins, Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head

        inner.almostContains(point)
    }
  }

  private def findAdjacencies(
                               partition: Iterable[(Int, LabeledPoint)]): Set[((Int, Int), (Int, Int))] = {

    val zero = (Map[Point, ClusterId](), Set[(ClusterId, ClusterId)]())

    val (seen, adjacencies) = partition.foldLeft(zero)({

      case ((seen, adjacencies), (partition, point)) =>

        // noise points are not relevant for adjacencies
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {

          val clusterId = (partition, point.cluster)

          seen.get(point) match {
            case None                => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }

        }
    })

    adjacencies
  }

  private def toMinimumBoundingRectangle(vector: ParArray[Double]): Rectangle = {
    val point = Point(vector)
    val x = corner(point.x)
    val y = corner(point.y)
    /**println("point: " + point.vector.mkString(" - ") + "\n" +
            "x: " + x + "\n" +
            "y: " + y + "\n")
    println("RECTANGLE: " + "x: " + x + "\n" +
                            "y: " + y + "\n" +
                            "x2: " + (x + minimumRectangleSize) + "\n" +
                            "y2: " + (y + minimumRectangleSize) + "\n")
    var tst = readLine()*/
    Rectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }

  private def corner(p: Double): Double =
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize

  private def shiftIfNegative(p: Double): Double =
    if (p < 0) p - minimumRectangleSize else p

}

