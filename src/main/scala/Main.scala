import dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.parallel.CollectionConverters._


object Main extends Serializable{

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DBScan").setMaster("local[*]")
        val sc = new SparkContext(conf)

        var input = sc.textFile("C:/Users/Vincenzo/Downloads/households_PCA_50k.csv") // file PATH must be passed by parameter

        val header = input.first()
        input = input.filter(row => row != header)

        val data = input.map(x => x
          .split(',')
          .map(_.toDouble)
          .par
        )

        val eps = 0.5241708601655404
        val minPoints = 4
        val maxPointsPerPartition = data.count()
        val dbscanModelTest = DBSCAN.train(data,eps, minPoints, maxPointsPerPartition.toInt)
        println("dbscanModelTest.labeledPoints: \n\n")
        val testOutput =  dbscanModelTest.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}")
        val o = testOutput.collect

        sc.parallelize(o).coalesce(1).saveAsTextFile("file:///C:/Users/Vincenzo/Downloads/TestOutput/households_PCA_50k_clustered_(" + eps + "_" + minPoints + "_" + maxPointsPerPartition + ")")

    }
}
