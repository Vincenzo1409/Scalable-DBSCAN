import dbscan.{Benchmark, DBSCAN}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.parallel.CollectionConverters._


object Main extends Serializable{

    def main(args: Array[String]): Unit = {

        val dataset_path = args(0)
        val output_path = args(1)
        val n_threads = args(2)
        val epsilon = args(3).toDouble

        val conf = new SparkConf().setAppName("DBScan").setMaster("local[" + n_threads + "]")
        val sc = new SparkContext(conf)

        var input = sc.textFile(dataset_path) // file PATH must be passed by parameter

        val header = input.first()
        input = input.filter(row => row != header)

        val data = input.map(x => x
          .split(',')
          .map(_.toDouble)
          .par
        )

        val eps = epsilon
        val minPoints = 4
        val maxPointsPerPartition = data.count()
//        val dbscanModelTest = DBSCAN.train(data,eps, minPoints, maxPointsPerPartition.toInt)
        val dbscanModelWithBenchmark = Benchmark time DBSCAN.train(data,eps, minPoints, maxPointsPerPartition.toInt)
        val dbscanModel = dbscanModelWithBenchmark._1
        val executionTime = dbscanModelWithBenchmark._2
        println("execution time: " + executionTime)

        val testOutput =  dbscanModel.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}")
        val o = testOutput.collect

        sc.parallelize(o).coalesce(1).saveAsTextFile(output_path + eps + "_" + minPoints + "_" + maxPointsPerPartition + ")")

    }
}
