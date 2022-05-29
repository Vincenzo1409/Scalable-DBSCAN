import dbscan.DBSCAN
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import java.io.Serializable


object Main extends Serializable{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DBScan").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("C:/Users/valla/Downloads/Telegram Desktop/labeled_data.csv") // file PATH must be passed by parameter

    //get and removing header row
    //    val header = input.first()
    //    input = input.filter(row => row != header)

    val data = input.map(x => x
      .split(',')
      .map(_.toDouble)
    )

    //    val data = input.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
    data.foreach(a => println(a.mkString(" - ")))
    println("\n\n\n")

    val dbscanModelTest = DBSCAN.train(data,0.25, 5, 250)
    println("dbscanModelTest.labeledPoints: \n\n")
    val testOutput =  dbscanModelTest.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}")

    val out = testOutput.collect()
    out.foreach(println)
    println(out.length)
    //testOutput.saveAsTextFile("C:/Users/Vincenzo/Downloads/outputTest.txt")

  }
}