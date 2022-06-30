package mKNN

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.collection.parallel.ParSeq


object KNN {


  val spark: SparkSession = SparkSession.builder.appName("Knn").config("spark.master", "local[*]").getOrCreate()
  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {

    val dataset_path = args(0)
    val df = readCsv(fileName = dataset_path, header = false).distinct()

    val colNames = df.columns
    var data = df

    for (colName<-colNames){
      data=data.withColumn(colName,col(colName).cast("Double"))
    }

    val all_df = data.collect().toList.par
    val listToReturn = for ((row,index) <- all_df.zipWithIndex) yield getOnlyDistances(all_df.filter(r => r!=row), row)

    val lToReturn = listToReturn.toList.filter(p => p != 0.0).sortBy(x => x)
    val elbowList = lToReturn.zipWithIndex


    elbowList foreach println
    val eList = elbowList.map(el => (el._2,new java.math.BigDecimal(el._1).toPlainString.toDouble))
    eList foreach println

    val first = eList.head
    val last = eList.last

    val line_sl = (last._1 - first._1) / (last._2 - first._2)
    println("line_sl: " + line_sl)

    val par_list = eList.par
    val ret_parlist = (for ((index, row) <- par_list) yield {
      getPointDistance(line_sl, index, row)
    })
    val max_dist = ret_parlist.maxBy(_._1)._2
    println("max_dist: "+ max_dist)

  }

  def readCsv(fileName: String, header: Boolean): DataFrame = {
    spark.read
      .format("csv")
      .option("header", header)
      .option("mode", "DROPMALFORMED")
      .load(fileName)
  }

  def computeEuclideanDistance(row1: Row, row2: Row): Double = {
    var distance = 0.0
    for (i <- 0 until row1.length - 1) {
      distance += math.pow(row1.getDouble(i) - row2.getDouble(i), 2)
    }
    math.sqrt(distance)
  }

  def getOnlyDistances(trainSet: ParSeq[Row], testRow: Row): Double = {
    val distances = (for (trainRow <- trainSet if !trainRow.anyNull) yield (trainRow, computeEuclideanDistance(trainRow, testRow))).minBy(_._2)
    distances._2
  }

  def getPointDistance(line_sl: Double, y: Int, x: Double): (Double, Double) = {
    val dist_from_line_sl = (y - (line_sl * x).abs) / scala.math.sqrt(1 + scala.math.pow(line_sl, 2))
    (dist_from_line_sl, x)
  }
}