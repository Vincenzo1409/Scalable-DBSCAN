package dbscan

object Benchmark {
  /**
   * Evaluate DBSCAN computational time
   * Call by name implementation of benchmark evaluator
   *
   * Input:
   *  @param blockToBenchmark which is instance of DBSCAN object
   *
   * Output: pair (result, timeSec):
   *  - result: output of Scalable-DBSCAN algorithm
   *  - timeSec: algorithm execution time in seconds
   */
  def time[A](blockToBenchmark: => A): (A, Double) = {
    val t0 = System.nanoTime()
    val result = blockToBenchmark
    val t1 = System.nanoTime()
    val timeDiff = t1 - t0
    val timeSec = timeDiff.toDouble / (1000.0 * 1000.0 * 1000.0)
    (result, timeSec)
  }
}




