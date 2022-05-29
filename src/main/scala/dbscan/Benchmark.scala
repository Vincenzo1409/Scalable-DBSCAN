package dbscan


object Benchmark {
  def time[A](blockToBenchmark: => A): (A, Double) = {
    val t0 = System.nanoTime()
    val result = blockToBenchmark
    val t1 = System.nanoTime()
    val timeDiff = t1 - t0
    val timeSec = timeDiff.toDouble / (1000.0 * 1000.0 * 1000.0)
    (result, timeSec)
  }
}
