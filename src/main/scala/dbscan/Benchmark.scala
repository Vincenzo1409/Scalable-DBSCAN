package dbscan

object Benchmark {
  def time[A](blockToBenchmark: => A): (A,Double) = {
    val t0 = System.nanoTime()
    val result = blockToBenchmark
    val t1 = System.nanoTime()
    val timeInSeconds = (t1 - t0) / 1e9
    (result,timeInSeconds)
  }
}
