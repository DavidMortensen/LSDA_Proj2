package dk.itu.LSDA.projects.project2

object Timing {

  // Times execution of function
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + " milliseconds")
    result
  }

}
