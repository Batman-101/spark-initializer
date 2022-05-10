package com.spark.common.utilities

object Debugging {
  def time[R](name: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(s"${name} - elapsed time: " + (t1 - t0) + "ns, " + (t1 - t0) / 1000000 + "ms")
    result
  }
}
