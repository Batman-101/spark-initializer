package com.spark.common.utilities

import com.spark.common.utilities.Debugging
import org.scalatest.flatspec.AnyFlatSpec


class DebuggingTest extends AnyFlatSpec {

  "Debugging.time" should "invoke the passed closure" in {
    var flipToTrue: Boolean = false

    Debugging.time("TestTimer")({
      flipToTrue = true
    })

    assert(flipToTrue, "The inner closure should have been invoked and this should be a true")
  }

    it should "bubble any exception up" in {
        assertThrows[java.lang.Exception](Debugging.time("TestTimer")({throw new Exception("derp")}))
    }

}
