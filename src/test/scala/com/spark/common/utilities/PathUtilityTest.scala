package com.spark.common.utilities

import com.spark.common.utilities.PathUtility
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.apache.hadoop.fs.Path

class PathUtilityTest extends AnyFlatSpec with BeforeAndAfterAll {
  "PathUtilityTest" should "test combineStringPath method" in {
    val path1 =  PathUtility.combineStringPath("/root", "/inp1", "inp2")
    val path2 =  PathUtility.combineStringPath("relative", "inp1", "//inp2")
    val path3 =  PathUtility.combineStringPath("//root", "/inp1", "inp2")
    val path4 =  PathUtility.combineStringPath("//root", "/inp1", "inp2/")
    val path5 =  PathUtility.combineStringPath("//root", "/inp1", "inp2//")



    path1 should be (new Path("/root/inp1/inp2"))
    path2 should be (new Path("relative/inp1/inp2"))
    path3 should be (new Path("/root/inp1/inp2"))
    path4 should be (new Path("/root/inp1/inp2"))
    path5 should be (new Path("/root/inp1/inp2"))
  }

  "PathUtilityTest" should "test combinePath method" in {

    val path1 =  PathUtility.combinePath(new Path("/root"), new Path("/inp1"),
      new Path("inp2"))
    val path2 =  PathUtility.combinePath(new Path("relative"), new Path( "inp1"),
      new Path("//inp2"))
    val path3 =  PathUtility.combinePath(new Path("//root"), new Path("/inp1"),
      new Path("inp2"))
    val path4 =  PathUtility.combinePath(new Path("//root"), new Path("/inp1"),
      new Path("inp2/"))
    val path5 =  PathUtility.combinePath(new Path("//root"), new Path("/inp1"),
      new Path("inp2//"))


    path1 should be (new Path("/root/inp1/inp2"))
    path2 should be (new Path("relative/inp1/inp2"))
    path3 should be (new Path("/root/inp1/inp2"))
    path4 should be (new Path("/root/inp1/inp2"))
    path5 should be (new Path("/root/inp1/inp2"))

  }

}
