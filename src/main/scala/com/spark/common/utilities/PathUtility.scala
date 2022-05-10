package com.spark.common.utilities

import org.apache.hadoop.fs.Path

/**
 * This Class is responsible to include methods for transformations performed on HDFS/GCS Path
 */

object PathUtility {

  /**
   * To read multiple string path object
   * <pre>
   * Example:
   * case 1: input paths = "/root", "/inp1", "inp2"
   *         output path = "/root/inp1/inp2"
   *
   * case 2: input paths = "relative", "inp1", "//inp2"
   *         output path = "relative/inp1/inp2"
   *
   * case 3: input paths = "//root", "/inp1", "inp2"
   *         output path = "/root/inp1/inp2"
   *
   * case 4: input paths = "//root", "/inp1", "inp2/
   *         output path = "/root/inp1/inp2"
   *
   *</pre>
   * @param strPaths     multiple string path object
   * @return Path
   */
  def combineStringPath(strPaths: String*): Path = {
      new Path((strPaths.head +
        strPaths.tail.map(x => "/" + x).mkString).replaceAll("[/]+", "/"))
  }

  /**
   * To read multiple path object
   * <pre>
   * Example:
   * case 1: input paths = Path("/root"), Path("/inp1"), Path("inp2")
   *         output path = "/root/inp1/inp2"
   *
   * case 2: input paths = Path("relative"), Path("inp1"), Path("//inp2")
   *         output path = "relative/inp1/inp2"
   *
   * case 3: input paths = Path("//root"), Path("/inp1"), Path("inp2")
   *         output path = "/root/inp1/inp2"
   *
   * case 4: input paths = Path("//root"), Path("/inp1"), Path("inp2/)
   *         output path = "/root/inp1/inp2"
   *</pre>
   * @param paths     multiple path object
   * @return Path
   */
  def combinePath(paths: Path*): Path = {
    new Path((paths.head.toString +
      paths.tail.map(path => path.toString).map(x => "/" + x).mkString).replaceAll("[/]+", "/"))
  }

}