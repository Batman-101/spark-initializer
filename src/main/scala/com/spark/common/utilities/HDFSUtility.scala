package com.spark.common.utilities

import com.spark.common.constant.ConfigConstants.EMPTY_STRING
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtility {

  /**
   * To read the latest available path having 'numerical digit' as 'org.apache.hadoop.fs.Path' name,
   *
   * @throws NumberFormatException if path is not a numerical digit.
   * @param path     numeric Path should be within the Int range
   * @param hdConfig Configuration
   * @return Path
   */
  @throws(classOf[NumberFormatException])
  def findLatestPath(path: Path, hdConfig: Configuration): Path = {

    val fs = FileSystem.get(hdConfig)
    val pathMap = fs.listStatus(path)
      .filter(_.isDirectory)
      .map(_.getPath)
      .flatMap((fileInfo: Path) => {
        Map[Int, Path](
          fileInfo.getName.replaceAll("/|:", EMPTY_STRING).toInt
            ->
            fileInfo
        )
      }).toMap

    // Get file path for latest folder
    pathMap(pathMap.keySet.max)
  }

  /**
   * To read the latest partition path having 'numerical digit' as 'org.apache.hadoop.fs.Path' name,
   *
   * @throws NumberFormatException if path is not a numerical digit.
   * @param basePath numeric Path should be within the Long range
   * @param hdConfig Configuration
   * @tparam T PathImplicits Partition Value Type (restricted to Int, Double, Float and Long)
   * @return Path
   */
  @throws(classOf[NumberFormatException])
  def findLatestPartitionPath[T](basePath: Path, hdConfig: Configuration)
                                (implicit pathImplicit: PathImplicits[T]): Path = {
    val paths: Array[Path] = getPaths(basePath, hdConfig)
    val filteredPaths = paths.filter(eachPath => eachPath.toString.contains("="))
    pathImplicit.findLatestPartitionPath(filteredPaths)
  }

  /** Deletes the,
   *  - specified path if path is empty and recursive flag is false,
   *  - specified path and its content if recursive flag is true.
   *
   * It return true if operation is successful and vice-versa.
   *
   * @param path       Path
   * @param recursive  Boolean true if want to delete recursively
   * @param hadoopConf Hadoop Configuration
   * @return Boolean
   */
  def deleteHdfsPath(path: Path, recursive: Boolean, hadoopConf: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConf)
    fs.exists(path) && fs.delete(path, recursive)
  }

  /**
   * To get unique Paths having partition value greater than given partitionValue
   *
   * @param basePath       numeric base path
   * @param hdConfig       Hadoop Configuration
   * @param partitionValue partition value
   * @param pathImplicit   path type implicit
   * @tparam T Partition Value Data Type which is restricted to Int, Float, Long and Double
   * @return Returns unique Paths having partition value greater than given partitionValue
   */
  def getPathsGreaterThanPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                          (implicit pathImplicit: PathImplicits[T]): Set[Path] = {
    pathImplicit.getPathMapGreaterThanPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * To get unique Paths having partition value less than given partitionValue
   *
   * @param basePath       numeric base path
   * @param hdConfig       Hadoop Configuration
   * @param partitionValue partition value
   * @param pathImplicit   path type implicit
   * @tparam T Partition Value Data Type which is restricted to Int, Float, Long and Double
   * @return Returns unique Paths having partition value less than given partitionValue
   */
  def getPathsLessThanPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                       (implicit pathImplicit: PathImplicits[T]): Set[Path] = {
    pathImplicit.getPathMapLessThanPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * To get unique Paths having partition value less than equal to given partitionValue
   *
   * @param basePath       numeric base path
   * @param hdConfig       Hadoop Configuration
   * @param partitionValue partition value
   * @param pathImplicit   path type implicit
   * @tparam T Partition Value Data Type which is restricted to Int, Float, Long and Double
   * @return Returns unique Paths having partition value less than equal to given partitionValue
   */
  def getPathsLessThanEqualPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                            (implicit pathImplicit: PathImplicits[T]): Set[Path] = {
    pathImplicit.getPathMapLessThanEqualPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Get set of Partition Paths greater than or equal to the given partitionValue in the basePath.
   *
   * @param basePath       Base Path to search for Partitions in
   * @param hdConfig       Hadoop Configuration
   * @param partitionValue Partition Value
   * @param pathImplicit   Implicit path Object
   * @tparam T Partition Value Type (restricted to Int, Double, Float and Long)
   * @return Returns set of partition paths greater than or equal to the given partitionValue
   */
  def getPathsGreaterThanEqualPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                               (implicit pathImplicit: PathImplicits[T]): Set[Path] = {
    pathImplicit.getPathMapGreaterThanEqualPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Get set of Partition Paths in between (inclusive of the boundary values) the given partitionValues in the basePath.
   *
   * @throws IllegalArgumentException when partitionValueStart > partitionValueEnd
   * @param basePath            Base Path to search for Partitions in
   * @param hdConfig            Hadoop Configuration
   * @param partitionValueStart Partition Value Lower Boundary
   * @param partitionValueEnd   Partition Value Upper Boundary
   * @param pathImplicit        Implicit path Object
   * @tparam T Partition Value Type (restricted to Int, Double, Float and Long)
   * @return Returns set of partition paths in between the given range
   */
  def getPathsBetweenPartitionValues[T](basePath: Path, hdConfig: Configuration, partitionValueStart: T,
                                        partitionValueEnd: T)(implicit pathImplicit: PathImplicits[T]): Set[Path] = {
    pathImplicit.getPathMapBetweenPartitionValues(getPaths(basePath, hdConfig), partitionValueStart, partitionValueEnd)
      .values.toSet
  }

  /**
   * To fetch the paths for a given base path
   *
   * @param basePath Base Path
   * @param hdConfig hadoop configuration
   * @return
   */
  private def getPaths(basePath: Path, hdConfig: Configuration): Array[Path] = {
    val fs = FileSystem.get(hdConfig)
    val paths: Array[Path] = fs.listStatus(basePath)
      .filter(_.isDirectory)
      .map(_.getPath)
    paths
  }
}
