package com.spark.common.utilities

import org.apache.hadoop.fs.{InvalidPathException, Path}

/**
 * To restricts any method with defined types
 *
 * type T can be defined in [[PathImplicits]] as implicits
 *
 * @tparam T Type is restricted to Int, Double, Float and Long
 */

sealed trait PathImplicits[T] {
  def getPartitionValuePathMap(paths: Array[Path]): Map[T, Path]

  def findLatestPartitionPath(paths: Array[Path]): Path

  def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: T, partitionValueEnd: T): Map[T, Path]
}


object PathImplicits {

  implicit object PathInInt extends PathImplicits[Int] {

    override def getPartitionValuePathMap(paths: Array[Path]): Map[Int, Path] = {
      paths.flatMap { (path: Path) => {
        Map[Int, Path](extractPartitionValue(path).toInt -> path)
      }
      }.toMap
    }

    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      val pathMap = getPartitionValuePathMap(paths)
      pathMap(pathMap.keySet.max)
    }

    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 < partitionValue }
    }

    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 > partitionValue }
    }

    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      getPartitionValuePathMap(paths).filter(partitionValueMap => partitionValueMap._1 <= partitionValue)
    }

    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      getPartitionValuePathMap(paths).filter(partitionValueMap => partitionValueMap._1 >= partitionValue)
    }

    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Int, partitionValueEnd: Int):
    Map[Int, Path] = {
      require(partitionValueStart <= partitionValueEnd)
      getPartitionValuePathMap(paths).filter { partitionValueMap =>
        partitionValueMap._1 >= partitionValueStart && partitionValueMap._1 <= partitionValueEnd }
    }
  }

  implicit object PathInDouble extends PathImplicits[Double] {

    override def getPartitionValuePathMap(paths: Array[Path]): Map[Double, Path] = {
      paths.flatMap { (path: Path) => {
        Map[Double, Path](extractPartitionValue(path).toDouble -> path)
      }
      }.toMap
    }

    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      val pathMap = getPartitionValuePathMap(paths)
      pathMap(pathMap.keySet.max)
    }

    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 < partitionValue }
    }

    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 > partitionValue }
    }

    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Double)
    : Map[Double, Path] = {
      getPartitionValuePathMap(paths).filter(partitionValueMap => partitionValueMap._1 <= partitionValue)
    }

    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Double):
    Map[Double, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 >= partitionValue }
    }

    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Double,
                                                  partitionValueEnd: Double): Map[Double, Path] = {
      require(partitionValueStart <= partitionValueEnd)
      getPartitionValuePathMap(paths).filter { partitionValueMap =>
        partitionValueMap._1 >= partitionValueStart && partitionValueMap._1 <= partitionValueEnd }
    }
  }

  implicit object PathInFloat extends PathImplicits[Float] {

    override def getPartitionValuePathMap(paths: Array[Path]): Map[Float, Path] = {
      paths.flatMap { (path: Path) => {
        Map[Float, Path](extractPartitionValue(path).toFloat -> path)
      }
      }.toMap
    }

    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      val pathMap = getPartitionValuePathMap(paths)
      pathMap(pathMap.keySet.max)
    }

    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 < partitionValue }
    }

    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 > partitionValue }
    }

    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Float)
    : Map[Float, Path] = {
      getPartitionValuePathMap(paths).filter(partitionValueMap => partitionValueMap._1 <= partitionValue)
    }

    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Float):
    Map[Float, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 >= partitionValue }
    }

    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Float,
                                                  partitionValueEnd: Float): Map[Float, Path] = {
      require(partitionValueStart <= partitionValueEnd)
      getPartitionValuePathMap(paths).filter { partitionValueMap =>
        partitionValueMap._1 >= partitionValueStart && partitionValueMap._1 <= partitionValueEnd }
    }
  }

  implicit object PathInLong extends PathImplicits[Long] {

    override def getPartitionValuePathMap(paths: Array[Path]): Map[Long, Path] = {
      paths.flatMap { (path: Path) => {
        Map[Long, Path](extractPartitionValue(path).toLong -> path)
      }
      }.toMap
    }

    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      val pathMap = getPartitionValuePathMap(paths)
      pathMap(pathMap.keySet.max)
    }

    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 < partitionValue }
    }

    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 > partitionValue }
    }

    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      getPartitionValuePathMap(paths).filter(partitionValueMap => partitionValueMap._1 <= partitionValue)
    }

    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      getPartitionValuePathMap(paths).filter { partitionValueMap => partitionValueMap._1 >= partitionValue }
    }

    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Long,
                                                  partitionValueEnd: Long): Map[Long, Path] = {
      require(partitionValueStart <= partitionValueEnd)
      getPartitionValuePathMap(paths).filter { partitionValueMap =>
        partitionValueMap._1 >= partitionValueStart && partitionValueMap._1 <= partitionValueEnd }
    }
  }

  /**
   * To extract the partition value from given Path
   *
   * @throws InvalidPathException if unable to extract partition value
   * @param fileInfo Path
   * @return String
   */
  @throws(classOf[InvalidPathException])
  private def extractPartitionValue(fileInfo: Path): String = {
    fileInfo.getName
      .split("=")
      .lift(1)
      .getOrElse(throw new InvalidPathException(fileInfo.toString, "Unable to extract partition value"))
      .trim
  }

}
