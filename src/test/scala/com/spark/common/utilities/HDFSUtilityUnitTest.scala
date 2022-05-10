package com.spark.common.utilities

import com.spark.common.utilities.HDFSUtility

import java.io.File
import java.util
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class HDFSUtilityUnitTest extends AnyFlatSpec with BeforeAndAfterAll {

  private val hdConfig = new Configuration()
  val hadoopOptionsConfig: Config = ConfigFactory.load("file:///")
  val hadoopOptionsEntrySet: mutable.Set[util.Map.Entry[String, ConfigValue]] = hadoopOptionsConfig.entrySet().asScala
  hadoopOptionsEntrySet.foreach {
    eachEntry: java.util.Map.Entry[String, ConfigValue] =>
      hdConfig.set(eachEntry.getKey, eachEntry.getValue.render().replace("\"", ""))
  }

  private val latestBasePath = "src/test/resources/SampleForTestCaseItShouldAutoDelete/"
  private val latestPartitionBasePathInt = "src/test/resources/SampleForEqualOperatorPathItShouldAutoDeleteInt/"
  private val latestPartitionBasePathDouble = "src/test/resources/SampleForEqualOperatorPathItShouldAutoDeleteDouble/"
  private val latestPartitionBasePathLong = "src/test/resources/SampleForEqualOperatorPathItShouldAutoDeleteLong/"
  private val latestPartitionBasePathFloat = "src/test/resources/SampleForEqualOperatorPathItShouldAutoDeleteFloat/"
  private val baseDeletePath = "src/test/resources/SampleDeleteForTestCaseItShouldAutoDelete/"

  override protected def afterAll(): Unit = {
    val scalaPath: ScalaPath = ScalaPath(latestBasePath)
    scalaPath.deleteRecursively()
    val scalaPathWithEqualOpInt: ScalaPath = ScalaPath(latestPartitionBasePathInt)
    scalaPathWithEqualOpInt.deleteRecursively()
    val scalaPathWithEqualOpDouble: ScalaPath = ScalaPath(latestPartitionBasePathDouble)
    scalaPathWithEqualOpDouble.deleteRecursively()
    val scalaPathWithEqualOpLong: ScalaPath = ScalaPath(latestPartitionBasePathLong)
    scalaPathWithEqualOpLong.deleteRecursively()
    val scalaPathWithEqualOpFloat: ScalaPath = ScalaPath(latestPartitionBasePathFloat)
    scalaPathWithEqualOpFloat.deleteRecursively()
    val scalaBaseDeletePath: ScalaPath = ScalaPath(baseDeletePath)
    scalaBaseDeletePath.deleteRecursively()
  }

  def mkdirs(path: List[String]): Boolean =
    path.tail.foldLeft(new File(path.head)) {
      (baseDir: File, name: String) => baseDir.mkdir; new File(baseDir, name)
    }.mkdir

  mkdirs(List(latestBasePath, "02"))
  mkdirs(List(latestBasePath, "04"))
  mkdirs(List(latestBasePath, "05"))

  private val latestPathAvailable: String =
    HDFSUtility.findLatestPath(new Path(latestBasePath), hdConfig).toString

  // Positive tests!
  "HDFSUtility.getLatestDirectory " should " give the latest available path" in {
    latestPathAvailable.endsWith("05") should be(true)
  }

  // Negative tests!
  "HDFSUtility.getLatestDirectory" should "not match with wrong latest path" in {
    latestPathAvailable.endsWith("04") should be(false)
  }

  /**
   * ************************************************ For Int data type *****************************************
   */

  mkdirs(List(latestPartitionBasePathInt, "date=06"))
  mkdirs(List(latestPartitionBasePathInt, "date=07"))
  mkdirs(List(latestPartitionBasePathInt, "date=08"))

  // '>= IN BETWEEN =<' - Input Validation Test
  "HDFSUtility.getInBetweenPartitionPaths" should "throws IllegalArgumentException if Int start > end" in {
    assertThrows[IllegalArgumentException](HDFSUtility.getPathsBetweenPartitionValues[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 14, 12))
  }

  // '>= IN BETWEEN =<' - Negative Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return empty set if no paths in between start and end under the Int Base Path" in {
    val inBetweenPartitionPathsInt: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 9, 10)
    assert(inBetweenPartitionPathsInt.isEmpty)
  }

  // '>= IN BETWEEN =<' - Positive Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return exactly the paths in between start and end under the Int Base Path" in {
    val inBetweenPartitionPathsInt: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 7, 8)
    val partitions: Set[String] = Set("07", "08")
    assert(inBetweenPartitionPathsInt.map(_.getName.split("=").last).equals(partitions))
  }

  // GREATER THAN EQUAL '>=' - Negative Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return empty set if no path >= start under the Int Base Path" in {
    val greaterThanEqualPartitionPathsInt: Set[Path] = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 9)
    assert(greaterThanEqualPartitionPathsInt.isEmpty)
  }

  // GREATER THAN EQUAL '>=' - Positive Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return exactly the paths >= start under the Int Base Path" in {
    val greaterThanEqualPartitionPathsInt = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 7)
    val partitions: Set[String] = Set("07", "08")
    assert(greaterThanEqualPartitionPathsInt.map(_.getName.split("=").last).equals(partitions))
  }

  private val latestPartitionBasePathWithEqualOpInt: String =
    HDFSUtility.findLatestPartitionPath[Int](new Path(latestPartitionBasePathInt), hdConfig).toString

  // GET LATEST DIRECTORY - Positive tests!
  "HDFSUtility.getLatestDirectory " should " give the latest available partition path in Int format" in {
    latestPartitionBasePathWithEqualOpInt.endsWith("08") should be(true)
  }

  // GET LATEST DIRECTORY - Negative tests!
  "HDFSUtility.getLatestDirectory" should "not match with wrong latest partition path in Int format" in {
    latestPartitionBasePathWithEqualOpInt.endsWith("07") should be(false)
  }

  // GET GREATER THAN '>' PARTITION PATHS - Positive tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return partition path '8'  given '7' as partitionValue path under the Int Base Path" in {
    val greaterThanPartitionPathsInt: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 7)
    greaterThanPartitionPathsInt.map { path: Path =>
      assert(path.getName.equals("date=08"))
    }
  }

  // GET GREATER THAN '>' PARTITION PATHS - Negative tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return empty partition paths set, given '8' as partitionValue path under the Int Base Path" in {
    val greaterThanPartitionPathsInt: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 8)
    assertResult(false)(greaterThanPartitionPathsInt.nonEmpty)
  }

  // GET LESS THAN '<' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return partition path '6'  given '7' as partitionValue path under the Int Base Path" in {
    val getLessThanPartitionPathsInt: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 7)
    getLessThanPartitionPathsInt.map { path: Path =>
      assert(path.getName.equals("date=06"))
    }
  }

  // GET LESS THAN '<' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return empty partition paths set, given '6' as partitionValue path under the Int Base Path" in {
    val getLessThanPartitionPathsInt: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 6)
    assertResult(false)(getLessThanPartitionPathsInt.nonEmpty)
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return partition path '6' and '7'  given '7' as partitionValue path under the Int Base Path" in {
    val getLessThanEqualPartitionPathsInt: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 7)
    val pathFinalComponents: Set[String] = getLessThanEqualPartitionPathsInt.map(path => path.getName)
    assert(pathFinalComponents.contains("date=07"))
    assert(pathFinalComponents.contains("date=06"))
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return empty partition paths set, given '5' as partitionValue path under the Int Base Path" in {
    val getLessThanEqualPartitionPathsInt: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Int](
      new Path(latestPartitionBasePathInt), hdConfig, 5)
    assertResult(false)(getLessThanEqualPartitionPathsInt.nonEmpty)
  }

  /**
   * ************************************************ For Double data type *****************************************
   */

  mkdirs(List(latestPartitionBasePathDouble, "date=9.0"))
  mkdirs(List(latestPartitionBasePathDouble, "date=10.0"))
  mkdirs(List(latestPartitionBasePathDouble, "date=11.0"))

  // '>= IN BETWEEN =<' - Input Validation Test
  "HDFSUtility.getInBetweenPartitionPaths" should "throws IllegalArgumentException if Double start > end" in {
    assertThrows[IllegalArgumentException](HDFSUtility.getPathsBetweenPartitionValues[Double](
      new Path(latestPartitionBasePathInt), hdConfig, 14.0, 12.0))

  }

  // '>= IN BETWEEN =<' - Negative Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return empty set if no paths in between start and end under the Double Base Path" in {
    val inBetweenPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 7.0, 8.0)
    assert(inBetweenPartitionPathsDouble.isEmpty)
  }

  // '>= IN BETWEEN =<' - Positive Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return exactly the paths in between start and end under the Double Base Path" in {
    val inBetweenPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 9.0, 11.0)
    val partitions: Set[String] = Set("9.0", "10.0", "11.0")
    assert(inBetweenPartitionPathsDouble.map(_.getName.split("=").last).equals(partitions))
  }

  // GREATER THAN EQUAL '>=' - Negative Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return empty set if no paths >= start under the Double Base Path" in {
    val greaterThanEqualPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 20.0)
    assert(greaterThanEqualPartitionPathsDouble.isEmpty)
  }

  // GREATER THAN EQUAL '>=' - Positive Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return exactly the paths >= start under the Double Base Path" in {
    val greaterThanEqualPartitionPathsDouble = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 10.0)
    val partitions: Set[String] = Set("10.0", "11.0")
    assert(greaterThanEqualPartitionPathsDouble.map(_.getName.split("=").last).equals(partitions))
  }

  private val latestPartitionBasePathWithEqualOpDouble: String =
    HDFSUtility.findLatestPartitionPath[Double](new Path(latestPartitionBasePathDouble), hdConfig).toString

  // GET LATEST DIRECTORY - Positive tests!
  "HDFSUtility.getLatestDirectory " should " give the latest available partition path in Double format" in {
    latestPartitionBasePathWithEqualOpDouble.endsWith("11.0") should be(true)
  }

  // GET LATEST DIRECTORY - Negative tests!
  "HDFSUtility.getLatestDirectory" should "not match with wrong latest partition path in Double format" in {
    latestPartitionBasePathWithEqualOpDouble.endsWith("10.0") should be(false)
  }

  // GET GREATER THAN '>' PARTITION PATHS - Positive tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return partition path '11.0'  given '10.0' as partitionValue path under the Double Base Path" in {
    val greaterThanPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 10.0)
    greaterThanPartitionPathsDouble.map { path: Path =>
      assert(path.getName.equals("date=11.0"))
    }
  }

  // GET GREATER THAN '>' PARTITION PATHS - Negative tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return empty partition paths set, given '11.0' as partitionValue path under the Double Base Path" in {
    val greaterThanPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 11.0)
    assertResult(false)(greaterThanPartitionPathsDouble.nonEmpty)
  }

  // GET LESS THAN '<' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return partition path '9.0'  given '10.0' as partitionValue path under the Double Base Path" in {
    val getLessThanPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 10.0)
    getLessThanPartitionPathsDouble.map { path: Path =>
      assert(path.getName.equals("date=9.0"))
    }
  }

  // GET LESS THAN '<' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return empty partition paths set, given '9.0' as partitionValue path under the Double Base Path" in {
    val getLessThanPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 9.0)
    assertResult(false)(getLessThanPartitionPathsDouble.nonEmpty)
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return partition path '9.0' and '10.0'  given '10.0' as partitionValue path under the Double Base Path" in {
    val getLessThanEqualPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 10.0)
    val pathFinalComponents: Set[String] = getLessThanEqualPartitionPathsDouble.map(path => path.getName)
    assert(pathFinalComponents.contains("date=10.0"))
    assert(pathFinalComponents.contains("date=9.0"))
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return empty partition paths set, given '8.0' as partitionValue path under the Double Base Path" in {
    val getLessThanEqualPartitionPathsDouble: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Double](
      new Path(latestPartitionBasePathDouble), hdConfig, 8.0)
    assertResult(false)(getLessThanEqualPartitionPathsDouble.nonEmpty)
  }

  /**
   * ************************************************ For Float data type *****************************************
   */

  mkdirs(List(latestPartitionBasePathFloat, "date=91.0"))
  mkdirs(List(latestPartitionBasePathFloat, "date=101.0"))
  mkdirs(List(latestPartitionBasePathFloat, "date=111.0"))

  // '>= IN BETWEEN =<' - Input Validation Test
  "HDFSUtility.getInBetweenPartitionPaths" should "throws IllegalArgumentException if Float start > end" in {
    assertThrows[IllegalArgumentException](HDFSUtility.getPathsBetweenPartitionValues[Float](
      new Path(latestPartitionBasePathInt), hdConfig, 71, 51))
  }

  // '>= IN BETWEEN =<' - Negative Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return empty set if no paths in between start and end under the Float Base Path" in {
    val inBetweenPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 71, 81)
    assert(inBetweenPartitionPathsFloat.isEmpty)
  }

  // '>= IN BETWEEN =<' - Positive Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return all the paths in between start and end (inclusive) under the Float Base Path" in {
    val inBetweenPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 91, 111)
    val partitions: Set[String] = Set("91.0", "101.0", "111.0")
    assert(inBetweenPartitionPathsFloat.map(_.getName.split("=").last).equals(partitions))
  }

  // GREATER THAN EQUAL '>=' - Negative Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return empty set if no path >= start under the Float Base Path" in {
    val greaterThanEqualPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 121)
    assert(greaterThanEqualPartitionPathsFloat.isEmpty)
  }

  // GREATER THAN EQUAL '>=' - Positive Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return exactly the paths >= start under the Float Base Path" in {
    val greaterThanEqualPartitionPathsFloat = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 91)
    val partitions: Set[String] = Set("91.0", "101.0", "111.0")
    assert(greaterThanEqualPartitionPathsFloat.map(_.getName.split("=").last).equals(partitions))
  }

  private val latestPartitionBasePathWithEqualOpFloat: String =
    HDFSUtility.findLatestPartitionPath[Float](new Path(latestPartitionBasePathFloat), hdConfig).toString

  // GET LATEST DIRECTORY - Positive tests!
  "HDFSUtility.getLatestDirectory " should " give the latest available partition path in Float format" in {
    latestPartitionBasePathWithEqualOpFloat.endsWith("111.0") should be(true)
  }

  // GET LATEST DIRECTORY - Negative tests!
  "HDFSUtility.getLatestDirectory" should "not match with wrong latest partition path in Float format" in {
    latestPartitionBasePathWithEqualOpFloat.endsWith("101.0") should be(false)
  }

  // GET GREATER THAN '>' PARTITION PATHS - Positive tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return partition path '111.0'  given '101.0' as partitionValue path under the Float Base Path" in {
    val greaterThanPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 101.0F)
    greaterThanPartitionPathsFloat.map { path: Path =>
      assert(path.getName.equals("date=111.0"))
    }
  }

  // GET GREATER THAN '>' PARTITION PATHS - Negative tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return empty partition paths set, given '111.0' as partitionValue path under the Float Base Path" in {
    val greaterThanPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 111.0F)
    assertResult(false)(greaterThanPartitionPathsFloat.nonEmpty)
  }

  // GET LESS THAN '<' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return partition path '91.0'  given '101.0' as partitionValue path under the Float Base Path" in {
    val getLessThanPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 101.0F)
    getLessThanPartitionPathsFloat.map { path: Path =>
      assert(path.getName.equals("date=91.0"))
    }
  }

  // GET LESS THAN '<' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return empty partition paths set, given '91.0' as partitionValue path under the Float Base Path" in {
    val getLessThanPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 91.0F)
    assertResult(false)(getLessThanPartitionPathsFloat.nonEmpty)
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return partition path '91.0' and '101.0'  given '10.0' as partitionValue path under the Float Base Path" in {
    val getLessThanEqualPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 101.0F)
    val pathFinalComponents: Set[String] = getLessThanEqualPartitionPathsFloat.map(path => path.getName)
    assert(pathFinalComponents.contains("date=101.0"))
    assert(pathFinalComponents.contains("date=91.0"))
  }

  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return empty partition paths set, given '81.0' as partitionValue path under the Float Base Path" in {
    val getLessThanEqualPartitionPathsFloat: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Float](
      new Path(latestPartitionBasePathFloat), hdConfig, 81.0F)
    assertResult(false)(getLessThanEqualPartitionPathsFloat.nonEmpty)
  }


  /**
   * ************************************************ For Long data type *****************************************
   */

  mkdirs(List(latestPartitionBasePathLong, "date=12"))
  mkdirs(List(latestPartitionBasePathLong, "date=13"))
  mkdirs(List(latestPartitionBasePathLong, "date=14"))

  // '>= IN BETWEEN =<' - Input Validation Test
  "HDFSUtility.getInBetweenPartitionPaths" should "throws IllegalArgumentException if Long start > end" in {
    assertThrows[IllegalArgumentException](HDFSUtility.getPathsBetweenPartitionValues[Float](
      new Path(latestPartitionBasePathInt), hdConfig, 12, 10))

  }

  // '>= IN BETWEEN =<' - Negative Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return empty set if no paths in between start and end under the Long Base Path" in {
    val inBetweenPartitionPathsLong: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 15, 16)
    assert(inBetweenPartitionPathsLong.isEmpty)
  }

  // '>= IN BETWEEN =<' - Positive Test
  "HDFSUtility.getInBetweenPartitionPaths" should
    "return all the paths in between start and end under the Long Base Path" in {
    val inBetweenPartitionPathsLong: Set[Path] = HDFSUtility.getPathsBetweenPartitionValues[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 12, 13)
    val partitions: Set[String] = Set("12", "13")
    println(inBetweenPartitionPathsLong.map(_.getName.split("=").last), partitions)
    assert(inBetweenPartitionPathsLong.map(_.getName.split("=").last).equals(partitions))
  }

  // GREATER THAN EQUAL '>=' - Negative Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return empty set if no path >= start under the Long Base Path" in {
    val greaterThanEqualPartitionPathsLong: Set[Path] = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 15)
    assert(greaterThanEqualPartitionPathsLong.isEmpty)
  }

  // GREATER THAN EQUAL '>=' - Positive Test
  "HDFSUtility.getGreaterThanEqualPartitionPaths" should "return exactly the paths >= start under the Long Base Path" in {
    val greaterThanEqualPartitionPathsLong = HDFSUtility.getPathsGreaterThanEqualPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 13)
    val partitions: Set[String] = Set("13", "14")
    assert(greaterThanEqualPartitionPathsLong.map(_.getName.split("=").last).equals(partitions))
  }

  private val latestPartitionBasePathWithEqualOpLong: String =
    HDFSUtility.findLatestPartitionPath[Long](new Path(latestPartitionBasePathLong), hdConfig).toString

  // GET LATEST DIRECTORY - Positive tests!
  "HDFSUtility.getLatestDirectory " should " give the latest available partition path in Long format" in {
    latestPartitionBasePathWithEqualOpLong.endsWith("14") should be(true)
  }

  // GET LATEST DIRECTORY - Negative tests!
  "HDFSUtility.getLatestDirectory" should "not match with wrong latest partition path in Long format" in {
    latestPartitionBasePathWithEqualOpLong.endsWith("13") should be(false)
  }

  // GET GREATER THAN '>' PARTITION PATHS - Positive tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return partition path '14'  given '13' as partitionValue path under the Long Base Path" in {
    val greaterThanPartitionPathsLong: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 13)
    greaterThanPartitionPathsLong.map { path: Path =>
      assert(path.getName.equals("date=14"))
    }
  }

  // GET GREATER THAN '>' PARTITION PATHS - Negative tests!
  "HDFSUtility.getGreaterThanPartitionPaths" should
    "return empty partition paths set, given '14' as partitionValue path under the Long Base Path" in {
    val greaterThanPartitionPathsLong: Set[Path] = HDFSUtility.getPathsGreaterThanPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 14)
    assertResult(false)(greaterThanPartitionPathsLong.nonEmpty)
  }

  // GET LESS THAN '<' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return partition path '12'  given '13' as partitionValue path under the Long Base Path" in {
    val getLessThanPartitionPathsLong: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 13)
    getLessThanPartitionPathsLong.map { path: Path =>
      assert(path.getName.equals("date=12"))
    }
  }

  // // GET LESS THAN '<' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanPartitionPaths" should
    "return empty partition paths set, given '12' as partitionValue path under the Long Base Path" in {
    val getLessThanPartitionPathsLong: Set[Path] = HDFSUtility.getPathsLessThanPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 12)
    assertResult(false)(getLessThanPartitionPathsLong.nonEmpty)
  }


  // GET LESS THAN EQUAL '<=' PARTITION PATHS - Positive tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return partition path '12' and '13'  given '13' as partitionValue path under the Long Base Path" in {
    val getLessThanEqualPartitionPathsLong: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 13)
    val pathFinalComponents: Set[String] = getLessThanEqualPartitionPathsLong.map(path => path.getName)
    assert(pathFinalComponents.contains("date=13"))
    assert(pathFinalComponents.contains("date=12"))
  }

  // // GET LESS THAN EQUAL '<=' PARTITION PATHS - Negative tests!
  "HDFSUtility.getLessThanEqualPartitionPaths" should
    "return empty partition paths set, given '11' as partitionValue path under the Long Base Path" in {
    val getLessThanEqualPartitionPathsLong: Set[Path] = HDFSUtility.getPathsLessThanEqualPartitionValue[Long](
      new Path(latestPartitionBasePathLong), hdConfig, 11)
    assertResult(false)(getLessThanEqualPartitionPathsLong.nonEmpty)
  }


  mkdirs(List(latestBasePath, "2021", "03"))
  mkdirs(List(latestBasePath, "2021", "01"))

  "HDFSUtility.deleteHdfsPath " should "throw IOException the directory recursively because recursive=false and is not empty" in {
    assertThrows[java.io.IOException](HDFSUtility.deleteHdfsPath(new Path(latestBasePath), recursive = false, hdConfig))
  }

  "HDFSUtility.deleteHdfsPath " should "delete the directory recursively " in {
    HDFSUtility.deleteHdfsPath(new Path(latestBasePath), recursive = true, hdConfig) should be(true)
  }

  mkdirs(List(baseDeletePath, "1.txt"))
  "HDFSUtility.deleteHdfsPath " should "delete the directory " in {
    HDFSUtility.deleteHdfsPath(new Path(baseDeletePath + "1.txt"), recursive = false, hdConfig) should be(true)
  }

  //Negative test
  "HDFSUtility.deleteHdfsPath " should " return false as directory does not exits " in {
    HDFSUtility.deleteHdfsPath(new Path("path not exist"), recursive = true, hdConfig) should be(false)
  }

}
