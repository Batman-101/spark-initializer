package com.spark.common.reader

import com.spark.common.reader.{CsvReader, JsonReader, ORCReader, ParquetReader, TextReader}
import com.spark.common.writer.{CsvWriter, JsonWriter, ORCWriter, ParquetWriter, TextWriter}

import java.io.File
import java.nio.file.Files
import com.spark.common.writer._
import org.apache.spark.sql.{AnalysisException, Dataset, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

object TestData {
  def apply(id: Int): TestData = TestData(id, s"data$id", id % 10)
}
case class TestData(id: Int, data: String, group: Int)


class ReaderWriterUnitTest extends AnyFlatSpec with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("TestSpark")
    .getOrCreate()

  import org.apache.spark.sql.types._

  val customSchema: StructType = new StructType()
    .add("id", IntegerType, true)
    .add("data", StringType, true)
    .add("group", IntegerType, true)

  import spark.implicits._

  override protected def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  /**
   * ORC reader writer tests
   */

  val tempDir: File = Files.createTempDirectory("testDirectory").toFile

  "ORCReader" should "be able to read an orc file and ORCwriter should write, they should be able to read and write in concert" in {
    val tempFilePath = tempDir.getPath+"test.orc"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an orc file
    val testDataFrame = testDataset.toDF
    ORCWriter.write(testDataFrame, tempFilePath, SaveMode.Overwrite)

    // Read the orc file
    val readDataset: Dataset[TestData] = ORCReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }

  "ORCReader" should "be able to read an multiple orc files and ORCWriter should write," +
    "they should be able to read and write in concert" in {
    val tempFilePath1 = tempDir.getPath+"testPath1.orc"
    val tempFilePath2 = tempDir.getPath+"testPath2.orc"

    // Generate test data for tempFilePath1
    val testDatasetForTempFilePath1: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })
    // Generate test data for tempFilePath2
    val testDatasetForTempFilePath2: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an orc files
    val testDataFrameForTempFilePath1 = testDatasetForTempFilePath1.toDF
    val testDataFrameForTempFilePath2 = testDatasetForTempFilePath1.toDF
    ORCWriter.write(testDataFrameForTempFilePath1, tempFilePath1, SaveMode.Overwrite)
    ORCWriter.write(testDataFrameForTempFilePath2, tempFilePath2, SaveMode.Overwrite)

    // Read the orc files
    val readDataset: Dataset[TestData] = ORCReader.read(spark, tempFilePath1, tempFilePath2)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDatasetForTempFilePath1).count() +
      readDataset.intersect(testDatasetForTempFilePath2).count()  == 2000)
  }

  // Negative Test: ORCReader read utility call without any path given to var args
  "ORCReader" should "be not able to read an orc file and it should throw analysis exception given no path" +
    "provided" in {

    // Read the orc file without providing any path to read utility
      assertThrows[AnalysisException](ORCReader.read(spark))
  }

  it should "have a mechanism for partition writes" in {
    val tempFilePath = tempDir.getPath+"testpart.orc"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an orc file
    val testDataFrame = testDataset.toDF
    ORCWriter.write(testDataFrame, tempFilePath, Seq("group"), saveMode=SaveMode.Overwrite)

    // Read the orc file
    val readDataset: Dataset[TestData] = ORCReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }

  /**
   * Parquet reader writer tests
   */

  val tempParquetDir: File = Files.createTempDirectory("testParquetDirectory").toFile

  "ParquetReader" should "be able to read an Parquet file and ParquetWriter should write, they should be able to read and write in concert" in {
    val tempFilePath = tempParquetDir.getPath+"_test.parquet"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an Parquet file
    val testDataFrame = testDataset.toDF
    ParquetWriter.write(testDataFrame, tempFilePath, SaveMode.Overwrite)

    // Read the Parquet file
    val readDataset: Dataset[TestData] = ParquetReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }

  "ParquetReader" should "be able to read an multiple parquet files and ParquetWriter should write," +
    "they should be able to read and write in concert" in {
    val tempFilePath1 = tempDir.getPath+"testPath1.parquet"
    val tempFilePath2 = tempDir.getPath+"testPath2.parquet"

    // Generate test data for tempFilePath1
    val testDatasetForTempFilePath1: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })
    // Generate test data for tempFilePath2
    val testDatasetForTempFilePath2: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an orc files
    val testDataFrameForTempFilePath1 = testDatasetForTempFilePath1.toDF
    val testDataFrameForTempFilePath2 = testDatasetForTempFilePath1.toDF
    ParquetWriter.write(testDataFrameForTempFilePath1, tempFilePath1, SaveMode.Overwrite)
    ParquetWriter.write(testDataFrameForTempFilePath2, tempFilePath2, SaveMode.Overwrite)

    // Read the orc files
    val readDataset: Dataset[TestData] = ParquetReader.read(spark, tempFilePath1, tempFilePath2)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDatasetForTempFilePath1).count() +
      readDataset.intersect(testDatasetForTempFilePath2).count()  == 2000)
  }

  // Negative Test: ParquetReader read utility call without any path given to var args
  "ParquetReader" should "be not able to read an parquet file and it should throw analysis exception given no path" +
    "provided" in {

    // Read the orc file without providing any path to read utility
    assertThrows[AnalysisException](ParquetReader.read(spark))
  }

  it should "have a mechanism for partition writes" in {
    val tempFilePath = tempParquetDir.getPath+"testpart.parquet"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an Parquet file
    val testDataFrame = testDataset.toDF
    ParquetWriter.write(testDataFrame, tempFilePath, Seq("group"), saveMode=SaveMode.Overwrite)

    // Read the Parquet file
    val readDataset: Dataset[TestData] = ParquetReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"), x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }


  /**
   * Json reader writer tests
   */
  val tempJsonDir: File = Files.createTempDirectory("testJsonDirectory").toFile

  "JsonReader" should "be able to read an Json file and JsonWriter should write, they should be able to read and write in concert" in {
    val tempFilePath = tempJsonDir.getPath+"_test.json"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an Json file
    val testDataFrame = testDataset.toDF
    JsonWriter.write(testDataFrame, tempFilePath, SaveMode.Overwrite)

    // Read the Json file
    val readDataset: Dataset[TestData] = JsonReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Long]("id").toInt,
          x.getAs[String]("data"), x.getAs[Long]("group").toInt)
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }

  "JsonReader" should "be able to read an multiple JSON files and JsonWriter should write," +
    "they should be able to read and write in concert" in {
    val tempFilePath1 = tempDir.getPath+"testPath1.json"
    val tempFilePath2 = tempDir.getPath+"testPath2.json"

    // Generate test data for tempFilePath1
    val testDatasetForTempFilePath1: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })
    // Generate test data for tempFilePath2
    val testDatasetForTempFilePath2: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an orc files
    val testDataFrameForTempFilePath1 = testDatasetForTempFilePath1.toDF
    val testDataFrameForTempFilePath2 = testDatasetForTempFilePath1.toDF
    JsonWriter.write(testDataFrameForTempFilePath1, tempFilePath1, SaveMode.Overwrite)
    JsonWriter.write(testDataFrameForTempFilePath2, tempFilePath2, SaveMode.Overwrite)

    // Read the orc files
    val readDataset: Dataset[TestData] = JsonReader.read(spark, tempFilePath1, tempFilePath2)
      .map(x => {
        TestData(x.getAs[Long]("id").toInt,
          x.getAs[String]("data"),
          x.getAs[Long]("group").toInt)
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDatasetForTempFilePath1).count() +
      readDataset.intersect(testDatasetForTempFilePath2).count()  == 2000)
  }


  // Negative Test: JsonReader read utility call without any path given to var args
  "JsonReader" should "be not able to read an JSON file and it should throw analysis exception given no path" +
    "provided" in {

    // Read the orc file without providing any path to read utility
    assertThrows[AnalysisException](JsonReader.read(spark))
  }

  it should "have a mechanism for partition writes" in {
    val tempFilePath = tempJsonDir.getPath+"testpart.json"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an Json file
    val testDataFrame = testDataset.toDF
    JsonWriter.write(testDataFrame, tempFilePath, Seq("group"), saveMode=SaveMode.Overwrite)

    // Read the Json file
    val readDataset: Dataset[TestData] = JsonReader.read(spark, tempFilePath)
      .map(x => {
        TestData(x.getAs[Long]("id").toInt,
          x.getAs[String]("data"),
          x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }


  /**
   * CSV reader writer tests
   */
  val tempCsvDir: File = Files.createTempDirectory("testCSVDirectory").toFile

  "CsvReader" should "be able to read an CSV file and CsvWriter should write, they should be able to read and write in concert" in {
    val tempFilePath = tempCsvDir.getPath+"_test.csv"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an csv file
    val testDataFrame = testDataset.toDF
    CsvWriter.write(testDataFrame, tempFilePath, SaveMode.Overwrite)

    // Read the Csv file
    val readDataset: Dataset[TestData] = CsvReader.read(spark, customSchema, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"),
          x.getAs[String]("data"), x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }

  "CsvReader" should "be able to read an multiple csv files and CsvWriter should write," +
    "they should be able to read and write in concert" in {
    val tempFilePath1 = tempDir.getPath+"testPath1.csv"
    val tempFilePath2 = tempDir.getPath+"testPath2.csv"

    // Generate test data for tempFilePath1
    val testDatasetForTempFilePath1: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })
    // Generate test data for tempFilePath2
    val testDatasetForTempFilePath2: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an csv files
    val testDataFrameForTempFilePath1 = testDatasetForTempFilePath1.toDF
    val testDataFrameForTempFilePath2 = testDatasetForTempFilePath1.toDF
    CsvWriter.write(testDataFrameForTempFilePath1, tempFilePath1, SaveMode.Overwrite)
    CsvWriter.write(testDataFrameForTempFilePath2, tempFilePath2, SaveMode.Overwrite)

    // Read the csv files
    val readDataset: Dataset[TestData] = CsvReader.read(spark, customSchema, tempFilePath1, tempFilePath2)
      .map(x => {
        TestData(x.getAs[Int]("id"),
          x.getAs[String]("data"),
          x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDatasetForTempFilePath1).count() +
      readDataset.intersect(testDatasetForTempFilePath2).count()  == 2000)
  }


  // Negative Test: CsvReader read utility call without any path given to var args
  "CsvReader" should "be able to perform read operation but empty dataframe will be returned as there is no path" +
    " provided" in {

    // Read the csv file without providing any path to read utility
    assert(CsvReader.read(spark, customSchema).isEmpty)
  }

  it should "have a mechanism for partition writes" in {
    val tempFilePath = tempCsvDir.getPath+"testpart.csv"

    // Generate test data
    val testDataset: Dataset[TestData] = spark.range(1000).map(x => {
      TestData(x.toInt)
    })

    // Write the test data to an csv file
    val testDataFrame = testDataset.toDF
    CsvWriter.write(testDataFrame, tempFilePath, Seq("group"), saveMode=SaveMode.Overwrite)

    // Read the csv file
    val readDataset: Dataset[TestData] = CsvReader.read(spark, customSchema, tempFilePath)
      .map(x => {
        TestData(x.getAs[Int]("id"),
          x.getAs[String]("data"),
          x.getAs[Int]("group"))
      })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 1000)
  }



  /**
   * Text reader writer tests
   */
  val tempTextDir: File = Files.createTempDirectory("testTextDirectory").toFile

  "TextReader" should "be able to read an Text file and TextWriter should write, they should be able to read and write in concert" in {
    val tempFilePath = tempTextDir.getPath+"_test.txt"

    // Generate test data
      val dataList = List(
      TextDataSingleColumn("abc"),
      TextDataSingleColumn("xyz")
    )

    val testDataset = spark.createDataset(dataList)

    // Write the test data to an Text file
    val testDataFrame = testDataset.toDF
    TextWriter.write(testDataFrame, tempFilePath, SaveMode.Overwrite)

    // Read the Text file
    val readDataset: Dataset[TextDataSingleColumn] = TextReader.read(spark, tempFilePath).map(x => {
      TextDataSingleColumn(x.getAs[String]("value"))
    })

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataset).count() == 2)
  }

  it should "have a mechanism for partition writes" in {
    val tempFilePath = tempTextDir.getPath + "testpart.txt"

    // Generate test data
    val testDataset: Dataset[TextDataMultiColumn] = spark.range(50).map(x => {
      TextDataMultiColumn(x.toString, (x/10).toString, (x%10).toString)
    })

    // Write the test data to an text file
    val testDataFrame = testDataset.toDF
    TextWriter.write(testDataFrame, tempFilePath, Seq("divisor", "remainder"), saveMode = SaveMode.Overwrite)

    // Read the text file
    val readDataset = TextReader.read(spark, tempFilePath).toDF

    // assert that the data written is an exact match to the data that was read
    assert(readDataset.intersect(testDataFrame).count() == 50)
  }

}
