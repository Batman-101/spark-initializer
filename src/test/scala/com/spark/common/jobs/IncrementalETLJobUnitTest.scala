package com.spark.common.jobs

import com.spark.common.jobs.IncrementalETLJob
import com.spark.common.reader.ORCReader
import com.spark.common.writer.ORCWriter
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class IncrementalETLJobUnitTest extends AnyFlatSpec with BeforeAndAfterAll {

  val basePath = "target/path/"

  case class IncrementalETLImpl(jobFun: (Config, SparkSession) => Unit) extends IncrementalETLJob {

    /**
     * To incremental work log details from audit table.
     *
     * @param config       Contains the current execution configuration
     * @param sparkSession Contains the current spark session information
     * @return DataFrame: Contains unit of work to be processed by the Job.
     */
    override def getIncrementalWorkLog(config: Config, sparkSession: SparkSession): DataFrame = {
      jobFun(config, sparkSession)

      val logicalBatchId = config.getString("logicalBatchId")

      val someSchema = List(
        StructField(logicalBatchId, StringType, nullable = true),
        StructField("eventId", StringType, nullable = true),
        StructField("readPath", StringType, nullable = true),
        StructField("writePath", StringType, nullable = true)
      )

      // Mocked Data frame,
      // In real situation this data will fetched based on user preference, For Example: In spark
      // This data is fetched by joining Batch_Run and File_Run table.
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(
        Row("file1.bin", "uuid1", s"${basePath}to/batch1/readData", s"${basePath}to/batch1/writeData"),
        Row("file2.bin", "uuid1", s"${basePath}to/batch2/readData", s"${basePath}to/batch2/writeData"))),
        StructType(someSchema))

    }

    /**
     * To update the configuration details w.r.t each unit of work
     *
     * Note: config: Config - Global object contains path like source/sink/error paths etc..
     *
     * Example: Source path, Sink path, Error path to be processed changes w.r.t each unit of work
     *
     * @param config       Contains the current execution configuration
     * @param sparkSession Contains the current spark session information
     * @param row          Contains information of each unit of work to be processed
     * @return Config updated with required details of each unit of work
     */
    override def updateBatchConfig(config: Config, sparkSession: SparkSession, row: Row): Config = {

      // sparkSession: SparkSession: can be used for your internal operation to fetch the configuration from HDFS path
      val readPath = row.getAs[String]("readPath")
      val writePath = row.getAs[String]("writePath")
      config
        .withValue("readPath", ConfigValueFactory.fromAnyRef(readPath))
        .withValue("writePath", ConfigValueFactory.fromAnyRef(writePath))
      //.withValue("testColumn", ConfigValueFactory.fromAnyRef("dummyValue"))
    }


    /**
     * To read the source data applicable for each unit of work
     *
     * @param config       Contains the current execution configuration like source location etc..
     * @param sparkSession Contains the current spark session information
     * @return DataFrame contains source data associated with each unit of work
     */
    override def readBatchData(config: Config, sparkSession: SparkSession): DataFrame = {

      // preparing test data for read the batch data
      val someSchema = List(
        StructField("test1", StringType, nullable = true),
        StructField("test2", StringType, nullable = true)
      )
      val testDataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(Row("data1", "data2"))),
        StructType(someSchema))
      val tempDir: String = config.getString("readPath")
      // written the batch data
      ORCWriter.write(testDataFrame, tempDir, SaveMode.Overwrite)
      // preparing test data ends here

      // Now reading batch data from path updated using updateBatchConfig method
      ORCReader.read(sparkSession, tempDir)
    }

    /**
     * To apply business logic on filtered source data(unit of work)
     *
     * @param config       Contains the current execution configuration
     * @param sparkSession Contains the current spark session information
     * @param dateFrame    DataFrame contains filtered source data(unit of work) based on logical unit of work
     * @return DataFrame contains sink data after applying business rules(transformation)
     */
    override def transformBatchData(config: Config, sparkSession: SparkSession, dateFrame: DataFrame): DataFrame = {

      // sparkSession: SparkSession: can be used for your internal operation like, fetch data from HDFS
      val newColumn = config.getString("testColumn")
      dateFrame.transform(_.withColumn(newColumn, lit("transformedTestColumn")))
    }

    /**
     * To persist sink dataframe to specified storage
     *
     * @param config          Contains the current execution configuration like storage location etc..
     * @param sparkSession    Contains the current spark session information
     * @param resultDataFrame DataFrame contains sink data after applying business rules(transformation)
     */
    override def loadBatchData(config: Config, sparkSession: SparkSession, resultDataFrame: DataFrame): Unit = {

      // sparkSession: SparkSession: can be used for your internal file related operation
      val writePath = config.getString("writePath")
      ORCWriter.write(dataFrame = resultDataFrame, writePath, SaveMode.Overwrite)
    }


    /**
     * Log information w.r.t each unit of work
     *
     * @param config          Contains the current execution configuration
     * @param sparkSession    Contains the current spark session information
     * @param row             Contains information related to each unit of work has been processed
     * @param sourceDataFrame Optional dataframe which contains filtered source data associated with each unit of work
     * @param sinkDataFrame   Optional dataframe which contains sink data associated with each unit of work
     * @param throwable       Optional throwable object in case of exception within the data processing flow for each unit of
     *                        work
     */
    override def logBatchAuditDetails(config: Config, sparkSession: SparkSession, row: Row,
                                      sourceDataFrame: Option[DataFrame], sinkDataFrame: Option[DataFrame],
                                      throwable: Option[Throwable]): Unit = {
      // While consuming this contract,
      // as we have source/sink dataframe, this method can be utilised for audit logging for the current unit of
      // work/batch, to any hdfs or any my-sql path whatever user wants.
    }

    /**
     * To apply post processing activity like <br>
     * - deletion of checkpoints <br>
     * - closing of resources etc .. <br>
     *
     * @param config       Contains the current execution configuration
     * @param sparkSession Contains the current spark session information
     * @param row          Contains information related to each unit of work has been processed
     *
     */
    override def postBatchProcessing(config: Config, sparkSession: SparkSession, row: Row): Unit = {
      // CleanUp activities can be performed in this contract w.r.t Job level
      // For example cleanup of checkPoint data, temporary files, etc.
    }

    /**
     * Log Job information for all processed unit of work
     *
     * @param config       Contains the current execution configuration
     * @param sparkSession Contains the current spark session information
     * @param throwable    Optional throwable object in case of exception within the data processing flow for each unit of
     *                     work
     */
    override def logJobAuditDetails(config: Config, sparkSession: SparkSession, throwable: Option[Throwable]): Unit = {
      // While consuming this contract,
      // as we have config object with all job information, this method can be utilised for audit logging for the current
      // job related information, to any hdfs or any my-sql path whatever user wants.
    }

  }


  override protected def afterAll(): Unit = {

    val scalaPath = Path(basePath)
    scalaPath.deleteRecursively()
  }

  "IncrementalETLJobUnitTest" should "be able to run all the batches from the work log created in overridden methods" in {
    var checkpoint = false

    val testIncrementalETLImpl = IncrementalETLImpl((a, _) => {
      println(s"Config: $a")
      assert(a.getString("logLevel") == "ERROR")
      checkpoint = true
    })

    testIncrementalETLImpl.main(Array("-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR",
      "-DlogicalBatchId=logical_batch_id", "-DtestColumn=dummyValue"))

    assert(checkpoint, "The checkpoint should be hit")
  }

  "IncrementalETLJobUnitTest" should " throw exception as logical_batch_id is not available in args which is required" +
    "in the implementation of getIncrementalWorkLog " in {
    val testIncrementalETLImpl = IncrementalETLImpl((_, _) => {})
    assertThrows[java.lang.Exception](
      testIncrementalETLImpl.main(Array("-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR"))
    )
  }
  "IncrementalETLJobUnitTest" should " do logBatchAuditDetails directly as testColumn is not available in args which is required in" +
    "the implementation of transformBatchData " in {
    var checkpoint = false
    val testIncrementalETLImpl = IncrementalETLImpl((_, _) => {
      checkpoint = true
    })
    testIncrementalETLImpl.main(Array("-DsparkOptions.spark.master=local[2]",
      "-DlogLevel=ERROR", "-DlogicalBatchId=logical_batch_id"))
    assert(checkpoint, "The checkpoint should be hit")
  }
}
