package com.spark.common.jobs

import com.spark.common.config.{JsonPropertiesLoader, SparkSessionInitializer}
import com.spark.common.utilities.ConfigUtils
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}


abstract class IncrementalETLJob extends LazyLogging {

  /**
   * Specify mandatory configuration parameter
   *
   * @return Set of keys that are required in a config
   */
  def getJobSpecificMandatoryConfigKeys: Set[String] = Set()

  /**
   * Specify configuration parameters to filter at Runtime
   *
   * @return Set of keys to filter parameters at runtime
   */
  def getConfigKeysToOverride: Set[String] = Set()

  /**
   * To incremental work log details from audit table.
   *
   * @param config       Contains the current execution configuration
   * @param sparkSession Contains the current spark session information
   * @return DataFrame: Contains unit of work to be processed by the Job.
   */
  def getIncrementalWorkLog(config: Config, sparkSession: SparkSession): DataFrame

  /**
   *
   * To modify work log i.e. breaking 1 record to N records, OR N records into 1 record
   * based on how many batches you want to process.
   *
   * For example: if N record in the incremental work log is having the same source path and
   * we can process all those work log as part of one batch then we could merge N records into 1.
   * <pre>
   * 1. Merge
   * Suppose work log has below details
   * {
   * { "readPath": "path/to/source1", "partitionBy": "20210503"},
   * { "readPath": "path/to/source1", "partitionBy": "20210504"},
   * { "readPath": "path/to/source2", "partitionBy": "20210506"},
   * { "readPath": "path/to/source2", "partitionBy": "20210507"}
   * }
   *
   * could be merge into
   * {
   * { "readPath": ""path/to/source1" },
   * { "readPath": ""path/to/source2" }
   * }
   *
   * 2. Diverge
   * Suppose work log has below details
   * {
   * { "readPath": "path/to/source1", "partitionBy": "20210503"},
   * { "readPath": "path/to/source2", "partitionBy": "20210507"}
   * }
   * could be diverge into
   * {
   * { "readPath": "path/to/source1", "partitionBy": "2021050300" },
   * { "readPath": "path/to/source1", "partitionBy": "2021050306" },
   * { "readPath": "path/to/source2", "partitionBy": "2021050712" },
   * { "readPath": "path/to/source2", "partitionBy": "2021050718" }
   * }
   * </pre>
   *
   * @param config       Contains the current execution configuration
   * @param dataFrame    DataFrame:  Contains unit of work to be processed by the Job.
   * @param sparkSession Contains the current spark session information
   * @return DataFrame : Contains modified unit of work to be processed by the Job.
   */
  def transformIncrementalWorkLog(config: Config, sparkSession: SparkSession, dataFrame: DataFrame): DataFrame
  = dataFrame

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
  def updateBatchConfig(config: Config, sparkSession: SparkSession, row: Row): Config

  /**
   * Specify mandatory configuration parameter w.r.t to each unit of work
   * unit of work can be a batch.
   *
   * @return Set of keys that are required in a config
   */
  def getBatchSpecificMandatoryConfigKeys: Set[String] = Set()

  /**
   * To read the source data applicable for each unit of work
   *
   * @param config       Contains the current execution configuration like source location etc..
   * @param sparkSession Contains the current spark session information
   * @return DataFrame contains source data associated with each unit of work
   */
  def readBatchData(config: Config, sparkSession: SparkSession): DataFrame

  /**
   * To filter the source data(unit of work) based on logical unit of work
   *
   * row: Row contains filtering criteria for source data
   *
   * Example: filter by partition key
   *
   * @param config       Contains the current execution configuration
   * @param sparkSession Contains the current spark session information
   * @param row          Contains information(eg: filtering criteria etc..) of each unit of work to be processed
   * @param dateFrame    DataFrame contains source data associated with each unit of work
   * @return DataFrame contains filtered source data(unit of work) based on logical unit of work
   */
  def filterBatchData(config: Config, sparkSession: SparkSession, row: Row, dateFrame: DataFrame): DataFrame = dateFrame

  /**
   * To apply business logic on filtered source data(unit of work)
   *
   * @param config       Contains the current execution configuration
   * @param sparkSession Contains the current spark session information
   * @param dateFrame    DataFrame contains filtered source data(unit of work) based on logical unit of work
   * @return DataFrame contains sink data after applying business rules(transformation)
   */
  def transformBatchData(config: Config, sparkSession: SparkSession, dateFrame: DataFrame): DataFrame

  /**
   * To persist sink dataframe to specified storage
   *
   * @param config          Contains the current execution configuration like storage location etc..
   * @param sparkSession    Contains the current spark session information
   * @param resultDataFrame DataFrame contains sink data after applying business rules(transformation)
   */
  def loadBatchData(config: Config, sparkSession: SparkSession, resultDataFrame: DataFrame): Unit

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
  def logBatchAuditDetails(config: Config, sparkSession: SparkSession, row: Row, sourceDataFrame: Option[DataFrame],
                           sinkDataFrame: Option[DataFrame], throwable: Option[Throwable]): Unit

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
  def postBatchProcessing(config: Config, sparkSession: SparkSession, row: Row): Unit = {
    //This is a default implementation and consumer can provide
    // the custom implementation in case of any post processing required after batch/job
  }

  /**
   * Log Job information for all processed unit of work
   *
   * @param config       Contains the current execution configuration
   * @param sparkSession Contains the current spark session information
   * @param throwable    Optional throwable object in case of exception within the data processing flow for each unit of
   *                     work
   */
  def logJobAuditDetails(config: Config, sparkSession: SparkSession, throwable: Option[Throwable])

  /**
   * Starts job execution after initialising config and SparkSession
   *
   * @param args Contains JVM run time arguments
   */
  def main(args: Array[String]): Unit = {
    val filteredRuntimeParameters: Array[String] = JsonPropertiesLoader
      .filterRuntimeConfigsKeys(args, getConfigKeysToOverride)
    val config: Config = ConfigUtils.getConfiguration(filteredRuntimeParameters)
    ConfigUtils.validateMandatoryConfig(config, getJobSpecificMandatoryConfigKeys)
    val sparkSession = SparkSessionInitializer.initializeSparkSession(config)
    logger.info("Config object and sparkSession initialization completed")
    Try {
      val deltaAuditDataFrame = getIncrementalWorkLog(config, sparkSession)
      val enrichDeltaAuditDataFrame = transformIncrementalWorkLog(config, sparkSession, deltaAuditDataFrame)
      logger.info("Fetching and enriching worklog completed")

      val workLog = enrichDeltaAuditDataFrame.collect()

      if (workLog.isEmpty)
        logger.info("Worklog is empty. Job will not proceed!!")

      workLog.foreach(row => {
        extractAndProcessBatch(row, config, sparkSession)
      })
    }
    match {
      case Success(_) =>
        logger.info("Processing of Job is completed successfully!")
        logJobAuditDetails(config, sparkSession, None)
        logger.info("Job run table is populated successfully!")
      case Failure(exception) =>
        logger.error("There was an exception in processing the job. Job will be aborted!", exception)
        logJobAuditDetails(config, sparkSession, Some(exception))
        throw exception
    }
    sparkSession.stop()
  }

  def extractAndProcessBatch(row: Row, config: Config, sparkSession: SparkSession): Unit = {
    Try {
      //This section is for extracting the batch data by updating config, reading and finally filtering current batch
      val updatedConfig = updateBatchConfig(config, sparkSession, row)
      logger.info("Batch config update completed!")

      ConfigUtils.validateMandatoryConfig(updatedConfig, getBatchSpecificMandatoryConfigKeys)

      val dataFrame: DataFrame = readBatchData(updatedConfig, sparkSession)
      logger.info("Batch data read from partition completed!")

      val filteredSourceDataFrame = filterBatchData(updatedConfig, sparkSession, row, dataFrame)
      logger.info("Batch data filter completed!")

      (updatedConfig, filteredSourceDataFrame)
    }
    match {
      case Success(result) =>
        processBatch(row, sparkSession, result)
      case Failure(exception) =>
        logger.warn("There was an exception in processing the batch!", exception)
        logBatchAuditDetails(config, sparkSession, row, None, None, Some(exception))
        logger.info("Audit tables are populated successfully!")
    }
  }

  private def processBatch(row: Row, sparkSession: SparkSession, result: (Config, DataFrame)): Unit = {
    val (updatedConfig, filteredSourceDataFrame) = (result._1, result._2)
    try {
      //This section is for processing already extracted data from previous step
      val sinkDataFrame = transformBatchData(updatedConfig, sparkSession, filteredSourceDataFrame)
      logger.info("Batch data transformation completed!")

      loadBatchData(updatedConfig, sparkSession, sinkDataFrame)
      logger.info("Processed batch data write completed!")

      logBatchAuditDetails(updatedConfig, sparkSession, row, Some(filteredSourceDataFrame), Some(sinkDataFrame), None)
      logger.info("Audit tables are populated successfully!")

    }
    catch {
      case exception: Throwable =>  logger.warn("There was an exception in processing the batch!", exception)
        logBatchAuditDetails(updatedConfig, sparkSession, row, Some(filteredSourceDataFrame), None, Some(exception))
    }
    finally {
      logger.info("Post batch processing started!")
      postBatchProcessing(updatedConfig, sparkSession, row)
    }
  }
}
