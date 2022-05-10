package com.spark.common.jobs

import com.spark.common.config.{JsonPropertiesLoader, SparkSessionInitializer}
import com.spark.common.utilities.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * This class is responsible for initialisation of all the configurations.
 * Creates and initialises the spark session.
 * Load configuration file and override it with run time JVM arguments.
 * Validates presence of any mandatory configuration parameter.
 * It contains the main method which gets executed from any subsequent job. 
 * Any Job can be created by extending this class.
 */
abstract class BaseJob {

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
    executeJob(config, sparkSession)

    sparkSession.stop()
  }

  /**
   * Every job needs to override this abstract method and provide business logic to operate on DataFrame
   *
   * @param config Contains the current execution configuration
   * @param spark  SparkSession provides access to spark context
   */
  def executeJob(config: Config, spark: SparkSession)

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
}
