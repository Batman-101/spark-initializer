package com.spark.common.utilities

import com.spark.common.config.JsonPropertiesLoader
import com.spark.common.constant.ConfigConstants

import java.util.UUID
import com.typesafe.config.{Config, ConfigValueFactory}
import JsonPropertiesLoader.insertProperties
import ConfigConstants._
import org.apache.hadoop.mapred.InvalidJobConfException

object ConfigUtils {
  /**
   * Initialises configuration, validates presence of mandatory configuration parameters
   *
   * @param properties Contains JVM run time arguments
   * @return Validated config with arguments added
   */
  def initConfig(properties: Map[String, String]): Config = {
    JsonPropertiesLoader.getOrCreateConfig(properties)
  }

  /**
   * Validates presence of any mandatory configuration parameter.
   * Concrete class implementing BaseJob overrides getJobSpecificMandatoryConfigKeys method & provides mandatory
   * configuration parameter.
   * Throws exception if mandatory parameter not available in configuration.
   */
  def validateMandatoryConfig(config: Config, mandatoryConfigKeys: Set[String]): Unit = {
    mandatoryConfigKeys.foreach(key => {
      if (!config.hasPath(key)) {
        throw new InvalidJobConfException(s"Mandatory config $key not found")
      }
    })
  }

  /**
   * Uses runtime arguments to create Configuration object.
   * It uses tags with precedence given below
   * 1) configFile
   * 2) environment
   * 3) command line arguments
   *
   * @param args Array[String] : Contains JVM run time arguments
   * @return Config
   */
  def getConfiguration(args: Array[String]): Config = {
    val properties = JobUtils.parseArgs(args)
    val initialConfig = ConfigUtils.initConfig(properties)
    val configWithEnvironmentIfExists = addEnvironmentConfigIfExists(initialConfig, properties)
    val config = addRandomUuidOnDemand(configWithEnvironmentIfExists)
    insertProperties(config, properties).resolve()
  }

  /**
   * To add properties into existing configuration if exists at run time
   *
   * @param config     Existing configuration on which user wants to add new properties
   * @param properties the new key value pair which contains configuration file name to be added
   * @return Config: Modified config instance
   */
  final def addEnvironmentConfigIfExists(config: Config, properties: Map[String, String]): Config = {
    properties.get(ConfigConstants.ENVIRONMENT_PROPERTY_KEY).map {
      env: String => {
        val envFiles: Array[String] = env.split(COMMA_SEPARATOR)
        envFiles.foldLeft(config) {
          (result: Config, envFile: String) => JsonPropertiesLoader.parseResources(envFile.trim).withFallback(result)
        }
      }
    }.getOrElse(config)
  }

  /**
   * To add unique id(as instanceUUID), auto generated with every config instance.
   *
   * @param config configuration instance in which instanceUUID will be added
   * @return Config: Modified config instance
   */
  def addRandomUuidOnDemand(config: Config): Config = {
    val configWithInstanceUuid = if (config.hasPath(GENERATE_INSTANCE_UUID_FLAG) && config.getBoolean(GENERATE_INSTANCE_UUID_FLAG)) {
      config.withValue(INSTANCE_UUID_PROPERTY_KEY, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString))
    } else {
      config
    }
    addUUIDsForGenerateUuidTag(configWithInstanceUuid)
  }

  /**
   * Tag `generateUUID` to be added into the conf file in array format to enable this feature.
   *
   * Example: generateUUID = ["eventId", "jobId", "etc1", "etc2" ... ]
   *
   * Result: Job config object will have key as `eventId` and respective generated UUID. Same applies for `jobId` etc.
   *
   * @param config configuration object in which generateUUID's tag(with generated UUID value) will be added
   * @return Config: Modified config instance
   */
  def addUUIDsForGenerateUuidTag(config: Config): Config = {
    if (config.hasPath(GENERATE_UUID) && !config.getStringList(GENERATE_UUID).isEmpty) {
      config.getStringList(GENERATE_UUID).asScala.foldLeft(config) {
        (result: Config, eachUuidTag: String) =>
          result.withValue(eachUuidTag, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString))
      }
    } else {
      config
    }
  }
}
