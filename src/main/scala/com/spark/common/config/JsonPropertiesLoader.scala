package com.spark.common.config

import com.typesafe.config._
import com.spark.common.constant.ConfigConstants.{CONFIG_FILE_PROPERTY_KEY, EMPTY_STRING}

object JsonPropertiesLoader {

  /**
   * Load the config with the name as defined by configFileName.
   * Will load reference.conf first, then load the config file on top of it.
   *
   * @param configFileName The name of the config file to be loaded
   * @return The read config.
   */
  def loadConfig(configFileName: String): Config = ConfigFactory
    .load(configFileName, ConfigParseOptions.defaults(), ConfigResolveOptions.defaults().setAllowUnresolved(true))

  /**
   * It will only load the configuration which has been provided into the run time config
   * unlike #load which loads the default config from reference.conf.
   *
   * @param propertyFileName String
   * @return the parsed configuration
   */
  def parseResources(propertyFileName: String): Config = ConfigFactory.parseResources(propertyFileName)

  /**
   * Create or get a Config object based on
   * passed properties
   *
   * @param properties : Map[String, String]
   * @return Config with properties added
   */
  def getOrCreateConfig(properties: Map[String, String]): Config = {

    properties.get(CONFIG_FILE_PROPERTY_KEY)
      .map(configFileName => loadConfig(configFileName))
      .getOrElse(ConfigFactory.empty())
  }

  /**
   * Create or get a Config object based on config file and runtime program argument(properties: Map[String, String])
   * It uses #parseResources that will only load the configuration associated with run time config file
   * unlike #load which loads the default config from reference.conf.
   *
   * Substitutions: The loaded object will not be resolved (substitutions have not been
   * processed). As a result, you will be able to add more fallbacks
   * and once you call the resolve on result, all substitutions will be seen.
   * Substitutions are the "${foo.bar}" syntax.
   * Example: getConfigUsingParseResource(properties: Map[String, String]).resolve()
   *
   * @param properties : Map[String, String]
   * @return Config with properties added
   */
  def getConfigUsingParseResource(properties: Map[String, String]): Config = {

    val config = properties.get(CONFIG_FILE_PROPERTY_KEY)
      .map(configFileName => parseResources(configFileName))
      .getOrElse(ConfigFactory.empty())

    insertProperties(config, properties)
  }

  /**
   * Insert properties into a config, keeping them at the level of the property key
   *
   * @param config     The config to which we should insert the properties
   * @param properties The properties, KVP, that get inserted into the config
   * @return
   */
  def insertProperties(config: Config, properties: Map[String, String]): Config = {
    properties.foldLeft(config) {
      (result: Config, current: (String, String)) =>
        result.withValue(current._1, ConfigValueFactory.fromAnyRef(current._2))
    }
  }

  /**
   * It gets a list of keys to filter during the runtime and will only allow those key-values to gets overriden.
   *
   * @param args      The properties, KVP, that get inserted into the config
   * @param paramName The list of keys from the config
   * @return Returns the config and filtered Runtime Parameters
   */
  def filterRuntimeConfigsKeys(args: Array[String], paramName: Set[String]): Array[String] = {
    if (paramName.isEmpty)
      args
    else {
      args.filter { (arg: String) =>
        val splitIndex: Int = arg.indexOf("=")
        val keyVal: Array[String] = arg.split("=")
        keyVal.length > 1 && paramName.contains(arg.slice(0, splitIndex).replace("-D", EMPTY_STRING))
      }
    }
  }
}
