package com.spark.common.config

import com.spark.common.config.JsonPropertiesLoader
import com.spark.common.utilities.ConfigUtils
import com.typesafe.config.{Config, ConfigFactory}
import com.spark.common.constant.ConfigConstants.CONFIG_FILE_PROPERTY_KEY
import org.scalatest.flatspec.AnyFlatSpec

class JsonPropertiesLoaderUnitTest extends AnyFlatSpec {

  "JsonPropertiesLoaderUnitTest" should "be able to read a specified config" in {
    val result = JsonPropertiesLoader.loadConfig("localtest.conf")
    assert(result.getString("logLevel") == "ERROR")
  }

  it should "be able to find a config given an input map" in {
    val result = JsonPropertiesLoader.getOrCreateConfig(Map(CONFIG_FILE_PROPERTY_KEY -> "localtest.conf"))
    assert(result.getString("logLevel") == "ERROR")
  }
  it should "return a empty config object as the wrong key for 'configFile' has been provided" in {
    val result = JsonPropertiesLoader.getConfigUsingParseResource(Map.empty)
    assert(result.isEmpty)
  }

  it should "be able to find a config given an input map for substitute config" in {
      val result = JsonPropertiesLoader.getConfigUsingParseResource(Map(CONFIG_FILE_PROPERTY_KEY -> "substitute.conf",
        "logLevelValue" -> "DEBUG", "container" -> "spark", "storageAccount" ->  "server" )).resolve()
      assert(result.getString("logLevel") == "DEBUG")
      assert(result.getString("defaultFileSystem") == "abfss://spark@server.dfs.core.windows.net")
    }

  it should "add properties from a map into a config" in {
    val properties = Map("food" -> "bananas")
    val result = JsonPropertiesLoader.insertProperties(ConfigFactory.load("localtest.conf"), properties)
    assert(result.getString("food") == "bananas")
  }

  it should "filter properties from a Array into a config when runtime override param is present" in {
    val properties = Array("-Dfood=bananas", "-Ddrink=coke")
    val paramName = Set("food")
    val result = JsonPropertiesLoader.filterRuntimeConfigsKeys(properties, paramName)
    assert(result.contains("-Dfood=bananas") equals result.contains("-Ddrink=coke") equals false)
  }

  it should "filter properties from a Array into a config when runtime override param is empty" in {
    val properties = Array("-Dfood=bananas", "-Ddrink=coke")
    val paramName: Set[String] = Set()
    val result = JsonPropertiesLoader.filterRuntimeConfigsKeys(properties, paramName)
    assert(result.contains("-Dfood=bananas") equals result.contains("-Ddrink=coke") equals true)
  }

  it should "filter properties from a Array into a config when runtime override is present but filter properties is empty" in {
    val properties = Array.empty[String]
    val paramName: Set[String] = Set("food")
    val result = JsonPropertiesLoader.filterRuntimeConfigsKeys(properties, paramName)
    assert(result.isEmpty equals true)
  }

  it should "filter properties from a Array into a config when both runtime override and filter properties is empty" in {
    val properties = Array.empty[String]
    val paramName: Set[String] = Set()
    val result = JsonPropertiesLoader.filterRuntimeConfigsKeys(properties, paramName)
    assert(result.isEmpty equals true)
  }

  it should "get Configuration method call" in {
    val args: Array[String] = Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf", "-DlogLevelValue=DEBUG",
      "-Dcontainer=spark", "-DstorageAccount=server", "-DlogLevel=ERROR")
    val result: Config = ConfigUtils.getConfiguration(args)
    assert(result.getString("logLevelValue") == "DEBUG" && result.getString("container") == "spark" &&
      result.getString("storageAccount") == "server" && result.getString("logLevel") == "ERROR")
  }
}
