package com.spark.common.config

import com.spark.common.exceptions.SparkInitialisationException
import com.typesafe.config.{Config, ConfigValue}
import com.spark.common.constant.ConfigConstants.{EMPTY_STRING, HADOOP_OPTIONS_PROPERTY_KEY, SPARK_OPTIONS_PROPERTY_KEY}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkSessionInitializer {

  /**
   * Creates spark session and return it using provided key value pair of spark configuration
   *
   * @param options Key value pair of spark configuration
   * @return
   */
  private def createSparkSession(options: Map[String, String]): SparkSession = {
    val sc: SparkConf = new SparkConf().setAll(options)
    SparkSession.builder.config(sc).getOrCreate()
  }


  /**
   * Creates Spark session based on configurations provided
   * Uses 'sparkOptions' and 'hadoopOptions' for configuration settings
   *
   * @throws SparkInitialisationException Exception indicating spark startup failure.
   * @param config : Config
   * @return Created SparkSession
   */
  def initializeSparkSession(config: Config): SparkSession = {
    try {
      val sparkOptions: Map[String, String] =
        if (config.hasPath(SPARK_OPTIONS_PROPERTY_KEY)) {
          val sparkOptionsConfig: Config = config.getConfig(SPARK_OPTIONS_PROPERTY_KEY)
          val entrySet = sparkOptionsConfig.entrySet().asScala
          entrySet.flatMap {
            entry: java.util.Map.Entry[String, ConfigValue] =>
              Map[String, String](entry.getKey -> entry.getValue.render().replace("\"", EMPTY_STRING))
          }.toMap
        } else {
          Map.empty[String, String]
        }
      val spark = SparkSessionInitializer.createSparkSession(sparkOptions)
      spark.sparkContext.setLogLevel(config.getString("logLevel"))

      //Hadoop file system property initialization starts
      if (config.hasPath(HADOOP_OPTIONS_PROPERTY_KEY)) {
        val hadoopOptionsConfig: Config = config.getConfig(HADOOP_OPTIONS_PROPERTY_KEY)
        val hadoopOptionsEntrySet = hadoopOptionsConfig.entrySet().asScala
        hadoopOptionsEntrySet.foreach {
          eachEntry: java.util.Map.Entry[String, ConfigValue] =>
            spark.sparkContext.hadoopConfiguration.set(eachEntry.getKey, eachEntry.getValue.render().replace("\"", EMPTY_STRING))
        }
      }
      //Hadoop file system property initialization ends
      spark
    } catch {
      case e: Exception =>
        throw SparkInitialisationException("Failed to initialize spark", e)
    }
  }
}
