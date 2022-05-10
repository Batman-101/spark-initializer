package com.spark.common.reader

/**
 * Read Operation of the JSON file using SparkSession
 */

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonReader {

  /**
   * Utility to read JSON and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param options      : This is a map of options
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).json(paths:_*)
  }

  /**
   * Utility to read JSON and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    JsonReader.read(sparkSession, Map[String, String](), paths:_*)
  }
}
