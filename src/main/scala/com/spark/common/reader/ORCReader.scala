package com.spark.common.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object ORCReader {
  /**
   * Utility to read ORC and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param options      : This is a map of options
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).orc(paths:_*)
  }

  /**
   * Utility to read OCR and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    ORCReader.read(sparkSession, Map[String, String](), paths:_*)
  }
}
