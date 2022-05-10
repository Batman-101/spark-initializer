package com.spark.common.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object TextReader {

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame.
   *
   * @param sparkSession : This is the Spark session
   * @param options      : This is a map of options
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).text(paths:_*)
  }

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame.
   *
   * @param sparkSession : This is the Spark session
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    TextReader.read(sparkSession, Map[String, String](), paths:_*)
  }
}
