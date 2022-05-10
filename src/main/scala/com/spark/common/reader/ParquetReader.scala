package com.spark.common.reader

/** *
 * Read Operation of the PARQUET file using SparkSession
 */

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetReader {
  /**
   * Utility to read Parquet and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param options      : This is a map of options
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).parquet(paths:_*)
  }

  /**
   * Utility to read Parquet and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param paths         : This is the path(s) from which the file(s) need to be read
   * @return (Dataframe) : The dataframe from the given file
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    ParquetReader.read(sparkSession, Map[String, String](), paths:_*)
  }
}
