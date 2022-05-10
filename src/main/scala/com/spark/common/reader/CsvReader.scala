package com.spark.common.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader {
  /**
   * Utility to read Csv and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param options      : This is a map of options
   * @param paths         : This is the path(s) from which the file(s) need to be read,
   * @return (Dataframe) : The dataframe from the given path,
   *         as per spark if no path provided empty data frame will be returned
   */
  def read(sparkSession: SparkSession, schema: StructType, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.schema(schema).options(options).csv(paths:_*)
  }

  /**
   * Utility to read Csv and return DataFrame
   *
   * @param sparkSession : This is the Spark session
   * @param paths         : This is the path(s) from which the file(s) need to be read,
   * @return (Dataframe) : The dataframe from the given path,
   *         as per spark if no path provided empty data frame will be returned
   */
  def read(sparkSession: SparkSession, schema: StructType, paths: String*): DataFrame = {
    CsvReader.read(sparkSession, schema, Map[String, String](), paths:_*)
  }
}
