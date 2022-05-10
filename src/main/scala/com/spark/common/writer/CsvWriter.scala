package com.spark.common.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object CsvWriter {
  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame   Spark Data Frame
   * @param path        Folder Path
   * @param partitionBy partitioning Sequence
   * @param options     key value pair of
   * @param saveMode    Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).csv(path)
  }

  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame Spark Data Frame
   * @param path      Folder Path
   * @param options   key value pair of
   * @param saveMode  Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).csv(path)
  }

  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame Spark Data Frame
   * @param path      Folder Path
   * @param saveMode  Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    CsvWriter.write(dataFrame, path, Map[String,String](), saveMode)
  }

}
