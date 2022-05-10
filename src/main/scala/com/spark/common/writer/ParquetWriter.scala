package com.spark.common.writer

/***
 * Write Operation of the PARQUET file using SparkSession
 * Utilities
 */
import org.apache.spark.sql.{DataFrame, SaveMode}

object ParquetWriter {

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
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).parquet(path)
  }

  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame   Spark Data Frame
   * @param path        Folder Path
   * @param options     key value pair of
   * @param saveMode    Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).parquet(path)

  }

  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame   Spark Data Frame
   * @param path        Folder Path
   * @param saveMode    Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    ParquetWriter.write(dataFrame, path, Map[String,String](), saveMode)
  }

}
