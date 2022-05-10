package com.spark.common.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object TextWriter {
  /**
   * Saves the content of the `DataFrame` in a text file at the specified path.
   * <p>The Dataframe can have one or more column with condition,
   *                    <br>partition by columns = total number of columns in a dataframe  - 1  <p>
   * It means data being persisted by text writer in hdfs file will have only one column of String type
   *
   * After persistence while reading, the retrieved data column will be named as 'value' of string type.
   *
   * Each row becomes a new line in the output file.
   *
   * @param dataFrame   Spark Data Frame
   * @param path        Folder Path
   * @param partitionBy partitioning Sequence
   * @param options     key value pair of
   * @param saveMode    Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).text(path)
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file.
   *
   * @param dataFrame Spark Data Frame
   * @param path      Folder Path
   * @param options   key value pair of
   * @param saveMode  Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).text(path)
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file.
   *
   * @param dataFrame Spark Data Frame
   * @param path      Folder Path
   * @param saveMode  Save mode of DataFrame to a data source
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    TextWriter.write(dataFrame, path, Map[String, String](), saveMode)
  }

}
