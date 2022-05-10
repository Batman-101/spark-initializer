package com.spark.common.utilities

import com.typesafe.config.Config
import com.spark.common.constant.ConfigConstants.EMPTY_STRING

object JobUtils {

  /**
   * Parse JVM run time arguments
   *
   * @param args Contains JVM run time arguments
   * @return Map [JVM run time parameter name, JVM run time parameter value]
   */
  def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap(arg => {
      // find first index of =
      val splitIndex = arg.indexOf("=")
      val keyVal = arg.split("=")
      if (keyVal.size > 1) {
        Map(arg.slice(0, splitIndex).replace("-D", EMPTY_STRING) -> arg.slice(splitIndex + 1, arg.length))
      } else {
        Map.empty[String, String]
      }
    }).toMap
  }

  /**
   * Get any '-D' arguments from the command line via the config.
   * Could be useful in situations where the args are ugly.
   * Also remember you can add environment variables here too!
   *
   * @param config the currently running config
   * @return Map of Keys and Values for the arguments.
   */
  def getArgumentsFromConfig(config: Config): Map[String, String] = {
    config.getString("java.command")
      .split(" ")
      .filter(x => x.startsWith("-D"))
      .map(x => {
        val split: Array[String] = x.split("=")
        split(0).replace("-D", EMPTY_STRING) -> split(1)
      })
      .toMap
  }
}
