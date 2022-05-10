package com.spark.common.exceptions

/**
 * Use this exception when spark session object does not get initialised.
 *
 * @param message exception description
 * @param exception inherent exception which cause this exception
 */
case class SparkInitialisationException(message: String, exception: Throwable) extends Exception(message, exception)
