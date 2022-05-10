package com.spark.common.exceptions

/**
 * Use this exception when there is an issue with the job
 *
 * @param message   exception description
 * @param exception inherent exception which cause this exception
 */
class DataException(message: String, exception: Throwable) extends Exception(message, exception)
