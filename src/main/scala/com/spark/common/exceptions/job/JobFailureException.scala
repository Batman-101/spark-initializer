package com.spark.common.exceptions.job

/**
 * Use this exception when there is an issue with the job
 *
 * @param message   exception description
 * @param exception inherent exception which cause this exception
 */
case class JobFailureException(message: String, exception: Throwable) extends Exception(message, exception)
