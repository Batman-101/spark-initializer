package com.spark.common.exceptions.data

import com.spark.common.exceptions.DataException

/**
 * Use this exception in case if the no records available for a given query
 *
 * @param message exception description
 * @param cause inherent exception which cause this exception
 */
case class NoDataFoundException(message: String, cause: Option[Throwable] = None) extends DataException(message, cause.orNull)
