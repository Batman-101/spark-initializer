package com.spark.common.exceptions.data

import com.spark.common.exceptions.DataException

/**
 * Use this exception in case of violation of integrity constraint(business constraint violation)
 *
 * @param message exception description
 * @param cause inherent exception which cause this exception
 */
case class DataIntegrityException(message: String, cause: Option[Throwable] = None) extends DataException(message, cause.orNull)
