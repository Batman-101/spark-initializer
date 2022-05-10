package com.spark.common.exceptions.data

import com.spark.common.exceptions.DataException

/**
 * Use this exception when one row in a table has no mapping corresponding to another table with
 * joining criteria.
 *
 * @param message exception description
 * @param cause inherent exception which cause this exception
 */
case class OneToNoneDataMappingException(message: String, cause: Option[Throwable] = None) extends DataException(message, cause.orNull)
