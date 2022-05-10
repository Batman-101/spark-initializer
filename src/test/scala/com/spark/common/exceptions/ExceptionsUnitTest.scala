package com.spark.common.exceptions

import com.spark.common.exceptions.SparkInitialisationException
import com.spark.common.exceptions.data.{DataIntegrityException, NoDataFoundException, OneToManyDataMappingException, OneToNoneDataMappingException}
import com.spark.common.exceptions.job.JobFailureException
import org.scalatest.flatspec.AnyFlatSpec

class ExceptionsUnitTest extends AnyFlatSpec {

  "DataIntegrityException" should "have a message and an optional internal throwable" in {

    val testException = DataIntegrityException("This is a test exception", None)
    assert(testException.message == "This is a test exception")
    assert(testException.cause.isEmpty)

  }

  "NoDataFoundException" should "have a message and an optional internal throwable" in {

    val testException = NoDataFoundException("This is a test exception", None)
    assert(testException.message == "This is a test exception")
    assert(testException.cause.isEmpty)
  }

  "OneToManyDataMappingException" should "have a message and an optional internal throwable" in {

    val testException = OneToManyDataMappingException("This is a test exception", None)
    assert(testException.message == "This is a test exception")
    assert(testException.cause.isEmpty)

  }

  "OneToNoneDataMappingException" should "have a message and an optional internal throwable" in {

    val testException = OneToNoneDataMappingException("This is a test exception", None)
    assert(testException.message == "This is a test exception")
    assert(testException.cause.isEmpty)

  }

  "JobFailureException" should "have a message and an optional internal throwable" in {

    val testException = JobFailureException("This is a test exception", new Throwable("Job got crashed"))
    assert(testException.message == "This is a test exception")
    assert(testException.exception.getMessage == "Job got crashed")
  }

  "SparkInitializationException" should "have a message and an optional internal throwable" in {

    val testException = SparkInitialisationException("This is a test exception", new Throwable("Blam, oni crashed"))
    assert(testException.message == "This is a test exception")
    assert(testException.exception.getMessage == "Blam, oni crashed")
  }

}
