package com.spark.common.config

import com.spark.common.config.SparkSessionInitializer
import com.spark.common.exceptions.SparkInitialisationException
import com.typesafe.config.ConfigFactory
import com.spark.common.constant.ConfigConstants.{HADOOP_OPTIONS_PROPERTY_KEY, SPARK_OPTIONS_PROPERTY_KEY}
import org.scalatest.flatspec.AnyFlatSpec

class SparkSessionInitializerUnitTest extends AnyFlatSpec {

  "SparkSessionInitializer" should "create a spark session" in {
    val session = SparkSessionInitializer.initializeSparkSession(ConfigFactory.load("localtest.conf"))

    val total = session.range(1000).reduce((a, b) => Long.box(a + b))

    assert(total == 499500)

    session.stop()
  }

  it should "create a blank config if sparkOptions section does not exist" in {
    try {
      SparkSessionInitializer.initializeSparkSession(
        ConfigFactory.load("localtest.conf")
          .withoutPath(SPARK_OPTIONS_PROPERTY_KEY))

      assert(false, "Should not get to this point!")
    } catch {
      case e: SparkInitialisationException =>
        assert(e.exception.getMessage == "A master URL must be set in your configuration")
      case e: Throwable =>
        assert(false, s"This is the wrong message: ${e.getMessage}")
    }

  }

  it should "create a blank config if hadoopOptions section does not exist. Default size with config is 482 items" in {
    val spark = SparkSessionInitializer.initializeSparkSession(
      ConfigFactory.load("localtest.conf")
        .withoutPath(HADOOP_OPTIONS_PROPERTY_KEY))

    assert(spark.sparkContext.hadoopConfiguration.asScala.size == 662)

    spark.stop()
  }

}
