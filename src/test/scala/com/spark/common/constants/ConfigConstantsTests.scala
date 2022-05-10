package com.spark.common.constants

import com.spark.common.constant.ConfigConstants._
import org.scalatest.flatspec.AnyFlatSpec

class ConfigConstantsTests extends AnyFlatSpec {

  "ConfigConstants object" should " return asserted values " in {
    assert(CONFIG_FILE_PROPERTY_KEY.equals("configFile"))
    assert(ENVIRONMENT_PROPERTY_KEY.equals("environment"))
    assert(SPARK_OPTIONS_PROPERTY_KEY.equals("sparkOptions"))
    assert(HADOOP_OPTIONS_PROPERTY_KEY.equals("hadoopOptions"))
    assert(INSTANCE_UUID_PROPERTY_KEY.equals("instanceUUID"))
    assert(INSTANCE_UUID_PROPERTY_KEY.equals("instanceUUID"))
    assert(GENERATE_INSTANCE_UUID_FLAG.equals("generateInstanceUUID"))
    assert(COMMA_SEPARATOR.equals(","))
    assert(EMPTY_STRING.equals(""))
  }

}
