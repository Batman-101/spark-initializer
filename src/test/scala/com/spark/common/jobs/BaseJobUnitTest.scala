package com.spark.common.jobs

import com.spark.common.constant.ConfigConstants
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import ConfigConstants.EMPTY_STRING
import com.spark.common.jobs.BaseJob
import com.spark.common.utilities.{ConfigUtils, JobUtils}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class BaseJobUnitTest extends AnyFlatSpec {

  case class BaseJobTestImpl(jobFun: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFun(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("configFile", "environment", "sparkOptions.spark.master", "logLevel", "container", "logLevelValue", "storageAccount")
    }
  }

  "BaseJob" should "be able to accept arguments passed in through main" in {
    var checkpoint = false

    val testBaseJobImpl = BaseJobTestImpl((a, _) => {
      println(s"Config: $a")
      assert(a.getString("logLevel") == "ERROR")
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR"))

    assert(checkpoint, "The checkpoint should be hit")
  }

  it should "be able to read a config file passed in through arguments" in {
    var checkpoint = false

    val testBaseJobImpl = BaseJobTestImpl((a, _) => {
      println(s"Config: $a")
      assert(a.getString("logLevel") == "ERROR")
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=localtest.conf"))

    assert(checkpoint, "The checkpoint should be hit")
  }

  // Positive: config object should have generated uuid as generateInstanceUUID flag been passed in localtest.conf.
  it should "be able to read a config instanceUUID passed in through config file flag as 'generateInstanceUUID' " in {
    var checkpoint = false
    val testBaseJobImpl = BaseJobTestImpl((a: Config, _) => {
      assert(a.getString(ConfigConstants.INSTANCE_UUID_PROPERTY_KEY).nonEmpty)
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=localtest.conf"))
    assert(checkpoint, "The checkpoint should be hit")
  }

  // Negative: config object should not have generated uuid as generateInstanceUUID flag been not passed in dummyEnv.conf.
  it should "not be able to read a config instanceUUID, as it  passed in through config file flag as 'generateInstanceUUID'" in {
    var checkpoint = false
    val testBaseJobImpl = BaseJobTestImpl((a: Config, _) => {
      assertThrows[ConfigException](a.getString(ConfigConstants.INSTANCE_UUID_PROPERTY_KEY))
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=dummyEnv.conf"))
    assert(checkpoint, "The checkpoint should be hit")
  }

  // Positive: config object should have generated uuid for eventId and someOtherId  as generateUUID flag been passed in localtest.conf.
  it should "be able to read a config eventId and someOtherId  passed in through config file flag as 'generateUUID' " in {
    var checkpoint = false
    val testBaseJobImpl = BaseJobTestImpl((a: Config, _) => {
      assert(a.getString("eventId").nonEmpty)
      assert(a.getString("someOtherId").nonEmpty)
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=localtest.conf"))
    assert(checkpoint, "The checkpoint should be hit")
  }

  // Negative: config object should not have generated uuid for eventId and someOtherId  as generateUUID flag been not passed in dummyEnv.conf.
  it should "not be able to read a config eventId and someOtherId as it passed in through config file flag as 'generateUUID'" in {
    var checkpoint = false
    val testBaseJobImpl = BaseJobTestImpl((a: Config, _) => {
      assertThrows[ConfigException](a.getString("eventId"))
      assertThrows[ConfigException](a.getString("someOtherId"))
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=dummyEnv.conf"))
    assert(checkpoint, "The checkpoint should be hit")
  }

  it should "parse arguments" in {
    val parsedArgs = JobUtils.parseArgs(Array())
    assert(parsedArgs.isEmpty)

    val parsedArgs2 = JobUtils.parseArgs(Array("-DThingOne=Thing1"))
    assert(parsedArgs2.size == 1)
    assert(parsedArgs2("ThingOne") == "Thing1")

    val parsedArgs3 = JobUtils.parseArgs(Array("ThingTwo=Thing2"))
    assert(parsedArgs3.size == 1)
    assert(parsedArgs3("ThingTwo") == "Thing2")
  }

  it should "skip arguments that have a key that is too small" in {
    val parsedArgs = JobUtils.parseArgs(Array(EMPTY_STRING))
    assert(parsedArgs.isEmpty)
  }

  it should "be able to get arguments from the config" in {
    val configArgs = JobUtils.getArgumentsFromConfig(ConfigFactory.parseString("java.command=\"derp -Dsomething=somethingelse\""))
    println(configArgs)
    assert(configArgs("something") == "somethingelse")
  }

  case class MandatoryConfigsBaseJobImpl(jobExecution: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = jobExecution(config, spark)

    override def getJobSpecificMandatoryConfigKeys: Set[String] = {
      Set("HappyHappy")
    }
  }

  it should "assert that some specified configs are populated" in {

    val baseJobImpl = MandatoryConfigsBaseJobImpl((a, b) => {
      assert(false, "Should not hit this point")
    })

    assertThrows[InvalidJobConfException](baseJobImpl.main(Array()))

  }

  it should "be able to read a runtime env file passed in through arguments and runtime override param" in {
    var checkpoint = false

    val testBaseJobImpl = BaseJobTestImpl((config, _) => {
      assert(config.getString("dummy") == "second")
      checkpoint = true
    })

    testBaseJobImpl.main(Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
      "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server", "-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR"))

    assert(checkpoint, "The checkpoint should be hit after substitution ")
  }

  case class BaseJobTestImplRuntimeParamMissing(jobFunTest: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFunTest(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("configFile", "environment", "container", "logLevelValue", "storageAccount")
    }
  }

  it should "be able to read a runtime env file passed in through arguments and few of the runtime override param are missing" in {
    var checkpoint = false

    val testBaseJobImplementation = BaseJobTestImplRuntimeParamMissing((config, _) => {
      assert(config.getString("container") == "spark")
      checkpoint = true
    })

    testBaseJobImplementation.main(Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
      "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server", "-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR"))

    assert(checkpoint, "The checkpoint should be hit after substitution ")
  }

  case class BaseJobTestImplConfigParamMissing(jobFunTest: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFunTest(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("configFile", "environment", "sparkOptions.spark.master", "logLevel", "container", "logLevelValue", "storageAccount")
    }
  }

  it should "be able to read a runtime env file passed in through arguments with few of the param missing and runtime override param" in {
    var checkpoint = false

    val testBaseJobImplementation = BaseJobTestImplConfigParamMissing((config, _) => {
      assert(config.getString("storageAccount") == "server")
      checkpoint = true
    })

    testBaseJobImplementation.main(Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
      "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server"))

    assert(checkpoint, "The checkpoint should be hit after substitution ")
  }

  case class BaseJobTestImplSubstitutionKeysMissing(jobFunTest: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFunTest(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("sparkOptions.spark.master", "logLevel", "logLevelValue", "storageAccount")
    }
  }

  it should "be able to read a runtime env file passed in through arguments and substitution keys are missing from runtime override param" in {
    var checkpoint = false

    val testBaseJobImplementation = BaseJobTestImplSubstitutionKeysMissing((config, _) => {
      assert(config.getString("dummy") == "second")
      checkpoint = true
    })

    assertThrows[com.typesafe.config.ConfigException](testBaseJobImplementation.main(
      Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
        "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server", "-DsparkOptions.spark.master=local[2]",
        "-DlogLevel=ERROR")))
  }

  case class BaseJobTestImplRuntimeKeyMissing(jobFunTest: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFunTest(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("configFile", "environment", "sparkOptions.spark.master", "logLevel", "container", "logLevelValue", "storageAccount")
    }
  }

  it should "be able to read a runtime env file passed in through arguments but it will not override it due to key missing in runtime param override" in {
    var checkpoint = false

    val testBaseJobImplementation = BaseJobTestImplRuntimeKeyMissing((config, _) => {
      assert(config.getString("dummy") == "second")
      checkpoint = true
    })

    testBaseJobImplementation.main(Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
      "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server", "-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR", "-Ddummy=third"))

    assert(checkpoint, "The checkpoint should be hit after substitution ")

  }

  case class BaseJobTestImplRuntimeKeyPresent(jobFunTest: (Config, SparkSession) => Unit) extends BaseJob {
    override def executeJob(config: Config, spark: SparkSession): Unit = {
      jobFunTest(config, spark)
    }

    override def getConfigKeysToOverride: Set[String] = {
      Set("sparkOptions.spark.master", "logLevel", "container", "logLevelValue", "storageAccount", "dummy")
    }
  }

  it should "be able to read a runtime env file passed in through arguments and it will override it" in {
    var checkpoint = false

    val testBaseJobImplementation = BaseJobTestImplRuntimeKeyPresent((config, _) => {
      assert(config.getString("dummy") == "third")
      checkpoint = true
    })

    testBaseJobImplementation.main(Array("-DconfigFile=localtest.conf", "-Denvironment=substitute.conf, dummyEnv.conf",
      "-DlogLevelValue=DEBUG", "-Dcontainer=spark", "-DstorageAccount=server", "-DsparkOptions.spark.master=local[2]", "-DlogLevel=ERROR", "-Ddummy=third"))

    assert(checkpoint, "The checkpoint should be hit after substitution ")

  }
}
