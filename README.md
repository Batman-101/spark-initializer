# spark-common
### Releases - [What's new ??](RELEASES.md)

Commit to a branch and then merge that into master. As there is no environment-specific stuff here, no need for dev branch.

###Library for common code components for the Finance Central Item Level Ledger applications.
###Not for business logic!

**Contains:**
* **[JsonPropertiesLoader](/src/main/scala/com//spark/common/config/JsonPropertiesLoader.scala)** - Functions that make managing configs easier, used by BaseJob
* **[SparkSessionInitializer](/src/main/scala/com//spark/common/config/SparkSessionInitializer.scala)** - Functions that handle bringing up Spark Sessions, used by BaseJob
* **[BaseJob](/src/main/scala/com//spark/common/jobs/BaseJob.scala)** - Create a new class and inherit from this, override the job, and you are good to go!
* **[IncrementalETLJob](/src/main/scala/com//spark/common/jobs/IncrementalETLJob.scala)** -  for processing multiple unit of work within one job by providing implementation of required contract.
* **[ORCReader](/src/main/scala/com//spark/common/reader/ORCReader.scala) and [ORCWriter](/src/main/scala/com//spark/common/writer/ORCWriter.scala)** - Tools to standardize reading and writing ORC files
* **[JsonReader](/src/main/scala/com//spark/common/reader/JsonReader.scala) and [JsonWriter](/src/main/scala/com//spark/common/writer/JsonWriter.scala)** - Tools to standardize reading and writing JSON files
* **[ParquetReader](/src/main/scala/com//spark/common/reader/ParquetReader.scala) and [ParquetWriter](/src/main/scala/com//spark/common/writer/ParquetWriter.scala)** - Tools to standardize reading and writing Parquet files
* **[CsvReader](/src/main/scala/com//spark/common/reader/CsvReader.scala) and [CsvWriter](/src/main/scala/com//spark/common/writer/CsvWriter.scala)** - Tools to standardize reading and writing CSV files
* **[TextReader](/src/main/scala/com//spark/common/reader/TextReader.scala) and [TextWriter](/src/main/scala/com//spark/common/writer/TextWriter.scala)** - Tools to standardize reading and writing Text files
* **[JobUtils](/src/main/scala/com//spark/common/utilities/JobUtils.scala)** - Capable of parse JVM run time arguments, get any '-D' arguments from the command line via the config
* **[ConfigUtils](/src/main/scala/com//spark/common/utilities/ConfigUtils.scala)** - Configuration Utils capable to initialises configuration, validate mandatory configuration parameters, adding properties into config object at run time, generate UUID on demand.
* **[HDFSUtility](/src/main/scala/com//spark/common/utilities/HDFSUtility.scala)** - Capable of deleting a hdfs path and fetching the latest available numerical path out of multiple hdfs paths within a base path.
* **[PathUtility](/src/main/scala/com//spark/common/utilities/PathUtility.scala)** - Path manipulation utility, creation of Path object from multiple String/Path objects




**Notes:**
- This requires a change to an application's configs. You will want to refactor the configs
to follow the Typesafe Config guidelines.
    - Specifically, A base config called 'reference.conf' that has common and default configurations and a deployment-specific configuration "dev.conf" or "local.conf" or whatever
- Because this is a shared API, no 'Try' objects are used, instead we throw exceptions (per Databricks code style for libraries) 
- parseResources method, unlike typesafe config default load method behaviour, which will only load the configuration associated with run time config file without considering reference.conf or loading JVM arguments like: ClassLoader#getResource, Thread#getContextClassLoader() etc. 
- **[BaseJob](/src/main/scala/com//spark/common/jobs/BaseJob.scala):** It provides you inherent capabilities like : 
    - **Creates a Config object :** It accepts non mandatory program arguments with tag 'configFile' and 'environment' and value as configuration files.
                                Where 'environment' configuration file tag given more preference than 'configFile' configuration file tag.
                                
             Example: "-DconfigFile=localtest.conf", "-Denvironment=substitute.conf"
                                
    - **Created a SparkSession object:** Looks for attributes sparkOptions, hadoopOptions in the configuration object and all the key value pair inside these
                                      attributes(sparkOptions, hadoopOptions) will bind while creating SparkSession object.
                                      
    - **Validates presence of the required configuration :** Overrides getJobSpecificMandatoryConfigKeys to specify the mandatory configuration.
- **[IncrementalETLJob](/src/main/scala/com//spark/common/jobs/IncrementalETLJob.scala):** It provides you inherent capabilities and required contracts to be implemented :
     - **Creates a Config object :** It accepts non mandatory program arguments with tag 'configFile' and 'environment' and value as configuration files.
                                    Where 'environment' configuration file tag given more preference than 'configFile' configuration file tag.
                                    
                 Example: "-DconfigFile=localtest.conf", "-Denvironment=substitute.conf"
                                    
     - **Created a SparkSession object:** Looks for attributes sparkOptions, hadoopOptions in the configuration object and all the key value pair inside these
                                          attributes(sparkOptions, hadoopOptions) will bind while creating SparkSession object.
                                          
     - **[Contracts:](/src/main/scala/com//spark/common/jobs/IncrementalETLJob.scala)** 
       
       - **def getJobSpecificMandatoryConfigKeys: Set[String] = Set()**  
                - Validates presence of the required configuration, overrides this to specify the mandatory configuration.
          
       - **def getConfigKeysToOverride: Set[String] = Set()**  
                - Specify configuration parameters to filter at Runtime, return Set of keys to filter parameters at runtime.
         
       - **def getIncrementalWorkLog(config: Config, sparkSession: SparkSession): DataFrame**  
                - To incremental work log details from audit table.  
       
       - **def transformIncrementalWorkLog(config: Config, sparkSession: SparkSession, dataFrame: DataFrame): DataFrame = dataFrame**  
                - To modify work log i.e. breaking 1 record to N records, OR N records into 1 record based on how many batches you want to process.  
           
       - **def updateBatchConfig(config: Config, sparkSession: SparkSession, row: Row): Config**  
                - To update the configuration details w.r.t each unit of work.  
         
       - **def getBatchSpecificMandatoryConfigKeys: Set[String] = Set()**
                - Specify mandatory configuration parameter w.r.t to each unit of work unit of work can be a batch. 
        
       - **def readBatchData(config: Config, sparkSession: SparkSession): DataFrame**  
                - To read the source data applicable for each unit of work.  
       
       - **def filterBatchData(config: Config, sparkSession: SparkSession, row: Row, dateFrame: DataFrame): DataFrame = dateFrame**  
                - To filter the source data(unit of work) based on logical unit of work. 
        
       - **def transformBatchData(config: Config, sparkSession: SparkSession, dateFrame: DataFrame): DataFrame**  
                - To apply business logic on filtered source data(unit of work) 
        
       - **def loadBatchData(config: Config, sparkSession: SparkSession, resultDataFrame: DataFrame): Unit**  
                - To persist sink dataframe to specified storage  
       
       - **def logBatchAuditDetails(config: Config, sparkSession: SparkSession, row: Row, sourceDataFrame: Option[DataFrame], sinkDataFrame: Option[DataFrame], throwable: Option[Throwable]): Unit**  
                - Log information w.r.t each unit of work.
         
       - **def postBatchProcessing(config: Config, sparkSession: SparkSession, row: Row)**  
                - To apply post processing activity like deletion of checkpoints, closing of resources etc. 
        
       - **def logJobAuditDetails(config: Config, sparkSession: SparkSession, throwable: Option[Throwable])**  
                - Log Job information for all processed unit of work.
