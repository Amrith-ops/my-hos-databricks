// Databricks notebook source
// MAGIC %sql
// MAGIC SELECT current_metastore()

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE CATALOG IF NOT EXISTS dev;
// MAGIC CREATE SCHEMA IF NOT EXISTS dev.hospital_treatment;

// COMMAND ----------

// writing the code for external location to be reused again with different external locations,paths
import org.apache.spark.sql.SparkSession
def create_external_locations(locationName:String, 
urlStorage:String, credentialName:String, comment:String, spark:SparkSession):Unit = {
    spark.sql(
      s"""
          CREATE EXTERNAL LOCATION IF NOT EXISTS $locationName
          URL '$urlStorage'
          WITH (STORAGE CREDENTIAL $credentialName)
          COMMENT '$comment'
      """
    )
}

// COMMAND ----------

def passConfigsToExtLoc(spark:SparkSession,configs: String*): Unit = {
  println("Passing configs to external location")
  create_external_locations(configs(0),configs(1),configs(2),configs(3),spark)
}
def call_create_external_location(listOfConfigs:List[List[String]], spark:SparkSession):Unit = {
    for(configValue:List[String] <- listOfConfigs) {
      passConfigsToExtLoc(spark,configValue:_*)
    }
}

val listOfLocations = List(
  List(
    "`PATIENTS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/patients/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PATIENTS IN LANDING CONTAINER"
  ),
  List(
    "`PROCEDURES_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/procedures/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PROCEDURES IN LANDING CONTAINER"
  ),
  List(
    "`PAYERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/payers/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PAYERS IN LANDING CONTAINER"
  ),
  List(
    "`ORGANIZATION_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/organization/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Organization IN LANDING CONTAINER"
  ),
   List(
    "`ENCOUNTERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/encounters/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Encounters IN LANDING CONTAINER"
  )
)

call_create_external_location(listOfLocations,spark)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
// now using external locations lets create external volumes

def create_external_volume(spark:SparkSession,volumeArgs:String*):Unit = {
  spark.sql(s"""

    CREATE EXTERNAL VOLUME ${volumeArgs(0)}.${volumeArgs(1)}.${volumeArgs(2)}
    LOCATION '${volumeArgs(3)}'
  
  """)
}

def pass_configs_to_external_volume(spark:SparkSession,configs:List[String]):Unit = {
  println("passing configs to external volume")
  create_external_volume(spark,configs:_*)

}

def call_create_external_volume(listOfConfigs:List[List[String]], spark:SparkSession): Unit = {
  for (config:List[String] <- listOfConfigs) {
    pass_configs_to_external_volume(spark, config)
  }
}

val listOfLocations = List(
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PATIENTS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/patients/"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PROCEDURES_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/procedures/"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PAYERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/payers/"
  )
  ,
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`ORGANIZATION_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/organization/"
  ),
   List(
    "`DEV`",
    "`hospital_treatment`",
    "`ENCOUNTERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/encounters/"
  )
)
call_create_external_volume(listOfLocations,spark)

// COMMAND ----------


