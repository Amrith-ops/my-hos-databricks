// Databricks notebook source
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

def passConfigsToExtLoc(spark:SparkSession,configs: String*): Unit = {
  println("Passing configs to external location")
  create_external_locations(configs(0),configs(1),configs(2),configs(3),spark)
}
def call_create_external_location(listOfConfigs:List[List[String]], spark:SparkSession):Unit = {
    for(configValue:List[String] <- listOfConfigs) {
      passConfigsToExtLoc(spark,configValue:_*)
    }
}

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
