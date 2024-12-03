// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format,current_timestamp}
import org.apache.spark.sql.types.{DateType,StringType,LongType,StructType, StructField, IntegerType}
import org.apache.spark.sql.streaming.Trigger

def extract_data_deltalake_payers(spark:SparkSession, inputPath:String,
outputPath:String): Unit = {
  // you will not get the data continously for payers
  /* 
    payers is the static content they dont change continuosly the data comes only once
  */
   val schema = StructType (
    List(
      StructField("Id",StringType),
      StructField("Name",StringType),
      StructField("ADDRESS",StringType),
      StructField("CITY",StringType),
      StructField("STATE_HEADQUARTERED",StringType),
      StructField("ZIP",IntegerType),
      StructField("PHONE",StringType)
    )
  )
  val copyDataLake = spark.read.format("csv")
                     .option("header","true")
                     .schema(schema)
                     .load(inputPath)
  val loadDate = copyDataLake.withColumn("LOAD_DATE",date_format(current_timestamp(),"yyyyMMddHH"))
  .withColumn("processed_timestamp", current_timestamp())

  loadDate.write.format("delta").partitionBy("LOAD_DATE").mode(SaveMode.Append).save(outputPath)
}

// COMMAND ----------

extract_data_deltalake_payers(spark,"abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/payers/","abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/payers/VOLUMES")

// COMMAND ----------

display(spark.read.load("abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/payers/VOLUMES"))
