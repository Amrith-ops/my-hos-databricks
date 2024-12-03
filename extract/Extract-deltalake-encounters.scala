// Databricks notebook source
import org.apache.spark.sql.types.{StructType,StructField,DateType,StringType,LongType,FloatType}
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession

def extract_datalake_delta_encounters(spark:SparkSession, 
inputPath:String,outputPath:String): Unit = {
  val schema = StructType (
    List(
      StructField("Id",StringType),
      StructField("START",DateType),
      StructField("STOP",DateType),
      StructField("PATIENT",StringType),
      StructField("ORGANIZATION",StringType),
      StructField("PAYER",StringType),
      StructField("ENCOUNTERCLASS",StringType),
      StructField("CODE",LongType),
      StructField("DESCRIPTION",StringType),
      StructField("BASE_ENCOUNTER_COST",FloatType),
      StructField("TOTAL_CLAIM_COST",FloatType),
      StructField("PAYER_COVERAGE",FloatType),
      StructField("REASONCODE",LongType),
      StructField("REASONDESCRIPTION",StringType)

    )
  )
  val copyDataLake = spark.readStream.format("cloudFiles")
                     .option("cloudFiles.format","csv")
                     .option("header","true")
                     .schema(schema)
                     .load(inputPath)

  val loadDate = copyDataLake.withColumn("LOAD_DATE",date_format(current_timestamp(),"yyyyMMddHH"))
  .withColumn("processed_timestamp", current_timestamp())

  val query = loadDate.writeStream
  .format("delta")
  .partitionBy("LOAD_DATE")
  .option("checkpointLocation","abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/encounters/VOLUMES/checkpoints")
  .option("path",outputPath)
  .trigger(Trigger.AvailableNow)
  .start()

  query.awaitTermination()

  println("Finished streaming the raw file from data lake to delta lake")
}

// COMMAND ----------

extract_datalake_delta_encounters(spark, "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/encounters/","abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/encounters/VOLUMES/data")

// COMMAND ----------

display(spark.read.load("abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/encounters/VOLUMES/data"))
