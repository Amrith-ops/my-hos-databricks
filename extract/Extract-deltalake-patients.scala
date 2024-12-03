// Databricks notebook source
import org.apache.spark.sql.types.{StructType,StructField,DateType,StringType,LongType,FloatType}
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession

def extract_datalake_to_deltalake(spark:SparkSession, inputPath:String, outputPath:String): Unit = {
  val schema = StructType (
    List (
      StructField("Id",StringType),
      StructField("BIRTHDATE",DateType),
      StructField("DEATHDATE",DateType),
      StructField("PREFIX",StringType),
      StructField("FIRST",StringType),
      StructField("LAST",StringType),
      StructField("SUFFIX",StringType),
      StructField("MAIDEN",StringType),
      StructField("MARITAL",StringType),
      StructField("RACE",StringType),
      StructField("ETHNICITY",StringType),
      StructField("GENDER",StringType),
      StructField("BIRTHPLACE",StringType),
      StructField("ADDRESS",StringType),
      StructField("CITY",StringType),
      StructField("STATE",StringType),
      StructField("COUNTY",StringType),
      StructField("ZIP",LongType),
      StructField("LAT",FloatType),
      StructField("LON",FloatType)

    )
  )
  val copyDataLake = spark.readStream.format("cloudFiles")
                     .option("cloudFiles.format","csv")
                     .option("header","true")
                     .schema(schema)
                     .load(inputPath)
  val loadDateAdded = copyDataLake.withColumn("LOAD_DATE",date_format(current_timestamp(), "yyyyMMddHH"))
                      .withColumn("processed_timestamp", current_timestamp())
  
  loadDateAdded.writeStream.format("delta").partitionBy("LOAD_DATE").
  option("checkpointLocation", "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/patients/VOLUMES/checkpoints").trigger(Trigger.AvailableNow).start(outputPath)
  println("Executing the streaming query..for patient datalake")
}

// COMMAND ----------

extract_datalake_to_deltalake(spark, "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/patients/","abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/patients/VOLUMES/data")

// COMMAND ----------

val data = spark.read.format("delta").load("abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/patients/VOLUMES/data/")

// COMMAND ----------

display(data)
