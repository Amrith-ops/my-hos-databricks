// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format,current_timestamp}
import org.apache.spark.sql.types.{DateType,StringType,LongType,StructType, StructField, IntegerType}
import org.apache.spark.sql.streaming.Trigger
def extract_datalake_delta_procedures(spark:SparkSession,
inputPath:String,outputPath:String):Unit = {
  val schema = StructType (
    List(
      StructField("START",DateType),
      StructField("STOP",DateType),
      StructField("PATIENT",StringType),
      StructField("ENCOUNTER",StringType),
      StructField("CODE",LongType),
      StructField("DESCRIPTION",StringType),
      StructField("BASE_COST",IntegerType),
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
  .option("checkpointLocation","abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/procedures/VOLUMES/checkpoints")
  .option("path",outputPath)
  .trigger(Trigger.AvailableNow)
  .start()

  query.awaitTermination()

  println("Finished streaming the raw file from data lake to delta lake")


}

// COMMAND ----------

extract_datalake_delta_procedures(
  spark,
  "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/procedures/",
  "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/procedures/VOLUMES/data/"
)

// COMMAND ----------

val loaded = spark.read.format("delta").load("abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/procedures/VOLUMES/data/")
display(loaded)
